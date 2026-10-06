"""Synthetic two-broker integration test; run inside the Compose network.

python tests/integration/kafka_smoke.py
Requires the app source and confluent-kafka==2.15.1. No production endpoints.
Creates uniquely named test topics and one inactive group on each isolated broker.
"""
import json
import http.client
import threading
from pathlib import Path
from http.server import ThreadingHTTPServer
from unittest.mock import patch
import logging
import signal
import tempfile
import time
import uuid

import confluent_kafka as ck
from confluent_kafka.admin import AdminClient, NewTopic

from flowbridge.live.jobs import JobManager


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def client_config(brokers):
    return {'bootstrap.servers': brokers, 'security.protocol': 'PLAINTEXT',
            'socket.timeout.ms': 4000, 'log_level': 0}


def admin(brokers):
    logger = logging.getLogger('smoke.silent')
    logger.handlers = [logging.NullHandler()]
    logger.propagate = False
    return AdminClient(client_config(brokers), logger=logger)


def wait_brokers(clients):
    deadline = time.monotonic() + 60
    ready = {}
    while time.monotonic() < deadline:
        for name, client in clients.items():
            if name in ready:
                continue
            try:
                metadata = client.list_topics(timeout=2)
                if metadata.cluster_id and metadata.brokers:
                    ready[name] = metadata.cluster_id
            except ck.KafkaException:
                pass
        if len(ready) == len(clients):
            require(len(set(ready.values())) == len(clients), 'Source and target must be distinct clusters')
            return
        time.sleep(.25)
    raise AssertionError('Isolated test brokers were not ready within 60 seconds')


def produce(brokers, topic, records):
    producer = ck.Producer(dict(client_config(brokers), **{'acks': 'all', 'message.timeout.ms': 10000}))
    outcomes = []
    for partition, key, value, headers, timestamp in records:
        producer.produce(topic, partition=partition, key=key, value=value,
                         headers=headers, timestamp=timestamp,
                         on_delivery=lambda error, message: outcomes.append((error, message.partition(), message.offset())))
    require(producer.flush(12) == 0, 'Synthetic record delivery timed out')
    require(len(outcomes) == len(records) and all(error is None for error, _, _ in outcomes), 'Synthetic producer failed')


def offsets(client, group, topic):
    request = ck.ConsumerGroupTopicPartitions(group)
    result = client.list_consumer_group_offsets([request], request_timeout=8)[group].result(8)
    return {p.partition: p.offset for p in result.topic_partitions if p.topic == topic and p.offset >= 0}


def set_offsets(client, group, topic, values):
    request = ck.ConsumerGroupTopicPartitions(group, [ck.TopicPartition(topic, p, offset) for p, offset in values.items()])
    # Fresh brokers can serve metadata before the __consumer_offsets coordinator
    # is ready. Retry only known transient setup failures, never unknown errors.
    deadline = time.monotonic() + 15
    transient = {getattr(ck.KafkaError, name, -99999) for name in
                 ('COORDINATOR_LOAD_IN_PROGRESS', 'COORDINATOR_NOT_AVAILABLE',
                  'NOT_COORDINATOR', 'UNKNOWN_TOPIC_OR_PART')}
    while True:
        try:
            result = client.alter_consumer_group_offsets([request], request_timeout=8)[group].result(8)
            errors = [p.error for p in result.topic_partitions if p.error is not None and p.error.code() != 0]
        except ck.KafkaException as error:
            errors = [error.args[0]]
        if not errors:
            return
        if time.monotonic() >= deadline or not all(e.retriable() or e.code() in transient for e in errors):
            raise AssertionError('Source test group offset setup failed with Kafka codes: ' +
                                 ', '.join(str(e.code()) for e in errors))
        time.sleep(.25)


def await_job(manager, job, copied=None, completed=False):
    deadline = time.monotonic() + 35
    while time.monotonic() < deadline:
        status = manager.status(job)
        require(status['state'] not in ('failed', 'cancelled', 'interrupted'), 'Migration failed: ' + json.dumps(status))
        if completed and status['state'] == 'completed':
            return status
        if not completed and status['state'] == 'mirroring' and status['lag'] == 0 and status['copied'] == copied:
            return status
        time.sleep(.15)
    raise AssertionError('Migration did not reach expected state: ' + json.dumps(manager.status(job)))


def collect(brokers, topic, counts):
    consumer = ck.Consumer(dict(client_config(brokers), **{'group.id': 'smoke-read-' + uuid.uuid4().hex,
                                                         'enable.auto.commit': False, 'enable.auto.offset.store': False,
                                                         'auto.offset.reset': 'error'}))
    collected = {}
    try:
        consumer.assign([ck.TopicPartition(topic, p, 0) for p in counts])
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline and len(collected) < sum(counts.values()):
            message = consumer.poll(.25)
            if message is None:
                continue
            require(not message.error(), 'Verification consumer error')
            collected[(message.partition(), message.offset())] = (message.key(), message.value(), message.headers() or [], message.timestamp()[1])
        require(len(collected) == sum(counts.values()), 'Target did not contain the expected record count')
        return collected
    finally:
        consumer.close()


def http_migration(clients):
    """Exercise the real authenticated HTTP surface against both real brokers."""
    from flowbridge.server import Handler
    from flowbridge.live import api
    suffix = uuid.uuid4().hex[:10]
    source_topic, target_topic = 'http-in-' + suffix, 'http-out-' + suffix
    for name, topic in [('source', source_topic), ('target', target_topic)]:
        clients[name].create_topics([NewTopic(topic, num_partitions=2, replication_factor=1,
                                             config={'cleanup.policy': 'delete', 'message.timestamp.type': 'CreateTime'})],
                                   request_timeout=10)[topic].result(10)
    now = int(time.time() * 1000)
    seeds = [(0, b'http-first', b'first', [('via', b'http')], now),
             (0, b'http-delete', None, [], now + 1),
             (1, b'http-third', b'\x00\xff', [], now + 2)]
    produce('source:9092', source_topic, seeds)
    with tempfile.TemporaryDirectory(prefix='flowbridge-http-smoke-') as directory:
        token = uuid.uuid4().hex + uuid.uuid4().hex
        token_file = Path(directory) / 'token'
        token_file.write_text(token)
        token_file.chmod(0o600)
        manager = JobManager(Path(directory) / 'state')
        environment = {'FLOWBRIDGE_LIVE_ENABLED': 'true', 'FLOWBRIDGE_LIVE_TOKEN_FILE': str(token_file)}
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        job = None
        with patch.dict('os.environ', environment), patch.object(api, '_manager', manager):
            thread.start()
            def request(path, data=None, authenticated=True):
                headers = {'Content-Type': 'application/json'}
                if authenticated:
                    headers['Authorization'] = 'Bearer ' + token
                connection = http.client.HTTPConnection('127.0.0.1', server.server_port, timeout=15)
                try:
                    connection.request('GET' if data is None else 'POST', path,
                                       None if data is None else json.dumps(data), headers)
                    response = connection.getresponse()
                    return response.status, json.loads(response.read())
                finally:
                    connection.close()
            def success(path, data=None):
                status, result = request(path, data)
                require(status == 200, 'Live HTTP request failed: ' + json.dumps({'status': status, 'result': result}))
                require(token not in json.dumps(result), 'Live HTTP response exposed owner token')
                return result
            def wait_http(completed=False):
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    state = success('/api/live/jobs/' + job)
                    require(state['state'] not in ('failed', 'cancelled', 'interrupted'), 'Live HTTP migration failed: ' + json.dumps(state))
                    if completed and state['state'] == 'completed':
                        return state
                    if not completed and state['state'] == 'mirroring' and state['lag'] == 0 and state['copied'] == 3:
                        return state
                    time.sleep(.15)
                raise AssertionError('Live HTTP migration did not reach expected state')
            try:
                require(request('/api/live/jobs', authenticated=False)[0] == 401, 'Unauthenticated job-list request was not rejected')
                assessment = success('/api/live/assess', {
                    'source': {'brokers': 'source:9092', 'topic': source_topic, 'security_protocol': 'PLAINTEXT'},
                    'target': {'brokers': 'target:9092', 'topic': target_topic, 'security_protocol': 'PLAINTEXT'},
                    'groups': []})
                started = success('/api/live/start', {'plan_id': assessment['plan_id'], 'acknowledge': True})
                job = started['job_id']
                require(request('/api/live/jobs/' + job, authenticated=False)[0] == 401, 'Unauthenticated job inspection was not rejected')
                wait_http()
                success('/api/live/cutover', {'job_id': job, 'acknowledge': True,
                                             'producers_stopped': True, 'consumers_stopped': True})
                finished = wait_http(completed=True)
                require(finished['copied'] == 3, 'HTTP migration copied count mismatch')
                actual = collect('target:9092', target_topic, {0: 2, 1: 1})
                expected, positions = {}, {0: 0, 1: 0}
                for partition, key, value, headers, timestamp in seeds:
                    expected[(partition, positions[partition])] = (key, value, headers, timestamp)
                    positions[partition] += 1
                require(actual == expected, 'HTTP-created migration did not preserve target records')
            finally:
                if job is not None and manager.status(job)['state'] != 'completed':
                    manager.cancel(job)
                    for _ in range(50):
                        if job not in manager.running: break
                        time.sleep(.1)
                server.shutdown()
                server.server_close()
                thread.join()
    return True


def main():
    def timeout(signum, frame):
        raise TimeoutError('Kafka integration smoke test exceeded 120 seconds')
    signal.signal(signal.SIGALRM, timeout)
    signal.alarm(120)
    managers = []
    try:
        clients = {'source': admin('source:9092'), 'target': admin('target:9092')}
        wait_brokers(clients)
        suffix = uuid.uuid4().hex[:10]
        source_topic, target_topic, group = 'smoke-in-' + suffix, 'smoke-out-' + suffix, 'smoke-group-' + suffix
        for name, topic in [('source', source_topic), ('target', target_topic)]:
            clients[name].create_topics([NewTopic(topic, num_partitions=2, replication_factor=1,
                                                 config={'cleanup.policy': 'delete', 'message.timestamp.type': 'CreateTime'})],
                                       request_timeout=10)[topic].result(10)
        now = int(time.time() * 1000)
        seeds = [(0, b'\x00key', b'\xff\x00payload', [('type', b'bytes'), ('dup', b'a'), ('dup', b'b')], now),
                 (0, b'empty', b'', [('null-header', None)], now + 1),
                 (0, b'deleted', None, [('op', b'delete')], now + 2),
                 (1, None, b'no-key', [], now + 3),
                 (1, b'partition-one', b'\x01\x02', [('binary', b'\x00\xff')], now + 4)]
        initial = [(p, b'existing', b'preexisting', [], now - 1) for p in (0, 1)]
        produce('target:9092', target_topic, initial)
        produce('source:9092', source_topic, seeds)
        source_committed = {0: 1, 1: 2}
        set_offsets(clients['source'], group, source_topic, source_committed)
        require(offsets(clients['source'], group, source_topic) == source_committed, 'Source offset setup was not persisted')
        with tempfile.TemporaryDirectory(prefix='flowbridge-smoke-') as directory:
            manager = JobManager(directory)
            request = {'source': {'brokers': 'source:9092', 'topic': source_topic, 'security_protocol': 'PLAINTEXT'},
                       'target': {'brokers': 'target:9092', 'topic': target_topic, 'security_protocol': 'PLAINTEXT'},
                       'groups': [group]}
            assessment = manager.assess(request)
            require(assessment['report']['ok'], 'Preflight did not pass')
            job = manager.start(assessment['plan_id'], True)['job_id']
            managers.append((manager, job))
            await_job(manager, job, copied=5)
            tail = [(0, b'tail-key', b'new-after-snapshot', [('tail', b'true')], now + 5)]
            produce('source:9092', source_topic, tail)
            await_job(manager, job, copied=6)
            # All synthetic producers have flushed/returned; no consumer joined the source group.
            manager.cutover({'job_id': job, 'acknowledge': True, 'producers_stopped': True, 'consumers_stopped': True})
            completed = await_job(manager, job, completed=True)
            require(offsets(clients['source'], group, source_topic) == source_committed, 'Migration changed source group offsets')
            require(offsets(clients['target'], group, target_topic) == {0: 2, 1: 3}, 'Target committed offsets were not translated across existing records')
            actual = collect('target:9092', target_topic, {0: 5, 1: 3})
            expected = {}
            next_offset = {0: 0, 1: 0}
            for partition, key, value, headers, timestamp in initial + seeds + tail:
                expected[(partition, next_offset[partition])] = (key, value, headers, timestamp)
                next_offset[partition] += 1
            require(actual == expected, 'Target keys, bytes, tombstones, headers, timestamps, partitions or offsets differ')
            require(completed['copied'] == 6, 'Incorrect copied-record total')
            http_verified = http_migration(clients)
            print(json.dumps({'result': 'passed', 'http_live_migration': http_verified, 'http_copied': 3, 'copied': 6, 'target_records': 8, 'partitions': 2,
                              'checks': ['initial copy', 'live tail', 'binary keys and values', 'empty value', 'tombstone',
                                         'duplicate and null headers', 'timestamps', 'partition preservation',
                                         'pre-existing target record retention', 'target offset translation', 'source offsets unchanged'],
                              'delivery': completed['result'].get('delivery')}))
        managers.clear()
    finally:
        for manager, job in managers:
            try: manager.cancel(job)
            except Exception: pass
        signal.alarm(0)


if __name__ == '__main__':
    main()
