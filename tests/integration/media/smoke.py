"""Isolated synthetic S3/Kafka media evidence; never uses real cloud credentials."""
import json
import os
from pathlib import Path
import signal
import tempfile
import time
import uuid

import boto3
from botocore.config import Config
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic


def wait_ready(callback, seconds=60):
    until = time.monotonic() + seconds
    while True:
        try:
            return callback()
        except Exception:
            if time.monotonic() >= until:
                raise
            time.sleep(1)


def clients():
    s3 = boto3.client('s3', endpoint_url='http://s3:9090', region_name='us-east-1',
                      aws_access_key_id='synthetic-test-only', aws_secret_access_key='synthetic-test-only',
                      config=Config(s3={'addressing_style': 'path'}, connect_timeout=3, read_timeout=5,
                                    retries={'max_attempts': 2}))
    wait_ready(s3.list_buckets)
    admin = AdminClient({'bootstrap.servers': 'kafka:9092'})
    wait_ready(lambda: admin.list_topics(timeout=3))
    return s3, admin


def fixture_blueprint(s3, admin):
    tag = uuid.uuid4().hex[:10]
    pipelines = []
    fixtures = {}
    for media_type, filename in [('image', 'image.png'), ('text', 'note.txt'), ('video', 'clip.mp4')]:
        source, destination = f'fb-{tag}-{media_type}-in', f'fb-{tag}-{media_type}-out'
        s3.create_bucket(Bucket=source)
        s3.create_bucket(Bucket=destination)
        content = Path('/demo/fixtures', filename).read_bytes()
        s3.put_object(Bucket=source, Key='incoming/' + filename, Body=content)
        fixtures[media_type] = content
        pipelines.append({'id': media_type, 'media_type': media_type,
            'source': {'bucket': source, 'prefix': 'incoming/'},
            'destination': {'bucket': destination, 'prefix': 'processed/'},
            'stream': {'topic': f'fb-{tag}-{media_type}', 'dead_letter_topic': f'fb-{tag}-{media_type}-dlq'},
            'processing': {'engine': 'http', 'url': 'https://processor.test/process', 'method': 'POST'}})
    topics = [NewTopic(p['stream'][key], num_partitions=1, replication_factor=1)
              for p in pipelines for key in ['topic', 'dead_letter_topic']]
    for future in admin.create_topics(topics).values():
        future.result(timeout=20)
    return {'schema': 'flowbridge/media-etl/v1', 'name': 'synthetic-media-proof',
            's3': {'endpoint': 'http://s3:9090', 'region': 'us-east-1', 'path_style_access': True},
            'kafka': {'brokers': 'kafka:9092'},
            'delivery': {'checkpoint_backend': 'sqlite', 'deduplication': 'bucket-key-version-etag', 'retries': 3},
            'pipelines': pipelines}, fixtures


def kafka_publisher():
    producer = Producer({'bootstrap.servers': 'kafka:9092', 'enable.idempotence': True, 'acks': 'all'})
    def publish(topic, event):
        failures = []
        producer.produce(topic, json.dumps(event, sort_keys=True).encode(),
                         on_delivery=lambda error, message: failures.append(error) if error else None)
        if producer.flush(10) or failures:
            raise RuntimeError('Synthetic Kafka publication failed')
    return publish


def run_engine(blueprint, fixtures, s3):
    from flowbridge.media_runtime import MediaRunner, MediaError, inspect_media
    import copy
    import hashlib
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    import http.client
    import threading
    lanes = {p['media_type']: p for p in blueprint['pipelines']}
    text_bucket = lanes['text']['source']['bucket']
    for key, value in [('transient.txt', b'transient success'), ('permanent.txt', b'permanent failure'),
                       ('missing.txt', b'deleted after discovery')]:
        s3.put_object(Bucket=text_bucket, Key='incoming/' + key, Body=value)
    s3.put_object(Bucket=lanes['image']['source']['bucket'], Key='incoming/corrupt.png', Body=b'not a png')
    attempts = {}
    class Processor(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_POST(self):
            data = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            key = data['key']; attempts[key] = attempts.get(key, 0) + 1
            fail = key.endswith('permanent.txt') or (key.endswith('transient.txt') and attempts[key] == 1)
            self.send_response(503 if fail else 200); self.end_headers()
            self.wfile.write(json.dumps({'accepted': not fail}).encode())
    server = ThreadingHTTPServer(('127.0.0.1', 0), Processor)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    def process(job, content):
        result = inspect_media(job, content)
        client = http.client.HTTPConnection('127.0.0.1', server.server_port, timeout=3)
        try:
            client.request('POST', '/process', json.dumps({'key': job['source_key']}), {'Content-Type':'application/json'})
            response = client.getresponse(); response.read()
            if response.status != 200: raise MediaError('synthetic_processing_503')
        finally: client.close()
        return result
    publish = kafka_publisher(); journal_events = []
    def publisher(topic, event):
        publish(topic, event); journal_events.append((topic, event))
    clock = [time.time()]
    with tempfile.TemporaryDirectory() as directory:
        state = str(Path(directory, 'ledger.sqlite'))
        runner = MediaRunner(blueprint, state, s3, processor=process, publisher=publisher, clock=lambda:clock[0])
        assert runner.discover()['discovered'] == 7
        assert runner.discover()['discovered'] == 0
        s3.delete_object(Bucket=text_bucket, Key='incoming/missing.txt')
        first = runner.run_once()
        assert first['counts']['complete'] == 3 and first['counts']['retry'] == 4, first
        # Recreate the worker with its persisted ledger; transient HTTP failure recovers.
        runner = MediaRunner(blueprint, state, s3, processor=process, publisher=publisher, clock=lambda:clock[0])
        for _ in range(3):
            clock[0] += 65; status = runner.run_once()
        assert status['counts']['complete'] == 4 and status['counts']['dead_letter'] == 3, status
        assert all(j['dlq_published'] for j in status['jobs'] if j['state'] == 'dead_letter')
        with runner.db() as db: rows = [dict(r) for r in db.execute('SELECT * FROM jobs')]
        for job in rows:
            if job['state'] != 'complete': continue
            obj = s3.get_object(Bucket=job['destination_bucket'], Key=job['destination_key'])
            content = obj['Body'].read(); obj['Body'].close()
            expected = b'transient success' if job['source_key'].endswith('transient.txt') else fixtures[job['media_type']]
            assert content == expected
            assert obj['Metadata']['flowbridge-job-id'] == job['id']
            result_obj = s3.get_object(Bucket=job['destination_bucket'], Key=job['destination_key']+'.flowbridge-result.json')
            result = json.loads(result_obj['Body'].read()); result_obj['Body'].close()
            assert result['sha256'] == hashlib.sha256(expected).hexdigest()
        assert len(journal_events) == 10, journal_events
        consumer = Consumer({'bootstrap.servers':'kafka:9092', 'group.id':str(uuid.uuid4()), 'auto.offset.reset':'earliest', 'enable.auto.commit':False})
        consumer.subscribe([p['stream'][k] for p in blueprint['pipelines'] for k in ['topic','dead_letter_topic']])
        received=[]; until=time.monotonic()+25
        try:
            while len(received)<10 and time.monotonic()<until:
                message=consumer.poll(1)
                if message is not None and not message.error(): received.append((message.topic(),json.loads(message.value())))
        finally: consumer.close()
        assert len(received)==10
        assert sorted((t,e['job_id']) for t,e in received)==sorted((t,e['job_id']) for t,e in journal_events)
        # Publication failure must retain a retry and avoid copying prematurely.
        s3.put_object(Bucket=text_bucket, Key='incoming/publish-outage.txt', Body=b'outbox recovery')
        assert runner.discover()['discovered']==1
        def unavailable(*_): raise RuntimeError('synthetic broker outage')
        runner.publisher=unavailable
        failed=runner.run_once(); assert failed['counts']['retry']==1
        outage=[j for j in failed['jobs'] if j['state']=='retry'][0]
        assert outage['copied']==0 and outage['published']==0
        runner.publisher=publisher; clock[0]+=65
        recovered=runner.run_once(); assert recovered['counts']['complete']==5
        bad=copy.deepcopy(blueprint); bad['pipelines'][0]['source']['bucket']='missing-'+uuid.uuid4().hex
        try: MediaRunner(bad, str(Path(directory,'missing.sqlite')), s3).discover()
        except MediaError as error: assert str(error)=='source_listing_failed'
        else: raise AssertionError('Missing source bucket incorrectly succeeded')
    server.shutdown(); server.server_close(); thread.join(timeout=3)
    print(json.dumps({'result':'passed','s3_fixture':'adobe/s3mock:5.1.0','kafka':'apache/kafka:4.2.0',
                      'source_buckets':3,'destination_buckets':3,'complete':5,'dead_letter':3,
                      'kafka_events_read_back':10,'worker_restart':True,'http_503_retry':True,
                      'broker_outage_retry':True,'native_target_runtime_execution':False}))


def main():
    signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(TimeoutError('Media smoke exceeded 180 seconds')))
    signal.alarm(180)
    s3, admin = clients()
    blueprint, fixtures = fixture_blueprint(s3, admin)
    run_engine(blueprint, fixtures, s3)


if __name__ == '__main__':
    main()
