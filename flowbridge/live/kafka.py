"""Bounded, at-least-once Kafka copying with explicit offset translation.

Requires confluent-kafka==2.15.1 (Apache-2.0). No source group commits, topic
creation, producer fencing or consumer shutdown is performed by this module.
The coordinator owns authorization, durable checkpoints and freeze gates.
"""
from __future__ import annotations

from dataclasses import dataclass
import copy
import logging
import re
import time
import uuid

CLIENT_REQUIREMENT = 'confluent-kafka==2.15.1'
MAX_RECORD_BYTES = 1024 * 1024
MAX_PLAN_PARTITIONS = 256
MAX_TRANSLATION_RECORDS = 1_000_000
_NAME = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]{0,248}$')


class KafkaBridgeError(RuntimeError):
    """A safe, credential-free failure, suitable for a job status report."""
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


def _fail(code, message):
    raise KafkaBridgeError(code, message) from None


def _name(value):
    if not isinstance(value, str) or not _NAME.fullmatch(value) or value in ('.', '..'):
        _fail('invalid_name', 'Use a valid fixed Kafka topic or group name.')
    return value


def endpoint_config(endpoint):
    """Validate a small connection allowlist; credentials remain in memory only."""
    allowed = {'brokers', 'security_protocol', 'username', 'password', 'sasl_mechanism', 'ssl_ca_pem'}
    if not isinstance(endpoint, dict) or set(endpoint) - allowed:
        _fail('endpoint_config', 'Unsupported Kafka connection settings.')
    brokers = endpoint.get('brokers')
    if not isinstance(brokers, str) or len(brokers) > 2048 or not all(
        re.fullmatch(r'(?:[A-Za-z0-9.-]+|\[[A-Fa-f0-9:]+\]):[0-9]{1,5}', part)
        and 0 < int(part.rsplit(':', 1)[1]) <= 65535 for part in brokers.split(',')
    ):
        _fail('endpoint_config', 'Kafka brokers must be host:port addresses.')
    protocol = endpoint.get('security_protocol', 'SSL')
    if protocol not in ('SSL', 'SASL_SSL', 'PLAINTEXT'):
        _fail('endpoint_config', 'Use SSL or SASL_SSL; PLAINTEXT is available only when explicitly selected.')
    config = {'bootstrap.servers': brokers, 'security.protocol': protocol,
              'socket.timeout.ms': 10000, 'log_level': 0,
              'receive.message.max.bytes': 2 * MAX_RECORD_BYTES + 65536}
    if protocol != 'PLAINTEXT':
        config.update({'enable.ssl.certificate.verification': True,
                       'ssl.endpoint.identification.algorithm': 'https'})
    if endpoint.get('ssl_ca_pem'):
        pem = endpoint['ssl_ca_pem']
        if protocol == 'PLAINTEXT' or not isinstance(pem, str) or len(pem) > 65536 or '-----BEGIN CERTIFICATE-----' not in pem:
            _fail('endpoint_config', 'Supply a PEM CA certificate only for a TLS connection.')
        config['ssl.ca.pem'] = pem
    if protocol == 'SASL_SSL':
        mechanism = endpoint.get('sasl_mechanism', 'SCRAM-SHA-512')
        if mechanism not in ('PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'):
            _fail('endpoint_config', 'Unsupported SASL mechanism.')
        for field in ('username', 'password'):
            if not isinstance(endpoint.get(field), str) or not endpoint[field] or len(endpoint[field]) > 4096:
                _fail('endpoint_config', 'SASL requires a username and password.')
        config.update({'sasl.mechanism': mechanism, 'sasl.username': endpoint['username'], 'sasl.password': endpoint['password']})
    elif any(endpoint.get(k) for k in ('username', 'password', 'sasl_mechanism')):
        _fail('endpoint_config', 'Credentials require SASL_SSL.')
    return config


@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: list[tuple[str, bytes | None]]
    timestamp: int

    @property
    def size(self):
        return len(self.key or b'') + len(self.value or b'') + sum(len(k.encode('utf-8')) + len(v or b'') for k, v in self.headers)


class KafkaAdapter:
    """Real client adapter. Fakes need the same public methods, not this class.

    metadata(topic) -> {cluster_id, partitions:{int:{low,high}}, cleanup_policy,
                        timestamp_type}
    records(topic, partition, start, stop, timeout, cancelled) -> Record iterator
    deliver(record, target_topic, target_partition, timeout) -> target offset
    group_state(group) -> {state, members}; group_offsets(group, topic) -> {p:offset}
    alter_group_offsets(group, topic, offsets) -> None; close() -> None
    """
    def __init__(self, endpoint, timeout=10):
        config = endpoint_config(endpoint)
        if not isinstance(timeout, (int, float)) or not 1 <= timeout <= 30:
            _fail('invalid_timeout', 'Client timeout must be between one and thirty seconds.')
        self.timeout = timeout
        try:
            import confluent_kafka as ck
            from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType
        except ImportError:
            _fail('missing_dependency', 'Install the pinned live Kafka dependency before connecting.')
        self.ck, self.ConfigResource, self.ResourceType = ck, ConfigResource, ResourceType
        logger = logging.getLogger('flowbridge.kafka.silent')
        logger.handlers = [logging.NullHandler()]
        logger.propagate = False
        try:
            self.admin = AdminClient(config, logger=logger)
            self.consumer = ck.Consumer(dict(config, **{
                'group.id': 'flowbridge-copy-' + uuid.uuid4().hex,
                'enable.auto.commit': False, 'enable.auto.offset.store': False,
                'allow.auto.create.topics': False, 'enable.partition.eof': True,
                'auto.offset.reset': 'error', 'isolation.level': 'read_committed',
                'queued.max.messages.kbytes': 2048,
                'fetch.message.max.bytes': MAX_RECORD_BYTES + 65536,
                'fetch.max.bytes': MAX_RECORD_BYTES + 65536,
            }), logger=logger)
            self.producer = ck.Producer(dict(config, **{
                'enable.idempotence': True, 'acks': 'all',
                'message.timeout.ms': int(timeout * 1000),
                'message.max.bytes': MAX_RECORD_BYTES + 65536,
                'queue.buffering.max.kbytes': 2048,
            }), logger=logger)
        except Exception:
            _fail('client_initialization', 'Kafka client initialization failed; verify the connection settings.')

    def metadata(self, topic):
        _name(topic)
        try:
            # Never request an unknown topic by name: list_topics(topic=...) can
            # trigger broker-side auto-creation. Inspect full metadata instead.
            cluster = self.admin.list_topics(timeout=self.timeout)
            item = cluster.topics.get(topic)
            if item is None or item.error or not cluster.cluster_id:
                _fail('topic_missing', 'The topic must already exist and be accessible.')
            if not 1 <= len(item.partitions) <= MAX_PLAN_PARTITIONS:
                _fail('partition_limit', 'The topic partition count exceeds this live-transfer limit.')
            partitions = {}
            for number, metadata in sorted(item.partitions.items()):
                if metadata.error or metadata.leader < 0:
                    _fail('partition_unavailable', 'A topic partition has no available leader.')
                low, high = self.consumer.get_watermark_offsets(self.ck.TopicPartition(topic, number), timeout=self.timeout, cached=False)
                partitions[number] = {'low': low, 'high': high}
            resource = self.ConfigResource(self.ResourceType.TOPIC, topic)
            configs = self.admin.describe_configs([resource], request_timeout=self.timeout)[resource].result(self.timeout)
            return {'cluster_id': cluster.cluster_id, 'partitions': partitions,
                    'cleanup_policy': configs['cleanup.policy'].value,
                    'timestamp_type': configs['message.timestamp.type'].value}
        except KafkaBridgeError:
            raise
        except Exception:
            _fail('metadata_failed', 'Kafka metadata or topic configuration could not be verified.')

    def records(self, topic, partition, start, stop, timeout, cancelled):
        deadline = time.monotonic() + timeout
        try:
            self.consumer.assign([self.ck.TopicPartition(topic, partition, start)])
            while time.monotonic() < deadline and not cancelled():
                message = self.consumer.poll(min(0.25, max(0, deadline-time.monotonic())))
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == self.ck.KafkaError._PARTITION_EOF:
                        return
                    _fail('consume_failed', 'Kafka could not read the planned partition range.')
                if message.offset() >= stop:
                    return
                timestamp_type, timestamp = message.timestamp()
                if timestamp_type == self.ck.TIMESTAMP_NOT_AVAILABLE or timestamp < 0:
                    _fail('timestamp_unavailable', 'A source record has no preservable timestamp.')
                yield Record(message.topic(), message.partition(), message.offset(), message.key(), message.value(), message.headers() or [], timestamp)
            if not cancelled():
                _fail('copy_timeout', 'The bounded copy step timed out before reaching the planned source offset.')
        except KafkaBridgeError:
            raise
        except Exception:
            _fail('consume_failed', 'Kafka consumption failed; inspect broker connectivity and permissions.')
        finally:
            try:
                self.consumer.unassign()
            except Exception:
                pass

    def deliver(self, record, target_topic, target_partition, timeout):
        outcome = []
        def delivered(error, message):
            outcome.append(None if error else message.offset())
        try:
            self.producer.produce(target_topic, partition=target_partition,
                                  key=record.key, value=record.value,
                                  headers=record.headers, timestamp=record.timestamp,
                                  on_delivery=delivered)
            remaining = self.producer.flush(timeout)
            if remaining or not outcome or outcome[0] is None or outcome[0] < 0:
                _fail('delivery_unconfirmed', 'Target delivery was not confirmed; retry may duplicate a delivered record.')
            return outcome[0]
        except KafkaBridgeError:
            raise
        except Exception:
            _fail('delivery_failed', 'Kafka target delivery failed; retry may duplicate a delivered record.')

    def group_state(self, group):
        _name(group)
        try:
            result = self.admin.describe_consumer_groups([group], request_timeout=self.timeout)[group].result(self.timeout)
            return {'state': result.state.name, 'members': len(result.members)}
        except Exception as exc:
            if isinstance(exc, self.ck.KafkaException) and exc.args and exc.args[0].code() == getattr(self.ck.KafkaError, 'GROUP_ID_NOT_FOUND', -99999):
                return {'state':'ABSENT', 'members':0}
            _fail('group_state_failed', 'Consumer group state could not be verified.')

    def group_offsets(self, group, topic):
        _name(group); _name(topic)
        try:
            request = self.ck.ConsumerGroupTopicPartitions(group)
            result = self.admin.list_consumer_group_offsets([request], require_stable=True, request_timeout=self.timeout)[group].result(self.timeout)
            offsets = {}
            for item in result.topic_partitions or []:
                if item.topic != topic:
                    continue
                if item.error:
                    _fail('group_offsets_failed', 'Consumer group offsets contain a partition error.')
                if item.offset >= 0:
                    offsets[item.partition] = item.offset
            return offsets
        except KafkaBridgeError:
            raise
        except Exception:
            _fail('group_offsets_failed', 'Consumer group committed offsets could not be read.')

    def alter_group_offsets(self, group, topic, offsets):
        _inactive(self, group)
        try:
            request = self.ck.ConsumerGroupTopicPartitions(group, [self.ck.TopicPartition(topic, p, value) for p, value in sorted(offsets.items())])
            result = self.admin.alter_consumer_group_offsets([request], request_timeout=self.timeout)[group].result(self.timeout)
            if any(item.error for item in result.topic_partitions or []):
                _fail('offset_update_failed', 'A target consumer group offset update failed; inspect current offsets before retrying.')
        except KafkaBridgeError:
            raise
        except Exception:
            _fail('offset_update_failed', 'Target offset update failed or is ambiguous; inspect current offsets before retrying.')
        if self.group_offsets(group, topic) != offsets:
            _fail('offset_verification_failed', 'Target consumer group offsets did not match after the update.')

    def close(self):
        try:
            self.consumer.close()  # enable.auto.commit=False: never commits.
            self.producer.purge(in_queue=True, in_flight=True, blocking=False)
            self.producer.poll(0)
        except Exception:
            pass


def _meta(adapter, topic):
    result = adapter.metadata(topic)
    if result.get('cleanup_policy') != 'delete':
        _fail('compaction_unsupported', 'Compacted topics are not supported by this contiguous-offset transfer mode.')
    if result.get('timestamp_type') != 'CreateTime':
        _fail('timestamp_policy', 'Both topics must use CreateTime to preserve record timestamps.')
    if not result.get('cluster_id') or not isinstance(result.get('partitions'), dict) or not 1 <= len(result['partitions']) <= MAX_PLAN_PARTITIONS:
        _fail('invalid_metadata', 'Kafka partition metadata is incomplete.')
    result = copy.deepcopy(result)
    result['partitions'] = {int(p):v for p,v in result['partitions'].items()}
    for p, watermarks in result['partitions'].items():
        if p < 0 or any(type(watermarks.get(k)) is not int for k in ('low','high')) or not 0 <= watermarks['low'] <= watermarks['high']:
            _fail('invalid_metadata', 'Kafka watermarks are invalid.')
    return result


def preflight(source, target, source_topic, target_topic, partition_map=None):
    _name(source_topic); _name(target_topic)
    left, right = _meta(source, source_topic), _meta(target, target_topic)
    if left['cluster_id'] == right['cluster_id'] and source_topic == target_topic:
        _fail('feedback_loop', 'Source and target must not be the same topic in the same cluster.')
    mapping = partition_map if partition_map is not None else {p:p for p in left['partitions']}
    try:
        mapping = {int(k):int(v) for k,v in mapping.items()}
    except (TypeError, ValueError, AttributeError):
        _fail('partition_map', 'Provide a complete one-to-one partition mapping.')
    if set(mapping) != set(left['partitions']) or set(mapping.values()) != set(right['partitions']) or len(mapping) != len(set(mapping.values())):
        _fail('partition_map', 'Source and target require matching partition counts and a complete one-to-one mapping.')
    return {'source_topic':source_topic, 'target_topic':target_topic,
            'source_cluster_id':left['cluster_id'], 'target_cluster_id':right['cluster_id'],
            'partitions':[{'source_partition':p,'target_partition':mapping[p],
                           'source_low':left['partitions'][p]['low'], 'source_high':left['partitions'][p]['high'],
                           'target_low':right['partitions'][mapping[p]]['low'], 'target_high':right['partitions'][mapping[p]]['high']}
                          for p in sorted(mapping)]}


def refresh_plan(source, target, plan):
    """Refresh source high watermarks while preserving original target baselines."""
    fresh = preflight(source, target, plan['source_topic'], plan['target_topic'],
                      {p['source_partition']:p['target_partition'] for p in plan['partitions']})
    if any(fresh[k] != plan[k] for k in ('source_cluster_id','target_cluster_id')):
        _fail('cluster_changed', 'Kafka cluster identity changed since preflight.')
    result = copy.deepcopy(plan)
    for original, current in zip(result['partitions'], fresh['partitions']):
        if current['source_low'] != original['source_low'] or current['source_high'] < original['source_high']:
            _fail('retention_changed', 'Source retention or truncation changed the planned range; create a new reviewed plan.')
        if current['target_low'] > original['target_high'] or current['target_high'] < original['target_high']:
            _fail('target_changed', 'Target retention or truncation invalidated the planned baseline.')
        original['source_high'] = current['source_high']
    return result


def copy_snapshot(source, target, plan, positions=None, on_delivery=None,
                  cancelled=lambda:False, max_records=10000, max_bytes=64*1024*1024,
                  timeout=60):
    """Copy one bounded step. Persist each delivery before advancing a position.

    on_delivery receives only offset metadata (no payload/secrets). Its return
    must mean durable success. If it raises after delivery, crash recovery may
    produce a duplicate: exactly-once delivery is not claimed.
    """
    if not callable(on_delivery):
        _fail('checkpoint_required', 'A durable delivery checkpoint callback is required.')
    if type(max_records) is not int or not 1 <= max_records <= 100000 or type(max_bytes) is not int or not 1 <= max_bytes <= 256*1024*1024 or not isinstance(timeout,(int,float)) or not 1 <= timeout <= 300:
        _fail('copy_limits', 'Copy limits exceed the allowed bounded step.')
    positions = {int(k):int(v) for k,v in (positions or {}).items()}
    deadline = time.monotonic()+timeout
    delivered, total_bytes = 0,0
    for part in plan['partitions']:
        p=part['source_partition']
        positions.setdefault(p,part['source_low'])
        if not part['source_low'] <= positions[p] <= part['source_high']:
            _fail('checkpoint_range', 'A saved source position is outside the planned range.')
    def result():
        return {'positions':dict(positions),'delivered':delivered,'bytes':total_bytes,
                'complete':all(positions[p['source_partition']]==p['source_high'] for p in plan['partitions']),
                'cancelled':bool(cancelled())}
    for part in plan['partitions']:
        p=part['source_partition']
        if positions[p] == part['source_high']:
            continue
        if cancelled() or time.monotonic() >= deadline:
            return result()
        records=source.records(plan['source_topic'],p,positions[p],part['source_high'],max(0.01,deadline-time.monotonic()),cancelled)
        try:
            for record in records:
                if cancelled() or delivered>=max_records or time.monotonic()>=deadline:
                    return result()
                if record.topic != plan['source_topic'] or record.partition != p or record.offset != positions[p]:
                    _fail('source_gap', 'Source offsets contain a gap or unexpected record; no unmapped offset may be skipped.')
                if record.size>MAX_RECORD_BYTES:
                    _fail('record_limit', 'A Kafka record exceeds the one-MiB copy limit.')
                if total_bytes+record.size>max_bytes:
                    return result()
                target_offset=target.deliver(record,plan['target_topic'],part['target_partition'],max(0.01,deadline-time.monotonic()))
                if type(target_offset) is not int or target_offset<0:
                    _fail('delivery_unconfirmed', 'The target did not return a confirmed record offset.')
                mapping={'source_topic':plan['source_topic'],'source_partition':p,'source_offset':record.offset,
                         'target_topic':plan['target_topic'],'target_partition':part['target_partition'],'target_offset':target_offset}
                try:
                    on_delivery(mapping)
                except Exception:
                    _fail('checkpoint_failed', 'Target delivery succeeded but its checkpoint failed; retry can duplicate the record.')
                positions[p]=record.offset+1
                delivered+=1; total_bytes+=record.size
                if positions[p]==part['source_high'] or delivered>=max_records:
                    break
        finally:
            close=getattr(records,'close',None)
            if close:close()
        if cancelled() or delivered>=max_records:
            return result()
        if positions[p]<part['source_high']:
            _fail('source_gap', 'The source range ended before its planned high watermark; gaps and unresolved transactions require review.')
    return result()


def tail_step(source, target, plan, **kwargs):
    fresh=refresh_plan(source,target,plan)
    result=copy_snapshot(source,target,fresh,**kwargs)
    result['plan']=fresh
    return result


def _inactive(adapter, group):
    state=adapter.group_state(group)
    if state.get('members') != 0 or state.get('state') not in ('EMPTY','DEAD','ABSENT'):
        _fail('active_group', 'A named consumer group is active or its state is uncertain; stop its consumers before cutover.')
    return state


def translate_group_offsets(source, target, plan, groups, lookup_mapping):
    """Preview named-group offset changes, rejecting gaps and foreign writes.

    lookup_mapping(source_topic, source_partition, source_offset) returns the
    exact mapping dict previously passed to on_delivery, or None.
    """
    if not isinstance(groups,list) or len(groups)>20:
        _fail('group_selection', 'Specify at most twenty distinct consumer groups for cutover.')
    for group in groups:_name(group)
    if len(set(groups))!=len(groups):
        _fail('group_selection', 'Consumer group names must be distinct.')
    if sum(p['source_high']-p['source_low'] for p in plan['partitions'])>MAX_TRANSLATION_RECORDS:
        _fail('translation_limit', 'This cutover exceeds the bounded one-million-record mapping verification limit.')
    fresh=refresh_plan(source,target,plan)
    if fresh != plan:
        _fail('source_not_quiescent', 'Source high watermarks changed; freeze producers and finish the final copy before cutover.')
    target_meta=_meta(target,plan['target_topic'])
    endpoints={}
    for part in plan['partitions']:
        previous=part['target_high']-1
        for offset in range(part['source_low'],part['source_high']):
            mapping=lookup_mapping(plan['source_topic'],part['source_partition'],offset)
            expected={'source_topic':plan['source_topic'],'source_partition':part['source_partition'],'source_offset':offset,
                      'target_topic':plan['target_topic'],'target_partition':part['target_partition']}
            if not isinstance(mapping,dict) or any(mapping.get(k)!=v for k,v in expected.items()) or type(mapping.get('target_offset')) is not int:
                _fail('unmapped_offset', 'A source offset lacks a verified delivery mapping; cutover is blocked.')
            if mapping['target_offset']!=previous+1:
                _fail('target_gap', 'Destination offsets contain duplicates or foreign writes; cutover needs manual reconciliation.')
            previous=mapping['target_offset']
        end=previous+1
        if target_meta['partitions'][part['target_partition']]['high']!=end or target_meta['partitions'][part['target_partition']]['low']>part['target_high']:
            _fail('target_changed', 'The destination range changed outside the verified copy; cutover is blocked.')
        endpoints[part['source_partition']]=end
    preview=[]
    for group in groups:
        _inactive(source,group);state=_inactive(target,group)
        source_offsets=source.group_offsets(group,plan['source_topic'])
        old_target={} if state['state']=='ABSENT' else target.group_offsets(group,plan['target_topic'])
        if set(source_offsets)!={p['source_partition'] for p in plan['partitions']}:
            _fail('group_offsets_incomplete', 'Every planned source partition must have a committed position for each selected group.')
        translated={}
        for part in plan['partitions']:
            p=part['source_partition'];position=source_offsets[p]
            if type(position) is not int or not part['source_low']<=position<=part['source_high']:
                _fail('group_offset_range', 'A consumer group position is outside the copied source range.')
            end=endpoints[p]
            translated[part['target_partition']]=end if position==part['source_high'] else lookup_mapping(plan['source_topic'],p,position)['target_offset']
        preview.append({'group':group,'source_offsets':source_offsets,'target_previous_offsets':old_target,'target_offsets':translated})
    return {'source_topic':plan['source_topic'],'target_topic':plan['target_topic'],'groups':preview}


def apply_group_offsets(source, target, preview):
    """Apply an approved preview. Coordinator must ensure the freeze gate holds.

    Multiple groups are not transactional: a failure can leave a partial update.
    A coordinator must persist each returned/observed target offset and inspect
    ambiguous failures before retrying. This function never updates the source.
    """
    preview=copy.deepcopy(preview)
    for item in preview['groups']:
        for key in ('source_offsets','target_previous_offsets','target_offsets'):
            item[key]={int(k):v for k,v in item[key].items()}
    def check(item):
        _inactive(source,item['group']);state=_inactive(target,item['group'])
        if source.group_offsets(item['group'],preview['source_topic']) != item['source_offsets']:
            _fail('group_moved', 'A source consumer group changed after the cutover preview.')
        current={} if state['state']=='ABSENT' else target.group_offsets(item['group'],preview['target_topic'])
        if current!=item['target_previous_offsets']:
            _fail('target_group_moved', 'A target consumer group changed after the cutover preview.')
    for item in preview['groups']:
        check(item)
    applied=[]
    for item in preview['groups']:
        check(item)
        target.alter_group_offsets(item['group'],preview['target_topic'],item['target_offsets'])
        if target.group_offsets(item['group'],preview['target_topic'])!=item['target_offsets']:
            _fail('offset_verification_failed', 'Applied target offsets could not be verified; inspect before retrying.')
        applied.append(item['group'])
    return {'applied_groups':applied,'source_offsets_modified':False}
