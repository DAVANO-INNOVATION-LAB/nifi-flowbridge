"""Native SeaTunnel 2.3.13 Kafka job emission at a verified source boundary.

The caller must obtain offsets from a trusted, halted and drained source. Emission
alone is not deployment, source fencing, or evidence of runtime equivalence.
"""
import json
from ..core import canonical, text, Invalid

VERSION = '2.3.13'
SOURCE_DOC = 'https://seatunnel.apache.org/docs/2.3.13/connectors/source/Kafka/'
SINK_DOC = 'https://seatunnel.apache.org/docs/2.3.13/connectors/sink/Kafka/'

def export_seatunnel_kafka(flow, start_offsets, consumer_group):
    validated = canonical(flow)
    if not isinstance(start_offsets, dict) or not start_offsets or len(start_offsets) > 1024:
        raise Invalid('invalid_offsets', 'Explicit source partition boundaries are required.')
    if any(type(p) is not int or p < 0 or type(o) is not int or o < 0 for p, o in start_offsets.items()):
        raise Invalid('invalid_offsets', 'Partition and next-offset values must be nonnegative integers.')
    text(consumer_group)
    source, sink = validated['source'], validated['sink']
    if source['brokers'] == sink['brokers'] and source['topic'] == sink['topic']:
        raise Invalid('feedback_loop', 'Source and destination topics must be separate.')
    quoted = json.dumps
    offsets = '\n'.join('      ' + quoted(source['topic'] + '-' + str(p)) + ' = ' + str(o) for p, o in sorted(start_offsets.items()))
    job = f'''env {{
  job.mode = "STREAMING"
  parallelism = 1
  checkpoint.interval = 1000
}}
source {{
  Kafka {{
    bootstrap.servers = {quoted(source['brokers'])}
    topic = {quoted(source['topic'])}
    consumer.group = {quoted(consumer_group)}
    start_mode = "specific_offsets"
    start_mode.offsets {{
{offsets}
    }}
    format = "NATIVE"
    format_error_handle_way = "fail"
    commit_on_checkpoint = true
    poll.timeout = 1000
  }}
}}
sink {{
  Kafka {{
    bootstrap.servers = {quoted(sink['brokers'])}
    topic = {quoted(sink['topic'])}
    format = "NATIVE"
    kafka.config {{
      acks = "all"
    }}
  }}
}}
'''
    return {'target': 'seatunnel', 'version': VERSION, 'files': {'seatunnel.conf': job},
            'source_offsets': {str(k): v for k, v in sorted(start_offsets.items())},
            'source_fence_required': True, 'native_runtime_verified': False,
            'limitations': ['Kafka transport only; arbitrary NiFi transformations are unsupported.',
                            'Offset partition completeness must be checked against the live broker.',
                            'This artifact does not fence or deploy either runtime.',
                            'At-least-once handoff; record metadata compatibility needs runtime reconciliation.'],
            'sources': [SOURCE_DOC, SINK_DOC]}


def seatunnel_job_document(flow, start_offsets, consumer_group):
    """REST JSON equivalent of the constrained native HOCON job."""
    export_seatunnel_kafka(flow, start_offsets, consumer_group)
    source, sink = flow['source'], flow['sink']
    return {'env': {'job.mode': 'STREAMING', 'parallelism': 1, 'checkpoint.interval': 1000},
            'source': [{'plugin_name': 'Kafka', 'plugin_output': 'native_records',
                        'bootstrap.servers': source['brokers'], 'topic': source['topic'],
                        'consumer.group': consumer_group, 'start_mode': 'specific_offsets',
                        'start_mode.offsets': {source['topic']+'-'+str(p): o for p,o in sorted(start_offsets.items())},
                        'format': 'NATIVE', 'format_error_handle_way': 'fail',
                        'commit_on_checkpoint': True, 'poll.timeout': 1000}],
            'sink': [{'plugin_name': 'Kafka', 'plugin_input': ['native_records'],
                      'bootstrap.servers': sink['brokers'], 'topic': sink['topic'],
                      'format': 'NATIVE', 'kafka.config': {'acks': 'all'}}]}


class SeaTunnelNativeClient:
    """Documented Zeta REST v2 actions using the bounded verified-TLS JsonClient.

    Base URL is the configured REST context, e.g. https://host:8080/seatunnel.
    Mutations require explicit allow_apply; submitted does not mean RUNNING.
    """
    def __init__(self, client):
        self.client = client

    @staticmethod
    def _job_id(value):
        from ..live.platforms import fail
        if isinstance(value, bool) or not isinstance(value, (int, str)) or not str(value).isdigit() or not 0 < int(value) < 2**63:
            fail('invalid_job_id', 'SeaTunnel job identifier must be a positive signed 64-bit integer.')
        return str(value)

    def inspect(self, job_id=None):
        from ..live.platforms import fail
        if job_id is None:
            data=self.client.request('GET','/overview')
            version=data.get('projectVersion')
            if not isinstance(version,str):fail('invalid_response','SeaTunnel overview did not identify a runtime version.')
            return {'platform':'seatunnel','version':version,'connected':True,
                    'supports_bounded_submission':version==VERSION,'native_migration_verified':False}
        identifier=self._job_id(job_id)
        data=self.client.request('GET','/job-info/'+identifier)
        if str(data.get('jobId'))!=identifier or not isinstance(data.get('jobStatus'),str):
            fail('invalid_response','SeaTunnel job status response was incomplete.')
        return {'platform':'seatunnel','job_id':identifier,'status':data['jobStatus'],
                'terminal':data['jobStatus'] in ('FINISHED','FAILED','CANCELED','CANCELLED')}

    def submit(self, flow, start_offsets, consumer_group, allow_apply=False):
        from ..live.platforms import fail
        document=seatunnel_job_document(flow,start_offsets,consumer_group)
        if allow_apply is not True:fail('apply_required','Explicit apply authorization is required to submit a native job.')
        if not self.inspect()['supports_bounded_submission']:
            fail('unsupported_runtime','Only the pinned SeaTunnel 2.3.13 runtime is supported for this job.')
        result=self.client.request('POST','/submit-job',document,query={'format':'json','jobName':flow['name']})
        identifier=self._job_id(result.get('jobId'))
        return {'platform':'seatunnel','job_id':identifier,'submitted':True,'running_verified':False}

    def stop(self, job_id, allow_apply=False):
        from ..live.platforms import fail
        identifier=self._job_id(job_id)
        if allow_apply is not True:fail('apply_required','Explicit apply authorization is required to stop a native job.')
        result=self.client.request('POST','/stop-job',{'jobId':int(identifier),'isStopWithSavePoint':False})
        if str(result.get('jobId'))!=identifier:fail('invalid_response','SeaTunnel did not acknowledge the requested job.')
        return {'platform':'seatunnel','job_id':identifier,'stop_requested':True,'stopped_verified':False}


# Exact defaults observed from the actual NiFi 2.12.0 fixture.
_NATIVE_DEFAULTS = {'PublishKafka': {'compression.type': 'none', 'acks': 'all', 'Message Demarcator': None, 'Kafka Key Attribute Encoding': 'utf-8', 'Record Reader': None, 'Record Metadata Strategy': 'FROM_PROPERTIES', 'Transactional ID Prefix': None, 'Header Encoding': 'UTF-8', 'max.request.size': '1 MB', 'Topic Name': 'target_topic', 'Kafka Key': None, 'partition': None, 'Kafka Connection Service': '4e13c021-be5c-3203-b3d2-9b24e940aa2b', 'Publish Strategy': 'USE_VALUE', 'Record Key Writer': None, 'Failure Strategy': 'Route to Failure', 'partitioner.class': 'org.apache.kafka.clients.producer.internals.DefaultPartitioner', 'Record Writer': None, 'Transactions Enabled': 'false', 'Message Key Field': None, 'FlowFile Attribute Header Pattern': None}, 'ConsumeKafka': {'Topics': 'source_topic', 'Commit Offsets': 'true', 'Header Name Pattern': None, 'Header Name Prefix': None, 'Key Format': 'byte-array', 'Key Record Reader': None, 'Message Demarcator': None, 'Record Reader': None, 'Key Attribute Encoding': 'utf-8', 'Max Uncommitted Size': None, 'Topic Format': 'names', 'Header Format': 'string', 'Header Encoding': 'UTF-8', 'Max Uncommitted Time': '100 millis', 'Schema Conflict Resolution': 'CREATE_NEW_FLOWFILE', 'Kafka Connection Service': '4e13c021-be5c-3203-b3d2-9b24e940aa2b', 'Separate By Key': 'false', 'Processing Strategy': 'FLOW_FILE', 'Record Writer': None, 'Group ID': 'nifi-proof-blue', 'auto.offset.reset': 'earliest', 'Output Strategy': 'USE_VALUE'}, 'Kafka3ConnectionService': {'ack.wait.time': '5 sec', 'sasl.username': None, 'max.poll.records': '10000', 'bootstrap.servers': 'kafka:9092', 'sasl.kerberos.service.name': None, 'security.protocol': 'PLAINTEXT', 'SSL Context Service': None, 'sasl.mechanism': 'GSSAPI', 'isolation.level': 'read_committed', 'oauth2-access-token-provider-service': None, 'max.block.ms': '5 sec', 'kerberos-user-service': None, 'default.api.timeout.ms': '60 sec'}}


def flow_from_nifi(document):
    """Strict two-node modern NiFi 2.12 profile; review data contract before apply."""
    def reject():raise Invalid('unsupported_native_nifi_profile','Only the pinned two-processor, plaintext, value-only Kafka profile is supported.')
    if not isinstance(document,dict):reject()
    if document.get('nifiVersion', '2.12.0') not in ('2', '2.12.0'):reject()
    group=document.get('flowContents')
    if not isinstance(group,dict):reject()
    if any(document.get(k) for k in ('parameterContexts','externalControllerServices')):reject()
    if any(group.get(k) for k in ('processGroups','remoteProcessGroups','inputPorts','outputPorts','funnels','parameterContextName')):reject()
    processors=group.get('processors',[]);services=group.get('controllerServices',[])
    if len(processors)!=2 or len(services)!=1:reject()
    by_type={p.get('type'):p for p in processors}
    consume=by_type.get('org.apache.nifi.kafka.processors.ConsumeKafka');publish=by_type.get('org.apache.nifi.kafka.processors.PublishKafka')
    service=services[0]
    if not consume or not publish or service.get('type')!='org.apache.nifi.kafka.service.Kafka3ConnectionService':reject()
    for component,artifact in [(consume,'nifi-kafka-nar'),(publish,'nifi-kafka-nar'),(service,'nifi-kafka-3-service-nar')]:
        if component.get('bundle')!={'group':'org.apache.nifi','artifact':artifact,'version':'2.12.0'} or component.get('annotationData'):reject()
        if component.get('executionNode','ALL')!='ALL' or component.get('concurrentlySchedulableTaskCount',1)!=1:reject()
        kind=component['type'].split('.')[-1];props=component.get('properties')
        if not isinstance(props,dict):reject()
        variable={'ConsumeKafka':{'Topics','Group ID','Kafka Connection Service'},'PublishKafka':{'Topic Name','Kafka Connection Service'},'Kafka3ConnectionService':{'bootstrap.servers'}}[kind]
        if set(props)-set(_NATIVE_DEFAULTS[kind]):reject()
        for key,default in _NATIVE_DEFAULTS[kind].items():
            if key not in variable and props.get(key)!=default:reject()
    service_id=service.get('identifier')
    if not service_id or any(p['properties'].get('Kafka Connection Service')!=service_id for p in (consume,publish)):reject()
    c_id,p_id=consume.get('identifier'),publish.get('identifier')
    if not c_id or not p_id or c_id==p_id:reject()
    edges=group.get('connections',[])
    for edge in edges:
        if edge.get('flowFileExpiration', '0 sec') != '0 sec' or edge.get('prioritizers') or edge.get('loadBalanceStrategy', 'DO_NOT_LOAD_BALANCE') != 'DO_NOT_LOAD_BALANCE':reject()
    actual=[(e.get('source',{}).get('id'),e.get('destination',{}).get('id'),e.get('selectedRelationships')) for e in edges]
    if len(actual)!=2 or (c_id,p_id,['success']) not in actual or (p_id,p_id,['failure']) not in actual:reject()
    if consume.get('autoTerminatedRelationships') or publish.get('autoTerminatedRelationships')!=['success']:reject()
    brokers=service['properties']['bootstrap.servers']
    flow={'schema':'flowbridge/v1','name':'nifi-native-kafka-handoff','source':{'type':'kafka','brokers':brokers,'topic':consume['properties']['Topics'],'group':consume['properties']['Group ID'],'offset':'earliest'},'sink':{'type':'kafka','brokers':brokers,'topic':publish['properties']['Topic Name']}}
    return canonical(flow)


def export_from_nifi(document, start_offsets, consumer_group):
    flow = flow_from_nifi(document)
    if set(start_offsets) != {0}:
        raise Invalid('unsupported_partition_contract', 'The verified native profile requires exactly partition zero.')
    artifact=export_seatunnel_kafka(flow,start_offsets,consumer_group)
    artifact['flow']=flow
    artifact['data_contract']={'partitions':1,'non_null_values_only':True,'keys_must_be_null':True,'headers_must_be_empty':True,'timestamp_equivalence':False}
    artifact['limitations'].append('The live broker must verify the single-partition, non-null value-only data contract; record timestamps are not equivalent.')
    return artifact
