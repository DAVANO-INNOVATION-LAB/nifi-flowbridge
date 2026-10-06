"""Strict, offline interchange for one Kafka source connected to one Kafka sink.

Native artifacts are reviewable migration drafts; no target is contacted. Deliberate
rejection is preferable to silently changing the semantics of a NiFi processor.
"""
import json
import re
import uuid

FORMATS = ('nifi', 'seatunnel', 'camel-k', 'kafka', 'flowbridge')
_SECRET = re.compile(r'password|secret|token|credential|sasl|jaas|private.?key', re.I)
_NAME = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,199}$')

class Invalid(ValueError):
    def __init__(self, code, message):
        self.code, self.message = code, message


def fail(code, message):
    raise Invalid(code, message)


def exact(obj, allowed, required=()):
    if not isinstance(obj, dict):
        fail('invalid_shape', 'Expected a JSON object.')
    if set(obj) - set(allowed):
        fail('unsupported_fields', 'The input includes fields outside the supported subset.')
    if not set(required) <= set(obj):
        fail('missing_fields', 'The input is missing required fields.')


def _safe(value, depth=0, count=None):
    if count is None:
        count = [0]
    count[0] += 1
    if count[0] > 10000 or depth > 35:
        fail('input_limit', 'Input exceeds the supported size or nesting limit.')
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                fail('invalid_shape', 'Object keys must be strings.')
            if _SECRET.search(key) and item not in (None, '', False, [], {}):
                fail('secret_configuration', 'Credential configuration must be removed before import; it will not be copied.')
            _safe(item, depth + 1, count)
    elif isinstance(value, list):
        for item in value:
            _safe(item, depth + 1, count)
    elif isinstance(value, str):
        if len(value) > 100000:
            fail('input_limit', 'A field exceeds the supported size limit.')
        if '${' in value or '#{' in value or '{{' in value:
            fail('dynamic_configuration', 'Expressions and parameter references require manual migration.')
        if re.search(r'://[^/\s]+:[^/@\s]+@', value):
            fail('secret_configuration', 'Embedded credentials must be removed before import.')
    elif value is not None and not isinstance(value, (bool, int, float)):
        fail('invalid_shape', 'Only JSON values are supported.')


def text(value, kind='name'):
    if not isinstance(value, str) or not value:
        fail('invalid_value', 'A required text field is empty or has the wrong type.')
    if kind == 'brokers':
        if len(value) > 2048 or not all(re.fullmatch(r'(?:[A-Za-z0-9.-]+|\[[0-9a-fA-F:]+\]):[0-9]{1,5}', b) and 0 < int(b.rsplit(':', 1)[1]) <= 65535 for b in value.split(',')):
            fail('invalid_brokers', 'Brokers must be a comma-separated list of host:port addresses.')
    elif not _NAME.fullmatch(value):
        fail('invalid_name', 'Names, topics and groups must contain only letters, numbers, dots, hyphens or underscores.')
    return value


def canonical(data, schema='flowbridge/v1'):
    exact(data, ('schema', 'name', 'source', 'sink'), ('schema', 'name', 'source', 'sink'))
    if data['schema'] != schema:
        fail('unsupported_version', 'Unsupported interchange schema version.')
    src, dst = data['source'], data['sink']
    exact(src, ('type', 'brokers', 'topic', 'group', 'offset'), ('type', 'brokers', 'topic', 'group', 'offset'))
    exact(dst, ('type', 'brokers', 'topic'), ('type', 'brokers', 'topic'))
    if src['type'] != 'kafka' or dst['type'] != 'kafka':
        fail('unsupported_connector', 'This release supports Kafka source to Kafka sink only.')
    if src['offset'] not in ('earliest', 'latest'):
        fail('unsupported_offset', 'Only earliest or latest offset reset policies are supported.')
    for endpoint in (src, dst):
        text(endpoint['brokers'], 'brokers')
        text(endpoint['topic'])
    text(src['group'])
    text(data['name'])
    if src['brokers'] == dst['brokers'] and src['topic'] == dst['topic']:
        fail('feedback_loop', 'Source and destination must not be the same Kafka topic on the same brokers.')
    return {'schema': 'flowbridge/v1', 'name': data['name'], 'source': dict(src), 'sink': dict(dst)}


def _flow(name, brokers, topic, group, offset, out_brokers, out_topic):
    return canonical({'schema':'flowbridge/v1', 'name':name, 'source':{'type':'kafka','brokers':brokers,'topic':topic,'group':group,'offset':offset}, 'sink':{'type':'kafka','brokers':out_brokers,'topic':out_topic}})


def nifi(data):
    for key in ('externalControllerServices', 'parameterContexts', 'parameterProviders'):
        if data.get(key):
            fail('unsupported_nifi_services', 'External services, parameter contexts and providers require manual migration.')
    group = data.get('flowContents', data.get('rootGroup', data))
    if not isinstance(group, dict):
        fail('invalid_shape', 'NiFi process group must be an object.')
    # Unknown topology and service configuration cannot be discarded.
    for key in ('processGroups','inputPorts','outputPorts','funnels','remoteProcessGroups','controllerServices','parameterContextName','parameterContextIdentifier','variables','flowFileConcurrency','flowFileOutboundPolicy'):
        if group.get(key):
            fail('unsupported_nifi_topology', 'Nested groups, ports, services and parameters require manual migration.')
    nodes, edges = group.get('processors'), group.get('connections')
    if not isinstance(nodes, list) or len(nodes) != 2 or not isinstance(edges, list) or len(edges) != 1:
        fail('unsupported_nifi_topology', 'Exactly two processors and one success connection are supported.')
    endpoints = {}
    ids = set()
    for node in nodes:
        if not isinstance(node, dict):
            fail('invalid_shape', 'NiFi processor must be an object.')
        ident = node.get('identifier', node.get('id'))
        if not isinstance(ident, str) or not ident or ident in ids:
            fail('invalid_graph', 'Processor identifiers must be present and unique.')
        ids.add(ident)
        kind = node.get('type', '').split('.')[-1]
        if kind not in ('ConsumeKafka_2_6','PublishKafka_2_6'):
            fail('unsupported_processor', 'Only ConsumeKafka_2_6 and PublishKafka_2_6 byte-flow processors are supported. Record processors and modern service-based processors need manual mapping.')
        role = 'source' if kind.startswith('Consume') else 'sink'
        if role in endpoints:
            fail('invalid_graph', 'The flow must have one consumer and one publisher.')
        for key, default in (('concurrentlySchedulableTaskCount', 1), ('executionNode', 'ALL'), ('runDurationMillis', 0), ('schedulingStrategy', 'TIMER_DRIVEN')):
            if node.get(key, default) != default:
                fail('unsupported_scheduling', 'Custom scheduling and concurrency require manual migration.')
        if node.get('retryCount', 0) or node.get('retriedRelationships'):
            fail('unsupported_retry', 'Processor retry policies require manual migration.')
        config = node.get('config', {})
        if not isinstance(config, dict):
            fail('invalid_shape', 'NiFi configuration must be an object.')
        for key, default in (('concurrentlySchedulableTaskCount', 1), ('executionNode', 'ALL'), ('runDurationMillis', 0), ('schedulingStrategy', 'TIMER_DRIVEN')):
            if config.get(key, default) != default:
                fail('unsupported_scheduling', 'Custom scheduling and concurrency require manual migration.')
        for settings in (node, config):
            if settings.get('schedulingPeriod', '0 sec') not in ('0 sec', '0 secs', '0 seconds', '0 ms'):
                fail('unsupported_scheduling', 'Custom scheduling periods require manual migration.')
            if settings.get('annotationData') or settings.get('retryCount', 0) or settings.get('retriedRelationships'):
                fail('unsupported_processor_configuration', 'Annotations and retry policies require manual migration.')
        if 'properties' in node and 'properties' in config and node['properties'] != config['properties']:
            fail('ambiguous_configuration', 'Conflicting processor property representations are not supported.')
        props = node.get('properties', config.get('properties', {}))
        allowed = {'bootstrap.servers','topic'} | ({'group.id','auto.offset.reset'} if role == 'source' else set())
        if not isinstance(props, dict) or any(v not in (None, '') for k,v in props.items() if k not in allowed):
            fail('unsupported_processor_configuration', 'Processor settings outside brokers, fixed topic, consumer group and offset reset require manual mapping.')
        endpoints[role] = (ident, props)
    if set(endpoints) != {'source','sink'}:
        fail('invalid_graph', 'The flow must have one consumer and one publisher.')
    edge = edges[0]
    if not isinstance(edge, dict):
        fail('invalid_graph', 'Connection must be an object.')
    def endpoint_id(value):
        if isinstance(value, dict):
            return value.get('id', value.get('identifier'))
        return value
    if endpoint_id(edge.get('source', edge.get('sourceId'))) != endpoints['source'][0] or endpoint_id(edge.get('destination', edge.get('destinationId'))) != endpoints['sink'][0]:
        fail('invalid_graph', 'The only connection must run from consumer to publisher.')
    if edge.get('selectedRelationships') != ['success']:
        fail('unsupported_relationship', 'Only the success relationship can be translated.')
    if edge.get('prioritizers') or edge.get('loadBalanceStrategy') not in (None, 'DO_NOT_LOAD_BALANCE') or edge.get('flowFileExpiration') not in (None,'0 sec','0 secs','0 seconds'):
        fail('unsupported_connection', 'Prioritization, expiration and load balancing require manual migration.')
    a,b = endpoints['source'][1], endpoints['sink'][1]
    return _flow(group.get('name','imported-flow'), a.get('bootstrap.servers'),a.get('topic'),a.get('group.id'),a.get('auto.offset.reset','latest'),b.get('bootstrap.servers'),b.get('topic'))


def seatunnel(data):
    exact(data, ('env','source','sink'), ('env','source','sink'))
    exact(data['env'], ('job.mode','job.name','parallelism'), ('job.mode','job.name','parallelism'))
    if data['env']['job.mode'] != 'STREAMING' or data['env']['parallelism'] != 1:
        fail('unsupported_seatunnel', 'Only streaming mode with parallelism one is supported.')
    for side in ('source','sink'):
        exact(data[side], ('Kafka',), ('Kafka',))
    a,b = data['source']['Kafka'],data['sink']['Kafka']
    exact(a, ('topic','bootstrap.servers','consumer.group','start_mode','format','plugin_output'), ('topic','bootstrap.servers','consumer.group','start_mode','format','plugin_output'))
    exact(b, ('topic','bootstrap.servers','format','plugin_input'), ('topic','bootstrap.servers','format','plugin_input'))
    if a['format'] != 'NATIVE' or b['format'] != 'NATIVE' or a['plugin_output'] != 'events' or b['plugin_input'] != 'events':
        fail('unsupported_seatunnel', 'Only directly connected NATIVE Kafka endpoints are supported.')
    return _flow(data['env']['job.name'],a['bootstrap.servers'],a['topic'],a['consumer.group'],a['start_mode'],b['bootstrap.servers'],b['topic'])


def camel(data):
    exact(data, ('apiVersion','kind','metadata','spec'), ('apiVersion','kind','metadata','spec'))
    exact(data['metadata'], ('name','namespace'), ('name',))
    if 'namespace' in data['metadata'] and not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', str(data['metadata']['namespace'])):
        fail('invalid_namespace', 'Camel K namespace must be a valid namespace name.')
    exact(data['spec'], ('flows',), ('flows',))
    if data['apiVersion'] != 'camel.apache.org/v1' or data['kind'] != 'Integration':
        fail('unsupported_camel', 'Only Camel K v1 Integration resources are supported.')
    flows = data['spec']['flows']
    if not isinstance(flows,list) or len(flows)!=1:
        fail('unsupported_camel', 'Only one direct Kafka route is supported.')
    exact(flows[0], ('from',), ('from',))
    a = flows[0]['from']
    exact(a, ('uri','parameters','steps'), ('uri','parameters','steps'))
    if not isinstance(a['steps'],list) or len(a['steps'])!=1:
        fail('unsupported_camel', 'Transforms and extra steps require manual migration.')
    exact(a['steps'][0], ('to',), ('to',))
    b = a['steps'][0]['to']
    exact(b, ('uri','parameters'), ('uri','parameters'))
    p,q = a['parameters'],b['parameters']
    exact(p, ('brokers','groupId','autoOffsetReset','keyDeserializer','valueDeserializer'), ('brokers','groupId','autoOffsetReset','keyDeserializer','valueDeserializer'))
    exact(q, ('brokers','keySerializer','valueSerializer'), ('brokers','keySerializer','valueSerializer'))
    if any(p[k] != 'org.apache.kafka.common.serialization.ByteArrayDeserializer' for k in ('keyDeserializer','valueDeserializer')) or any(q[k] != 'org.apache.kafka.common.serialization.ByteArraySerializer' for k in ('keySerializer','valueSerializer')):
        fail('unsupported_serialization', 'Only byte-array key and value serialization is supported.')
    if not isinstance(a['uri'],str) or not isinstance(b['uri'],str) or not a['uri'].startswith('kafka:') or not b['uri'].startswith('kafka:'):
        fail('unsupported_connector', 'Only fixed Kafka endpoints are supported.')
    return _flow(data['metadata']['name'],p['brokers'],a['uri'][6:],p['groupId'],p['autoOffsetReset'],q['brokers'],b['uri'][6:])


def analyze(data, source='auto'):
    report = {'ok':False,'source':source if isinstance(source,str) and source in (*FORMATS,'auto') else 'unknown','errors':[],'warnings':[]}
    try:
        if not isinstance(data, dict):
            fail('invalid_shape', 'Import requires a JSON object, not source code or arbitrary configuration text.')
        _safe(data)
        if source == 'auto':
            schema = data.get('schema')
            source = 'flowbridge' if schema == 'flowbridge/v1' else 'kafka' if schema == 'flowbridge-kafka/v1' else 'camel-k' if data.get('kind') == 'Integration' else 'seatunnel' if 'env' in data and 'source' in data else 'nifi' if any(k in data for k in ('flowContents','rootGroup','processors')) else None
        report['source'] = source if isinstance(source,str) and source in FORMATS else 'unknown'
        if source not in FORMATS:
            fail('unknown_format', 'Choose a supported format. Arbitrary Java, Kafka Connect configurations, YAML and HOCON text are not imported by this release.')
        flow = {'nifi':nifi,'seatunnel':seatunnel,'camel-k':camel,'kafka':lambda d:canonical(d,'flowbridge-kafka/v1'),'flowbridge':canonical}[source](data)
        report['ok'] = True
        report['warnings'] = [{'code':'migration_review','message':'Draft migration: review offsets, delivery guarantees, Kafka keys and headers, failure handling, provenance, security and runtime compatibility before deployment. Only byte payload routing is represented.'}]
        if source == 'camel-k' and data.get('metadata', {}).get('namespace'):
            report['warnings'].append({'code':'deployment_namespace','message':'The source namespace is deployment metadata and is not carried into the portable flow. Select the destination namespace explicitly.'})
        return {'report':report,'flow':flow}
    except Invalid as error:
        report['errors'].append({'code':error.code,'message':error.message})
    except (KeyError, TypeError, ValueError, AttributeError, RecursionError):
        report['errors'].append({'code':'invalid_shape','message':'The document does not match the supported format.'})
    return {'report':report,'flow':None}


def _emit(flow, target):
    a,b=flow['source'],flow['sink']
    if target in ('flowbridge','kafka'):
        return dict(flow, schema='flowbridge/v1' if target=='flowbridge' else 'flowbridge-kafka/v1')
    if target=='seatunnel':
        return {'env':{'job.mode':'STREAMING','job.name':flow['name'],'parallelism':1},'source':{'Kafka':{'topic':a['topic'],'bootstrap.servers':a['brokers'],'consumer.group':a['group'],'start_mode':a['offset'],'format':'NATIVE','plugin_output':'events'}},'sink':{'Kafka':{'topic':b['topic'],'bootstrap.servers':b['brokers'],'format':'NATIVE','plugin_input':'events'}}}
    if target=='camel-k':
        return {'apiVersion':'camel.apache.org/v1','kind':'Integration','metadata':{'name':flow['name']},'spec':{'flows':[{'from':{'uri':'kafka:'+a['topic'],'parameters':{'brokers':a['brokers'],'groupId':a['group'],'autoOffsetReset':a['offset'],'keyDeserializer':'org.apache.kafka.common.serialization.ByteArrayDeserializer','valueDeserializer':'org.apache.kafka.common.serialization.ByteArrayDeserializer'},'steps':[{'to':{'uri':'kafka:'+b['topic'],'parameters':{'brokers':b['brokers'],'keySerializer':'org.apache.kafka.common.serialization.ByteArraySerializer','valueSerializer':'org.apache.kafka.common.serialization.ByteArraySerializer'}}}]}}]}}
    if target=='nifi':
        ids = [str(uuid.uuid5(uuid.NAMESPACE_URL,'flowbridge:'+flow['name']+':'+x)) for x in ('group','source','sink','edge')]
        def processor(role, i, props):
            return {'identifier':ids[i],'groupIdentifier':ids[0],'componentType':'PROCESSOR','name':'Consume Kafka' if role=='Consume' else 'Publish Kafka','type':'org.apache.nifi.processors.kafka.pubsub.'+role+'Kafka_2_6','bundle':{'group':'org.apache.nifi','artifact':'nifi-kafka-2-6-nar','version':'1.28.0'},'position':{'x':0,'y':(i-1)*200},'properties':props,'schedulingStrategy':'TIMER_DRIVEN','schedulingPeriod':'0 sec','concurrentlySchedulableTaskCount':1,'executionNode':'ALL','penaltyDuration':'30 sec','yieldDuration':'1 sec','runDurationMillis':0,'autoTerminatedRelationships':[],'scheduledState':'DISABLED'}
        return {'flowEncodingVersion':'1.0','externalControllerServices':{},'parameterContexts':{},'parameterProviders':{},'flowContents':{'identifier':ids[0],'componentType':'PROCESS_GROUP','name':flow['name'],'position':{'x':0,'y':0},'processors':[processor('Consume',1,{'bootstrap.servers':a['brokers'],'topic':a['topic'],'group.id':a['group'],'auto.offset.reset':a['offset']}),processor('Publish',2,{'bootstrap.servers':b['brokers'],'topic':b['topic']})],'connections':[{'identifier':ids[3],'groupIdentifier':ids[0],'componentType':'CONNECTION','name':'success','source':{'id':ids[1],'type':'PROCESSOR','groupId':ids[0]},'destination':{'id':ids[2],'type':'PROCESSOR','groupId':ids[0]},'selectedRelationships':['success'],'backPressureObjectThreshold':10000,'backPressureDataSizeThreshold':'1 GB','flowFileExpiration':'0 sec','prioritizers':[]}],'processGroups':[],'controllerServices':[],'inputPorts':[],'outputPorts':[],'funnels':[],'remoteProcessGroups':[]}}
    fail('unknown_target','Choose a supported export target.')


def convert(data, source='auto', target='flowbridge'):
    result=analyze(data,source)
    result.update({'target':target if isinstance(target,str) and target in FORMATS else 'unknown','files':{}})
    if not result['report']['ok']:
        return result
    if target not in FORMATS:
        result['report']['ok']=False
        result['report']['errors'].append({'code':'unknown_target','message':'Choose a supported export target.'})
        return result
    if target=='camel-k' and not re.fullmatch(r'[a-z][a-z0-9-]{0,62}', result['flow']['name']):
        result['report']['ok']=False
        result['report']['errors'].append({'code':'invalid_kubernetes_name','message':'Camel K export requires a lowercase name starting with a letter and using letters, numbers and hyphens, at most 63 characters.'})
        return result
    artifact=_emit(result['flow'],target)
    filename={'flowbridge':'flowbridge.json','kafka':'kafka-flow.json','seatunnel':'seatunnel.json','camel-k':'integration.json','nifi':'nifi-flow.json'}[target]
    result['files'][filename]=json.dumps(artifact,indent=2,sort_keys=True)+'\n'
    if target=='nifi':
        result['report']['warnings'].append({'code':'nifi_draft','message':'NiFi 1.28.0 draft: processors are stopped. Configure publisher success/failure handling and verify the Kafka bundle before enabling. NiFi 2.x is not supported.'})
    result['files']['migration-report.json']=json.dumps(result['report'],indent=2)+'\n'
    return result
