"""Three-lane media ETL blueprint and reviewable NiFi 2.12.0 flow export.

The reference worker's SQLite ledger is not a portable NiFi checkpoint. Native
NiFi exports are stopped integration templates and explicitly report unresolved
runtime/configuration and cross-system idempotency requirements.
"""
import copy
import json
import re
import uuid
from urllib.parse import urlsplit
from pathlib import Path

SCHEMA = 'flowbridge/media-etl/v1'
NIFI_VERSION = '2.12.0'
_MARKER = 'FLOWBRIDGE_MEDIA_BLUEPRINT_V1\n'
_BUCKET = re.compile(r'^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$')
_NAME = re.compile(r'^[a-z][a-z0-9-]{0,62}$')
_TOPIC = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]{0,248}$')
_SECRET = re.compile(r'password|secret|token|credential|access.?key|authorization', re.I)


class _Invalid(ValueError):
    pass


def _require(condition,message):
    if not condition:raise _Invalid(message)


def _exact(value,keys):
    _require(isinstance(value,dict) and set(value)==set(keys),'Blueprint fields must match the supported media contract exactly.')


def _url(value):
    _require(isinstance(value,str) and len(value)<=2048,'Endpoint URL is invalid.')
    parsed=urlsplit(value)
    _require(parsed.scheme in ('https','http') and parsed.hostname and not parsed.username and not parsed.password and not parsed.query and not parsed.fragment,'Endpoints must be HTTP(S) URLs without credentials, query strings or fragments.')
    try:parsed.port
    except ValueError:raise _Invalid('Endpoint port is invalid.') from None


def _safe(value,depth=0):
    _require(depth<25,'Blueprint nesting exceeds the supported limit.')
    if isinstance(value,dict):
        for key,item in value.items():
            _require(isinstance(key,str) and not (_SECRET.search(key) and item not in (None,'')),'Credentials must be provided separately, never in a media blueprint.')
            _safe(item,depth+1)
    elif isinstance(value,list):
        _require(len(value)<=50,'Blueprint list exceeds the supported limit.')
        for item in value:_safe(item,depth+1)
    elif isinstance(value,str):
        _require(len(value)<=4096 and not any(x in value for x in ('${','#{','{{','-----BEGIN PRIVATE KEY-----')),'Expressions, key material and oversized values are not allowed in media blueprints.')
    else:_require(value is None or isinstance(value,(bool,int)),'Media blueprints accept JSON scalar values only.')


def validate_media(document):
    report={'ok':False,'errors':[],'warnings':[]}
    try:
        _safe(document)
        _exact(document,('schema','name','s3','kafka','delivery','pipelines'))
        _require(document['schema']==SCHEMA,'Unsupported media blueprint schema.')
        _require(isinstance(document['name'],str) and _NAME.fullmatch(document['name']),'Blueprint name must be a lowercase DNS-style name.')
        s3=document['s3'];_exact(s3,('endpoint','region','path_style_access'));_url(s3['endpoint'])
        _require(isinstance(s3['region'],str) and re.fullmatch(r'[a-z0-9-]{1,64}',s3['region']),'S3 region is invalid.')
        _require(type(s3['path_style_access']) is bool,'S3 path_style_access must be a boolean.')
        _exact(document['kafka'],('brokers',))
        brokers=document['kafka']['brokers']
        _require(isinstance(brokers,str) and len(brokers)<=2048 and all(re.fullmatch(r'(?:[A-Za-z0-9.-]+|\[[A-Fa-f0-9:]+\]):[0-9]{1,5}', part) and 0<int(part.rsplit(':',1)[1])<=65535 for part in brokers.split(',')), 'Kafka brokers must be a comma-separated host:port list.')
        delivery=document['delivery'];_exact(delivery,('checkpoint_backend','deduplication','retries'))
        _require(delivery['checkpoint_backend']=='sqlite','The reference worker requires its durable SQLite checkpoint ledger.')
        _require(delivery['deduplication']=='bucket-key-version-etag','Only explicit bucket/key/version/ETag identity is supported.')
        _require(type(delivery['retries']) is int and 1<=delivery['retries']<=10,'Retries must be an integer from one through ten.')
        pipelines=document['pipelines'];_require(isinstance(pipelines,list) and len(pipelines)==3,'Exactly three image, text and video pipelines are required.')
        ids=set();media=set();sources=set();destinations=set();topics=set()
        for lane in pipelines:
            _exact(lane,('id','media_type','source','destination','stream','processing'))
            _require(isinstance(lane['id'],str) and _NAME.fullmatch(lane['id']) and lane['id'] not in ids,'Pipeline identifiers must be unique lowercase names.');ids.add(lane['id'])
            _require(lane['media_type'] in ('image','text','video') and lane['media_type'] not in media,'Provide one image, one text and one video pipeline.');media.add(lane['media_type'])
            for name,buckets in (('source',sources),('destination',destinations)):
                spec=lane[name];_exact(spec,('bucket','prefix'))
                bucket=spec['bucket']
                _require(isinstance(bucket,str) and _BUCKET.fullmatch(bucket) and '..' not in bucket and not re.fullmatch(r'\d+\.\d+\.\d+\.\d+',bucket) and bucket not in buckets,'All three source buckets and all three destination buckets must be distinct valid S3 bucket names.');buckets.add(bucket)
                prefix=spec['prefix']
                _require(isinstance(prefix,str) and len(prefix)<=512 and (not prefix or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9/_.-]*/?',prefix)) and '..' not in prefix,'Prefixes must be relative literal S3 key prefixes without traversal or expressions.')
            _exact(lane['stream'],('topic','dead_letter_topic'))
            for topic in lane['stream'].values():
                _require(isinstance(topic,str) and _TOPIC.fullmatch(topic) and topic not in topics,'All source and dead-letter topics must have unique valid Kafka names.');topics.add(topic)
            processing=lane['processing'];_exact(processing,('engine','url','method'))
            _require(processing['engine']=='http' and processing['method']=='POST','The processing contract is an HTTP POST with an object-reference JSON document.')
            _url(processing['url'])
        _require(not sources&destinations,'Source and destination bucket sets must be disjoint to prevent feedback and overwriting source objects.')
        report['ok']=True
        report['warnings']=[{'code':'at_least_once','message':'Cross-system transfer is at least once. Processing engines must enforce the supplied idempotency key; a crash after an external side effect can require reconciliation.'},{'code':'checkpoint_scope','message':'SQLite checkpoints belong to the reference worker; NiFi, Airflow and other platforms require their own durable runtime state. Checkpoints are not automatically interchangeable.'}]
        if s3['endpoint'].startswith('http:') or any(p['processing']['url'].startswith('http:') for p in pipelines):
            report['warnings'].append({'code':'plaintext_endpoint','message':'Plain HTTP is configured. Use it only for an explicitly trusted local test network; configure HTTPS before production use.'})
        return {'report':report,'blueprint':copy.deepcopy(document)}
    except (_Invalid,TypeError,ValueError,KeyError,AttributeError) as exc:
        report['errors'].append({'code':'invalid_media_blueprint','message':str(exc) if isinstance(exc,_Invalid) else 'The media blueprint has malformed fields.'})
        return {'report':report,'blueprint':None}


def _uid(name):return str(uuid.uuid5(uuid.NAMESPACE_URL,'https://flowbridge.davano.example/media/'+name))
def _json(value):return json.dumps(value,indent=2,sort_keys=True)+'\n'


def _native(blueprint):
    name=blueprint['name'];rootid=_uid(name);awsid=_uid(name+'/aws');kafkaid=_uid(name+'/kafka')
    services=[{'identifier':awsid,'groupIdentifier':rootid,'componentType':'CONTROLLER_SERVICE','name':'Configure AWS credentials through deployment identity','type':'org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService','bundle':{'group':'org.apache.nifi','artifact':'nifi-aws-nar','version':NIFI_VERSION},'properties':{'Use Default Credentials':'true'},'scheduledState':'DISABLED'},
              {'identifier':kafkaid,'groupIdentifier':rootid,'componentType':'CONTROLLER_SERVICE','name':'Configure Kafka TLS and authentication before enabling','type':'org.apache.nifi.kafka.service.Kafka3ConnectionService','bundle':{'group':'org.apache.nifi','artifact':'nifi-kafka-3-service-nar','version':NIFI_VERSION},'properties':{'bootstrap.servers':blueprint['kafka']['brokers'],'security.protocol':'SSL'},'scheduledState':'DISABLED'}]
    root={'identifier':rootid,'name':name,'componentType':'PROCESS_GROUP','position':{'x':0,'y':0},'comments':_MARKER+json.dumps(blueprint,sort_keys=True,separators=(',',':')),'processors':[],'connections':[],'processGroups':[],'controllerServices':services,'inputPorts':[],'outputPorts':[],'funnels':[],'remoteProcessGroups':[]}
    group_defaults={'labels':[], 'defaultFlowFileExpiration':'0 sec', 'defaultBackPressureObjectThreshold':1000, 'defaultBackPressureDataSizeThreshold':'1 GB', 'scheduledState':'DISABLED', 'executionEngine':'INHERITED', 'maxConcurrentTasks':1, 'statelessFlowTimeout':'1 min', 'flowFileConcurrency':'UNBOUNDED', 'flowFileOutboundPolicy':'STREAM_WHEN_AVAILABLE'}
    root.update(copy.deepcopy(group_defaults))
    for service in services:
        service.update({'propertyDescriptors':{}, 'controllerServiceApis':[], 'bulletinLevel':'WARN'})
    reference_fields=['source_bucket','source_key','source_version','source_etag','destination_bucket','destination_key','media_type','pipeline_id','event_id']
    for index,lane in enumerate(blueprint['pipelines']):
        gid=_uid(name+'/'+lane['id']);nodes=[];edges=[]
        group={'identifier':gid,'name':lane['id'],'componentType':'PROCESS_GROUP','groupIdentifier':rootid,'position':{'x':index*700,'y':0},'processors':nodes,'connections':edges,'processGroups':[],'controllerServices':[],'inputPorts':[],'outputPorts':[],'funnels':[],'remoteProcessGroups':[]}
        group.update(copy.deepcopy(group_defaults))
        def node(key,kind,properties,terminate=(),retry=()):
            if kind in ('ListS3','FetchS3Object','PutS3Object'):namespace='org.apache.nifi.processors.aws.s3.';artifact='nifi-aws-nar'
            elif kind in ('PublishKafka','ConsumeKafka'):namespace='org.apache.nifi.kafka.processors.';artifact='nifi-kafka-nar'
            elif kind=='UpdateAttribute':namespace='org.apache.nifi.processors.attributes.';artifact='nifi-update-attribute-nar'
            else:namespace='org.apache.nifi.processors.standard.';artifact='nifi-standard-nar'
            ident=_uid(name+'/'+lane['id']+'/'+key)
            entry={'identifier':ident,'groupIdentifier':gid,'componentType':'PROCESSOR','name':key,'type':namespace+kind,'bundle':{'group':'org.apache.nifi','artifact':artifact,'version':NIFI_VERSION},'position':{'x':(len(nodes)%3)*350,'y':(len(nodes)//3)*200},'properties':properties,'schedulingStrategy':'TIMER_DRIVEN','schedulingPeriod':'30 sec' if kind=='ListS3' else '0 sec','executionNode':'PRIMARY' if kind=='ListS3' else 'ALL','concurrentlySchedulableTaskCount':1,'penaltyDuration':'30 sec','yieldDuration':'1 sec','runDurationMillis':0,'scheduledState':'DISABLED','autoTerminatedRelationships':list(terminate),'retriedRelationships':list(retry),'retryCount':blueprint['delivery']['retries'],'backoffMechanism':'PENALIZE_FLOWFILE','maxBackoffPeriod':'10 mins'}
            entry.update({'style':{}, 'bulletinLevel':'WARN', 'propertyDescriptors':{}})
            for service_key in ('AWS Credentials Provider Service','Kafka Connection Service'):
                if service_key in properties:
                    entry['propertyDescriptors'][service_key]={'name':service_key,'displayName':service_key,'identifiesControllerService':True,'sensitive':False,'dynamic':False}
            nodes.append(entry);return ident
        def edge(left,right,relationships=('success',),destination_type='PROCESSOR'):
            ident=_uid(name+'/'+lane['id']+'/edge/'+left+'/'+right+'/'+','.join(relationships))
            edges.append({'identifier':ident,'groupIdentifier':gid,'componentType':'CONNECTION','name':','.join(relationships),'source':{'id':left,'type':'PROCESSOR','groupId':gid},'destination':{'id':right,'type':destination_type,'groupId':gid},'selectedRelationships':list(relationships),'backPressureObjectThreshold':1000,'backPressureDataSizeThreshold':'1 GB','flowFileExpiration':'0 sec','prioritizers':[], 'bends':[], 'labelIndex':0, 'zIndex':1, 'loadBalanceStrategy':'DO_NOT_LOAD_BALANCE', 'partitioningAttribute':'', 'loadBalanceCompression':'DO_NOT_COMPRESS'})
        def s3(bucket):return {'AWS Credentials Provider Service':awsid,'Bucket':bucket,'Region':'use-custom-region','Custom Region':blueprint['s3']['region'],'Endpoint Override URL':blueprint['s3']['endpoint'],'Use Path Style Access':str(blueprint['s3']['path_style_access']).lower()}
        listing=node('List versioned source objects','ListS3',dict(s3(lane['source']['bucket']),**({'Prefix':lane['source']['prefix']} if lane['source']['prefix'] else {}),**{'Use Versions':'true','Listing Strategy':'timestamps'}))
        attributes=node('Create immutable object reference','UpdateAttribute',{'source_bucket':lane['source']['bucket'],'source_key':'${filename}','source_version':'${s3.version}','source_etag':'${s3.etag}','destination_bucket':lane['destination']['bucket'],'destination_key':lane['destination']['prefix']+'${filename}','media_type':lane['media_type'],'pipeline_id':lane['id'],'event_id':"${s3.bucket:append('|'):append(${filename}):append('|'):append(${s3.version}):append('|'):append(${s3.etag}):hash('SHA-256')}"})
        serializer_properties={'Attributes List':','.join(reference_fields),'Destination':'flowfile-content','Include Core Attributes':'false','Null Value':'false'}
        serialize=node('Serialize object reference only','AttributesToJSON',serializer_properties)
        publish=node('Persist metadata to Kafka','PublishKafka',{'Kafka Connection Service':kafkaid,'Topic Name':lane['stream']['topic'],'acks':'all','Failure Strategy':'Route to Failure'},terminate=('success',),retry=('failure',))
        consume=node('Consume durable object references','ConsumeKafka',{'Kafka Connection Service':kafkaid,'Topics':lane['stream']['topic'],'Topic Format':'names','Group ID':name+'-'+lane['id'],'Processing Strategy':'FLOW_FILE','auto.offset.reset':'earliest'})
        restore=node('Restore object reference attributes','EvaluateJsonPath',dict({'Destination':'flowfile-attribute','Return Type':'scalar','Path Not Found Behavior':'warn'},**{key:'$.'+key for key in reference_fields}))
        fetch=node('Fetch exact source version','FetchS3Object',dict(s3('${source_bucket}'),**{'Object Key':'${source_key}','Version':'${source_version}'}),retry=('failure',))
        put=node('Write destination object','PutS3Object',dict(s3('${destination_bucket}'),**{'Object Key':'${destination_key}'}),retry=('failure',))
        engine_body=node('Create processing engine reference','AttributesToJSON',serializer_properties)
        invoke=node('Submit destination reference to engine','InvokeHTTP',{'HTTP Method':'POST','HTTP URL':lane['processing']['url'],'Request Body Enabled':'true','Request Content-Type':'application/json','Response Body Ignored':'true','Response Redirects Enabled':'False','Idempotency-Key':'${event_id}'},terminate=('Original','Response'),retry=('Retry','Failure'))
        dlq_body=node('Serialize failed object reference','AttributesToJSON',serializer_properties)
        dlq=node('Publish failed reference to dead letter topic','PublishKafka',{'Kafka Connection Service':kafkaid,'Topic Name':lane['stream']['dead_letter_topic'],'acks':'all','Failure Strategy':'Route to Failure'},terminate=('success',),retry=('failure',))
        quarantine=_uid(name+'/'+lane['id']+'/quarantine')
        group['outputPorts'].append({'identifier':quarantine,'groupIdentifier':gid,'componentType':'OUTPUT_PORT','name':'WIRE DURABLE FAILURE REVIEW BEFORE START','position':{'x':0,'y':1000},'scheduledState':'DISABLED','concurrentlySchedulableTaskCount':1})
        for left,right,relations in ((listing,attributes,('success',)),(attributes,serialize,('success',)),(serialize,publish,('success',)),(consume,restore,('success',)),(restore,fetch,('matched',)),(fetch,put,('success',)),(put,engine_body,('success',)),(engine_body,invoke,('success',)),(dlq_body,dlq,('success',))):edge(left,right,relations)
        for failed,relationships in ((serialize,('failure',)),(publish,('failure',)),(restore,('failure','unmatched')),(fetch,('failure',)),(put,('failure',)),(engine_body,('failure',)),(invoke,('Retry','No Retry','Failure'))):edge(failed,dlq_body,relationships)
        edge(dlq_body,quarantine,('failure',),'OUTPUT_PORT');edge(dlq,quarantine,('failure',),'OUTPUT_PORT')
        root['processGroups'].append(group)
    return {'flowEncodingVersion':'1.0','flowContents':root,'externalControllerServices':{},'parameterContexts':{},'parameterProviders':{}}


def export_nifi_media(document):
    result=validate_media(document);result['files']={}
    if not result['report']['ok']:return result
    result['report'].update({'full_contract_supported':False,'target':'nifi','target_version':NIFI_VERSION,'ready':False,'runtime_validated':False,'generator_import_tested':'NiFi 2.12.0 fixture accepted; not execution proof','artifact_kind':'stopped_integration_template'})
    result['report']['warnings'] += [
        {'code':'native_configuration_required','message':'NiFi services and all processors are disabled. Configure AWS deployment identity, Kafka TLS/authentication, required topics, persistent NiFi repositories/state and the failure-review output ports before starting.'},
        {'code':'dedup_semantics_require_review','message':'Native ListS3 timestamp/version listing plus engine Idempotency-Key does not reproduce the reference SQLite dedup ledger. Replay, late versions, engine deduplication and crash behavior require native integration validation.'},
        {'code':'native_unverified','message':'Generated against NiFi 2.12.0 component property documentation; a representative generated flow imported into NiFi 2.12.0 with all 36 processors disabled; native ETL execution and semantic equivalence remain unverified.'},
        {'code':'processing_contract','message':'The engine receives destination-object reference JSON, not image/video bytes. It must read the destination object and persist idempotent completion itself.'},
    ]
    native=_native(result['blueprint'])
    result['report']['ok']=False
    result['report']['errors'].append({'code':'native_media_semantics_incomplete','message':'Review-only native template: listing-version policy, deduplication identity encoding, destination-key layout and persistent checkpoint behavior do not yet match the reference worker. Full-contract deployment is blocked.'})
    result['files']={'nifi-media-flow.json':_json(native),'media-etl.json':_json(result['blueprint']),'media-migration-report.json':_json(result['report'])}
    result['files']['LICENSE']=(Path(__file__).resolve().parents[1]/'LICENSE').read_text()
    result['files']['README-NIFI-MEDIA.md']='''# NiFi media integration template

This file is a stopped NiFi 2.12.0 integration template, not a verified equivalent
of the reference media worker. The migration report intentionally marks the
full contract unsupported. The included media-etl.json preserves the requested
canonical intent; it is not a portable runtime checkpoint.

Each lane contains S3 listing, metadata-only Kafka publication/consumption,
version-aware object fetch, destination object write, HTTP engine notification,
bounded processor retries, a metadata dead-letter publisher and a final failure
output port. Media bytes stay out of Kafka. The engine receives an object
reference JSON document and must fetch the destination object itself.

Before any activation, configure AWS deployment identity and scoped permissions,
Kafka TLS/authentication, all six Kafka topics and six S3 buckets, persistent NiFi
FlowFile/content/provenance repositories and state providers. Wire each failure
output port to durable operator review. Enable controller services only after
validation. Processors remain disabled in this export; enabling them is a separate operator action.

Known semantic gaps block equivalence: ListS3 version listing versus latest-object
reference discovery; native timestamp state versus SQLite deduplication; native
identity encoding and destination-key layout versus the reference worker's
version-keyed destination layout. Engine idempotency must be demonstrated across
any selected replay/migration boundary. Do not start this template until these
gaps have been resolved and native success/failure/restart tests pass.

The strict media importer recognizes only the exact generated template and
recovers its intended blueprint. Editing a processor, service or connection
invalidates that recognition; use general graph analysis for modified flows.

Property references:
- https://nifi.apache.org/components/org.apache.nifi.processors.aws.s3.ListS3/
- https://nifi.apache.org/components/org.apache.nifi.processors.aws.s3.FetchS3Object/
- https://nifi.apache.org/components/org.apache.nifi.processors.aws.s3.PutS3Object/
- https://nifi.apache.org/components/org.apache.nifi.kafka.processors.PublishKafka/
- https://nifi.apache.org/components/org.apache.nifi.kafka.processors.ConsumeKafka/
- https://nifi.apache.org/components/org.apache.nifi.processors.standard.InvokeHTTP/
'''
    return result


def import_nifi_media(document):
    """Import only an unchanged generated native template, never trust a tag.

    Full structural regeneration comparison catches mutated processors, buckets,
    edges, retries and failure paths. Arbitrary NiFi graphs belong to the general
    graph analyzer rather than this exact media-contract importer.
    """
    error={'report':{'ok':False,'errors':[{'code':'unsupported_native_media','message':'This importer accepts only an unchanged generated NiFi media template; modified or arbitrary flows require graph analysis and explicit mappings.'}],'warnings':[]},'blueprint':None}
    if not isinstance(document,dict):return error
    comments=document.get('flowContents',{}).get('comments') if isinstance(document.get('flowContents'),dict) else None
    if not isinstance(comments,str) or len(comments)>100000 or not comments.startswith(_MARKER):return error
    try:blueprint=json.loads(comments[len(_MARKER):])
    except (ValueError,RecursionError):return error
    result=validate_media(blueprint)
    if not result['report']['ok']:return result
    if document!=_native(result['blueprint']):return error
    result['report']['warnings'].append({'code':'native_template_only','message':'Recovered the exact generated media blueprint. Native runtime readiness and checkpoint portability are not established.'})
    return result
