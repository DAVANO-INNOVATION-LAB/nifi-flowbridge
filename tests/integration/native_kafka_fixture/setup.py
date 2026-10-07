import json,os,time
from api import NiFi
from confluent_kafka.admin import AdminClient,NewTopic
n=NiFi(os.environ['CONTINUOUS_TEST_PASSWORD'])
admin=AdminClient({'bootstrap.servers':'kafka:9092'})
for future in admin.create_topics([NewTopic('source_topic',1,1),NewTopic('target_topic',1,1)]).values():future.result(timeout=30)
root=n.call('/flow/process-groups/root')['processGroupFlow']['id']
parent=n.call('/process-groups/'+root+'/process-groups',{'revision':{'version':0},'component':{'name':'native-kafka-cutover','position':{'x':0,'y':0}}})['id']
svc=n.call('/process-groups/'+parent+'/controller-services',{'revision':{'version':0},'component':{'name':'Isolated Kafka','type':'org.apache.nifi.kafka.service.Kafka3ConnectionService','bundle':{'group':'org.apache.nifi','artifact':'nifi-kafka-3-service-nar','version':'2.12.0'}}})
sid=svc['id'];n.call('/controller-services/'+sid,{'revision':svc['revision'],'component':{'id':sid,'properties':{'bootstrap.servers':'kafka:9092','security.protocol':'PLAINTEXT'}}},'PUT')
svc=n.call('/controller-services/'+sid);n.call('/controller-services/'+sid+'/run-status',{'revision':svc['revision'],'state':'ENABLED','disconnectedNodeAcknowledged':False},'PUT')
consume=n.create(parent,'org.apache.nifi.kafka.processors.ConsumeKafka','Kafka source','nifi-kafka-nar')
publish=n.create(parent,'org.apache.nifi.kafka.processors.PublishKafka','Kafka destination','nifi-kafka-nar')
n.configure(consume,{'Kafka Connection Service':sid,'Topics':'source_topic','Topic Format':'names','Group ID':'nifi-proof-blue','Processing Strategy':'FLOW_FILE','auto.offset.reset':'earliest'},(), '0 sec')
n.configure(publish,{'Kafka Connection Service':sid,'Topic Name':'target_topic','acks':'all','Failure Strategy':'Route to Failure','Transactions Enabled':'false'},('success',),'0 sec')
for source,target,relationship in [(consume,publish,'success'),(publish,publish,'failure')]:n.call('/process-groups/'+parent+'/connections',{'revision':{'version':0},'component':{'source':{'id':source,'groupId':parent,'type':'PROCESSOR'},'destination':{'id':target,'groupId':parent,'type':'PROCESSOR'},'selectedRelationships':[relationship]}})
time.sleep(2)
document=n.call('/process-groups/'+parent+'/download')
print(json.dumps({'parent':parent,'consume':consume,'publish':publish,'service':sid,'document':document,'processors':[n.call('/processors/'+pid)['component'] for pid in (consume,publish)]}))
