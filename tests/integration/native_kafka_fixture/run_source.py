"""Continuing Kafka arrivals and an observed native NiFi stop/drain barrier."""
import json,os,threading,time,hashlib
from pathlib import Path
from api import NiFi
from confluent_kafka import Producer,Consumer,TopicPartition
setup=json.loads(Path('/tmp/setup.json').read_text());n=NiFi(os.environ['CONTINUOUS_TEST_PASSWORD'])
producer=Producer({'bootstrap.servers':'kafka:9092','enable.idempotence':True});errors=[];stop=threading.Event();records=[]
def produce():
 try:
  index=0
  with Path('/tmp/produced.jsonl').open('w') as log:
   while not stop.is_set() and index<600:
    value=f'record-{index:04d}'.encode();delivery=[]
    producer.produce('source_topic',value,on_delivery=lambda error,message:delivery.append(error));assert producer.flush(5)==0
    assert delivery==[None],delivery
    item={'value':value.decode(),'sha256':hashlib.sha256(value).hexdigest()};records.append(item);log.write(json.dumps(item)+'\n');log.flush();index+=1;stop.wait(.3)
 except Exception as error:errors.append(str(error))
thread=threading.Thread(target=produce,daemon=True);thread.start()
reader=Consumer({'bootstrap.servers':'kafka:9092','group.id':'native-output-observer','auto.offset.reset':'earliest','enable.auto.commit':False});reader.assign([TopicPartition('target_topic',0,0)])
try:
 n.state(setup['publish'],'RUNNING');n.state(setup['consume'],'RUNNING');native=[];deadline=time.monotonic()+50
 while len(native)<6:
  if time.monotonic()>deadline:raise RuntimeError('native output timeout')
  message=reader.poll(1)
  if message and not message.error():native.append(message.value().decode())
 n.state(setup['consume'],'STOPPED');deadline=time.monotonic()+30
 while True:
  snapshot=n.call('/flow/process-groups/'+setup['parent']+'/status?recursive=true')['processGroupStatus']['aggregateSnapshot']
  queued=int(str(snapshot.get('flowFilesQueued',snapshot.get('queuedCount',0))).replace(',',''));active=snapshot.get('activeThreadCount',0)
  if queued==0 and active==0:break
  if time.monotonic()>deadline:raise RuntimeError('native drain timeout')
  time.sleep(.3)
 n.state(setup['publish'],'STOPPED')
 group=Consumer({'bootstrap.servers':'kafka:9092','group.id':'nifi-proof-blue','enable.auto.commit':False})
 offset=group.committed([TopicPartition('source_topic',0)],timeout=10)[0].offset;group.close();assert offset>=6,offset
 # Every committed source message must already exist in the drained blue output.
 deadline=time.monotonic()+10
 while len(native)<offset and time.monotonic()<deadline:
  message=reader.poll(.5)
  if message and not message.error():native.append(message.value().decode())
 assert native==[f'record-{i:04d}' for i in range(offset)],(native,offset)
 document=n.call('/process-groups/'+setup['parent']+'/download')
 Path('/tmp/native-export.json').write_text(json.dumps(document,indent=2))
 evidence={'source':'Apache NiFi 2.12.0','group':'nifi-proof-blue','source_topic':'source_topic','target_topic':'target_topic','source_committed_offsets':{'0':offset},'native_output':native,'native_count':len(native),'queued_flowfiles':queued,'active_threads':active,'source_processors_stopped':True,'handoff_timestamp':time.time(),'flow':{'name':'native-kafka-cutover','source':{'type':'kafka','brokers':'kafka:9092','topic':'source_topic','group':'nifi-proof-blue','offset':'earliest'},'sink':{'type':'kafka','brokers':'kafka:9092','topic':'target_topic'}}}
 Path('/tmp/handoff.json').write_text(json.dumps(evidence,indent=2));print(json.dumps(evidence),flush=True)
 deadline=time.monotonic()+160
 while not Path('/tmp/stop-producer').exists() and time.monotonic()<deadline and not errors:time.sleep(.2)
finally:
 stop.set();thread.join(timeout=6);reader.close()
 Path('/tmp/producer-complete.json').write_text(json.dumps({'produced':len(records),'errors':errors,'stopped_at':time.time()}))
