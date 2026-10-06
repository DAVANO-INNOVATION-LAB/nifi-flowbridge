"""Real S3Mock I/O; blue is a synthetic worker, NOT a native NiFi runtime."""
import datetime
import hashlib
import json
from pathlib import Path
import signal
import tempfile
import threading
import time
import boto3
from botocore.config import Config
from flowbridge.bluegreen import BlueGreenPlan
from flowbridge.fleet import discover_fleet
from test_fleet import fleet

signal.signal(signal.SIGALRM,lambda *_: (_ for _ in ()).throw(TimeoutError('fleet proof bound')))
signal.alarm(240)
def factory(c):return boto3.client('s3',endpoint_url=c['endpoint'],region_name=c['region'],aws_access_key_id='synthetic-only',aws_secret_access_key='synthetic-only',config=Config(s3={'addressing_style':'path'},connect_timeout=2,read_timeout=5,retries={'max_attempts':1}))
profile=discover_fleet(fleet(60))['profile']
source=factory(profile['lanes'][0]['source']);target=factory(profile['lanes'][0]['destination'])
for client in (source,target):
 deadline=time.monotonic()+60
 while True:
  try:client.list_buckets();break
  except Exception:
   if time.monotonic()>deadline:raise
   time.sleep(1)
mapping={lane['id']:{**lane['destination'],'bucket':'shadow-'+str(i)} for i,lane in enumerate(profile['lanes'])}
for lane in profile['lanes']:
 source.create_bucket(Bucket=lane['source']['bucket']);target.create_bucket(Bucket=lane['destination']['bucket']);target.create_bucket(Bucket=mapping[lane['id']]['bucket'])
expected={};guard=threading.Lock();stop=threading.Event();blue_stop=threading.Event();errors=[];copied=set();timeline=[]
def mark(phase,**extra):
 row={'phase':phase,'timestamp':datetime.datetime.now(datetime.timezone.utc).isoformat(),'produced':len(expected),**extra};timeline.append(row)
def produce_one(index,key):
 lane=profile['lanes'][index];body=('real-fleet-object-'+str(index)+'-'+key).encode()
 source.put_object(Bucket=lane['source']['bucket'],Key=key,Body=body,ContentType='application/octet-stream')
 with guard:expected[(index,key)]=body
for i in range(60):produce_one(i,'initial')
def producer():
 try:
  index=0
  while not stop.wait(.4):produce_one(index%60,'arrival-'+str(index));index+=1
 except Exception as exc:errors.append(type(exc).__name__)
def blue():
 try:
  while not blue_stop.is_set():
   with guard:items=list(expected.items())
   for (index,key),body in items:
    if (index,key) in copied:continue
    lane=profile['lanes'][index]
    target.put_object(Bucket=lane['destination']['bucket'],Key=key,Body=body,ContentType='application/octet-stream',Metadata={'media_type':lane['media_type']});copied.add((index,key))
   blue_stop.wait(.05)
 except Exception as exc:errors.append(type(exc).__name__)
blue_thread=threading.Thread(target=blue);producer_thread=threading.Thread(target=producer)
blue_thread.start();producer_thread.start();mark('blue_running',runtime='synthetic Python worker; not NiFi')
class FixtureFence:
 def fence(self,p):
  blue_stop.set();blue_thread.join(10)
  if blue_thread.is_alive() or errors:raise RuntimeError('blue did not halt')
  return {'pipeline_ids':[l['id'] for l in p['lanes']],'queued_flowfiles':0,'active_threads':0,'source_stopped':True,'fence_id':'joined-synthetic-blue-thread'}
 def verify(self,p,proof):return blue_stop.is_set() and not blue_thread.is_alive() and not errors
try:
 with tempfile.TemporaryDirectory() as temporary:
  state=Path(temporary)/'plan.db';plan=BlueGreenPlan(profile,state,factory)
  for attempt in range(12):
   plan.shadow_once(mapping);result=plan.validate_shadow()
   if result['phase']=='ready':break
  assert result['phase']=='ready',result.get('failure')
  mark('shadow_validated',pipelines=60,objects=result['validation']['object_count'])
  lane=profile['lanes'][0];bucket=mapping[lane['id']]['bucket']
  target.put_object(Bucket=bucket,Key='initial',Body=b'corruption')
  bad=plan.validate_shadow();assert bad['phase']=='blocked'
  mark('one_pipeline_mismatch_blocks_fleet',failures=len(bad['validation']['failures']))
  target.put_object(Bucket=bucket,Key='initial',Body=expected[(0,'initial')],ContentType='application/octet-stream',Metadata={'media_type':lane['media_type']})
  for attempt in range(12):
   plan.shadow_once(mapping);result=plan.validate_shadow()
   if result['phase']=='ready':break
  assert result['phase']=='ready'
  assert plan.request_cutover()['phase']=='blocked';assert not blue_stop.is_set()
  mark('unconfigured_fence_blocked',production_changed=False)
  plan=BlueGreenPlan(profile,state,factory,FixtureFence())
  result=plan.request_cutover();assert result['phase']=='active'
  mark('trusted_cutover',blue_joined=True,baseline=result['baseline'])
  for _ in range(3):time.sleep(.8);assert plan.run_production_once()['phase']=='active'
  stop.set();producer_thread.join(10);assert not errors
  plan.run_production_once();mark('later_arrivals_processed')
  checked=[]
  for (index,key),body in expected.items():
   lane=profile['lanes'][index];obj=target.get_object(Bucket=lane['destination']['bucket'],Key=key);actual=obj['Body'].read();obj['Body'].close()
   assert actual==body and obj['Metadata']['media_type']==lane['media_type']
   checked.append({'pipeline_id':lane['id'],'key':key,'sha256':hashlib.sha256(actual).hexdigest()})
  total=sum(target.list_objects_v2(Bucket=l['destination']['bucket']).get('KeyCount',0) for l in profile['lanes'])
  assert total==len(expected)
  restarted=BlueGreenPlan(profile,state,factory,FixtureFence());repeat=restarted.run_production_once()['production_result']['counts']['processed'];assert repeat==0
  mark('reconciled',objects=total,missing=0,restart_repeat=repeat)
  print(json.dumps({'result':'passed','recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'pipelines':60,'native_nifi_runtime_executed':False,'blue_runtime':'synthetic Python producer/blue worker','s3_runtime':'Adobe S3Mock 5.1.0','network':'internal Docker network, no host ports','memory_limits_mib':1408,'total_produced':len(expected),'total_production_objects':total,'missing':0,'all_hashes_and_media_metadata_matched':True,'single_pipeline_mismatch_blocked':True,'unconfigured_cutover_blocked':True,'trusted_blue_worker_joined':True,'restart_repeat_processed':repeat,'timeline':timeline,'objects':checked,'limitations':['Synthetic blue worker, not a 60-pipeline native NiFi run.','Fixture fencing joins a local thread; no distributed fencing or production route switch.','Byte-preserving transfer with literal metadata, not arbitrary ETL.','At-least-once delivery; no zero downtime claim.']}))
finally:
 stop.set();blue_stop.set();producer_thread.join(10);blue_thread.join(10)
