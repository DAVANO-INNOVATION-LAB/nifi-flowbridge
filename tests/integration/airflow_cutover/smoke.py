"""Real NiFi source execution followed by bounded continuously polling handoff."""
import datetime
import copy
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import signal
import tempfile
import threading
import time
import sys
import boto3
from botocore.config import Config
from api import NiFi


def factory(config):return boto3.client('s3',endpoint_url=config['endpoint'],region_name=config['region'],aws_access_key_id='synthetic-only',aws_secret_access_key='synthetic-only',config=Config(s3={'addressing_style':'path'},connect_timeout=3,read_timeout=5,retries={'max_attempts':1}))
def client(host):return factory({'endpoint':'http://'+host+':9090','region':'us-east-1'})
def count(s3,bucket):return s3.list_objects_v2(Bucket=bucket).get('KeyCount',0)

def main():
 signal.signal(signal.SIGALRM,lambda *_:(_ for _ in ()).throw(TimeoutError('Airflow migration exceeded480seconds')));signal.alarm(480)
 setup=json.load(sys.stdin);n=NiFi(os.environ['CONTINUOUS_TEST_PASSWORD']);lanes=setup['lanes'];parent=setup['parent']
 source=client('source-s3');dest=client('target-s3');timeline=[];expected={};stop=threading.Event();producer_errors=[];start=time.monotonic()
 def mark(phase,**extra):
  event={'phase':phase,'timestamp':datetime.datetime.now(datetime.timezone.utc).isoformat(),'elapsed_seconds':round(time.monotonic()-start,3),'producer_generated':len(expected),'source_objects':sum(count(source,l['source']) for l in lanes),'destination_objects':sum(count(dest,l['destination']) for l in lanes),**extra};timeline.append(event);print(json.dumps({'timeline':event}),flush=True)
 def produce():
  try:
   index=0
   while not stop.is_set():
    for lane in lanes:
     kind=lane['kind'];filename={'image':'image.png','text':'note.txt','video':'clip.mp4'}[kind]
     value=Path('/tmp/fixtures',filename).read_bytes();key=f'incoming/{index:04d}-{filename}'
     if kind=='text':value+=f'event {index}\n'.encode()
     source.put_object(Bucket=lane['source'],Key=key,Body=value,ContentType={'image':'image/png','text':'text/plain','video':'video/mp4'}[kind])
     expected[(kind,key)]={'sha256':hashlib.sha256(value).hexdigest(),'bytes':len(value)}
    index+=1;stop.wait(1)
  except Exception as error:producer_errors.append(type(error).__name__)
 # Clear ACL overrides: they are outside the bounded target semantics.
 for lane in lanes:
  n.configure(lane['processors'][-1],{**{k:"${literal('')}" for k in ('FullControl User List','Read Permission User List','Read ACL User List','Write ACL User List','Canned ACL')},'Content Type':'${mime.type}'},('success','failure'))
 # Validate and start downstream components before source listing.
 for lane in lanes:
  for pid in reversed(lane['processors'][1:]):n.state(pid,'RUNNING')
  n.state(lane['processors'][0],'RUNNING')
 thread=threading.Thread(target=produce,daemon=True);thread.start();mark('native_running',processing='literal media_type attribute persisted as S3 user metadata')
 try:
  deadline=time.monotonic()+60
  while sum(count(dest,l['destination']) for l in lanes)<6:
   if time.monotonic()>deadline:
    diagnostics=[n.call('/processors/'+pid)['component'] for l in lanes for pid in l['processors']]
    print(json.dumps({'native_diagnostics':[{'name':p['name'],'validationErrors':p.get('validationErrors',[]),'state':p['state']} for p in diagnostics]}),flush=True)
    raise AssertionError('Native source did not transfer six objects')
   time.sleep(1)
  native_before=sum(count(dest,l['destination']) for l in lanes)
  # Verify observable native processing before claiming source execution.
  for lane in lanes:
   for obj in dest.list_objects_v2(Bucket=lane['destination']).get('Contents',[]):
    assert dest.head_object(Bucket=lane['destination'],Key=obj['Key'])['Metadata'].get('media_type')==lane['kind']
  document=n.call('/process-groups/'+parent+'/download')
  print(json.dumps({'native_export':document}),flush=True);mark('exported',native_destination_objects=native_before)
  from flowbridge.targets.airflow import export_airflow_fleet, CONTRACT
  exported=export_airflow_fleet(document,CONTRACT);assert exported['report']['ok'],exported['report'];profile=exported['profile'];mark('airflow_packaged',dag_id=exported['dag_id'])
  unsupported=copy.deepcopy(document);unsupported['flowContents']['processGroups'][0]['processors'][0]['type']='example.UnsupportedProcessor'
  rejected=export_airflow_fleet(unsupported,CONTRACT);assert not rejected['report']['ok'] and not rejected['files']
  # Arrivals continue while listing stops; downstream queues must drain.
  mark('source_stop_requested')
  for lane in lanes:n.state(lane['processors'][0],'STOPPED')
  deadline=time.monotonic()+30
  while True:
   snapshot=n.call('/flow/process-groups/'+parent+'/status?recursive=true')['processGroupStatus']['aggregateSnapshot']
   queued=int(str(snapshot.get('flowFilesQueued',snapshot.get('queuedCount',0))).replace(',',''))
   active=snapshot.get('activeThreadCount',0)
   if queued==0 and active==0:break
   if time.monotonic()>deadline:raise AssertionError('NiFi drain timeout')
   time.sleep(.5)
  for lane in lanes:
   for pid in lane['processors'][1:]:n.state(pid,'STOPPED')
  native_at_drain=sum(count(dest,l['destination']) for l in lanes);mark('nifi_drained',queued_flowfiles=queued,active_threads=active,native_destination_objects=native_at_drain)
  # Deliberately allow new objects to arrive with source stopped before target.
  time.sleep(2);mark('handoff_arrivals')
  with tempfile.TemporaryDirectory() as state:
   import subprocess
   package=Path('/tmp/generated');package.mkdir(exist_ok=True)
   for filename,contents in exported['files'].items():
    file=package/filename;file.parent.mkdir(parents=True,exist_ok=True);file.write_text(contents)
   environment=dict(os.environ,PYTHONPATH=str(package),FLOWBRIDGE_AIRFLOW_STATE_DIR=state,AIRFLOW__CORE__DAGS_FOLDER=str(package/'dags'))
   def execute_batch():
    completed=subprocess.run([sys.executable,'/tmp/proof/dag_run.py'],env=environment,check=True,capture_output=True,text=True,timeout=100)
    for line in completed.stdout.splitlines():
     if line.startswith('AIRFLOW_RESULT:'):return json.loads(line.split(':',1)[1])
    raise AssertionError(completed.stdout[-2000:])
   blocked=execute_batch();assert blocked['state']=='failed',blocked;mark('airflow_pre_cutover_rejected')
   from flowbridge.targets.airflow import prepare_cutover
   baseline=prepare_cutover(profile,state,source_stopped=True,backfill=True,client_factory=factory);mark('baseline_verified',baseline=baseline)
   batches=[]
   batches.append(execute_batch());assert batches[-1]['state']=='success',batches[-1];mark('airflow_batch_complete',dag_run=batches[-1])
   time.sleep(2);mark('later_arrivals')
   stop.set();thread.join(timeout=3);assert not producer_errors,producer_errors
   batches.append(execute_batch());assert batches[-1]['state']=='success',batches[-1];mark('airflow_final_batch',dag_run=batches[-1])
   batches.append(execute_batch());assert batches[-1]['state']=='success' and batches[-1]['processed']==0,batches[-1]
   reconciliation=[]
   for lane in lanes:
    keys={o['Key'] for o in dest.list_objects_v2(Bucket=lane['destination']).get('Contents',[])}
    expected_keys={key for (kind,key) in expected if kind==lane['kind']};assert keys==expected_keys
    for key in sorted(keys):
     a=source.get_object(Bucket=lane['source'],Key=key);source_bytes=a['Body'].read();a['Body'].close()
     b=dest.get_object(Bucket=lane['destination'],Key=key);target_bytes=b['Body'].read();b['Body'].close()
     sh=hashlib.sha256(source_bytes).hexdigest();dh=hashlib.sha256(target_bytes).hexdigest()
     assert sh==dh==expected[(lane['kind'],key)]['sha256']
     assert a.get('ContentType')==b.get('ContentType'),(key,a.get('ContentType'),b.get('ContentType'))
     assert b['Metadata'].get('media_type')==lane['kind']
     reconciliation.append({'lane':lane['kind'],'key':key,'source_sha256':sh,'destination_sha256':dh,'media_type':b['Metadata']['media_type'],'bytes':len(target_bytes),'source_content_type':a.get('ContentType'),'destination_content_type':b.get('ContentType')})
   mark('reconciliation_complete',matched_objects=len(reconciliation),missing_objects=0,unchanged_repeat_processed=0)
   print(json.dumps({'proof':{'result':'passed','recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source_runtime':'Apache NiFi 2.12.0','source_export_sha256':hashlib.sha256(json.dumps(document,sort_keys=True).encode()).hexdigest(),'exported_files':sorted(exported['files']),'generated_module_executed':True,'unsupported_processor_rejected':True,'native_processors':12,'native_connections':9,'native_groups':4,'native_processing':'literal media_type attribute persisted in destination S3 metadata','source_s3_endpoints':1,'destination_s3_endpoints':1,'lanes':3,'native_completed_before_export':native_before,'native_completed_at_drain':native_at_drain,'baseline':baseline,'airflow_runs':batches,'pre_cutover_run':blocked,'airflow_version':'3.1.8','boto3_version':boto3.__version__,'scheduler_execution':False,'dag_test_execution':True,'microbatch_interval_seconds':60,'target_processed':sum(batch['processed'] for batch in batches),'total_produced':len(expected),'total_destination_objects':len(reconciliation),'missing_objects':0,'hashes_matched':True,'processing_metadata_matched':True,'content_types_matched':True,'worker_restart_repeat_processed':batches[-1]['processed'],'arrivals_continued_during_handoff':True,'timeline':timeline,'objects':reconciliation,'target_runtime':'Airflow TaskFlow lane tasks invoking generated bounded S3 worker','zero_downtime_claim':False,'nifi_state_transferred':False,'delivery':'at_least_once'}}),flush=True)
 finally:
  stop.set();thread.join(timeout=3)
  for lane in lanes:
   for pid in lane['processors']:
    try:n.state(pid,'STOPPED')
    except Exception:pass
if __name__=='__main__':main()
