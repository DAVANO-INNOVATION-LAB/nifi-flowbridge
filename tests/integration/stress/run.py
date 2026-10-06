"""Isolated S3Mock workload. Never point at production endpoints."""
import concurrent.futures, hashlib, json, resource, tempfile, time
from pathlib import Path
import boto3
from botocore.config import Config
from flowbridge.nifi_s3 import analyze_nifi_s3,ContinuousS3Runner

def factory(c):
 return boto3.client('s3',endpoint_url=c['endpoint'],region_name=c['region'],aws_access_key_id='synthetic',aws_secret_access_key='synthetic',config=Config(s3={'addressing_style':'path'},max_pool_connections=12,retries={'max_attempts':1},connect_timeout=2,read_timeout=10))
profile=analyze_nifi_s3(json.load(open('/stress/native.json')))['profile']
source=factory(profile['lanes'][0]['source']);target=factory(profile['lanes'][0]['destination'])
for client in (source,target):
 for attempt in range(60):
  try:client.list_buckets();break
  except Exception:time.sleep(1)
 else:raise RuntimeError('fixture startup timeout')
for lane in profile['lanes']:
 source.create_bucket(Bucket=lane['source']['bucket']);target.create_bucket(Bucket=lane['destination']['bucket'])
expected={}
for lane in profile['lanes']:
 for i in range(1100):
  key=lane['source']['prefix']+f'{i:05d}.bin';body=(f'{lane["media_type"]}:{i}:'.encode()*400)[:2048]
  expected[(lane['source']['bucket'],key)]=(lane,body)
 key=lane['source']['prefix']+'large.bin';expected[(lane['source']['bucket'],key)]=(lane,b'large-media-'*(1024*1024))
def upload(item):
 (bucket,key),(lane,body)=item;source.put_object(Bucket=bucket,Key=key,Body=body,ContentType='application/octet-stream')
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:list(pool.map(upload,expected.items()))
with tempfile.TemporaryDirectory() as state:
 runner=ContinuousS3Runner(profile,Path(state)/'ledger.db',factory)
 start=time.monotonic();baseline=runner.establish_cutover(True,True);baseline_s=time.monotonic()-start
 start=time.monotonic();result=runner.run_once();elapsed=time.monotonic()-start
 assert result['counts']=={'processed':len(expected),'failures':0},result['counts']
 def verify(item):
  (bucket,key),(lane,body)=item;r=target.get_object(Bucket=lane['destination']['bucket'],Key=key)
  content=r['Body'].read();r['Body'].close();assert hashlib.sha256(content).digest()==hashlib.sha256(body).digest();assert r['Metadata']['media_type']==lane['media_type']
 with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:list(pool.map(verify,expected.items()))
 repeat=ContinuousS3Runner(profile,Path(state)/'ledger.db',factory).run_once();assert repeat['counts']=={'processed':0,'failures':0}
 # A real TCP refusal must remain visible; restoring endpoint access must recover.
 lane=profile['lanes'][0];key=lane['source']['prefix']+'outage.bin';source.put_object(Bucket=lane['source']['bucket'],Key=key,Body=b'after outage')
 old=runner.clients[(lane['id'],'destination')];runner.clients[(lane['id'],'destination')]=factory({**lane['destination'],'endpoint':'http://127.0.0.1:1'})
 failed=runner.run_once();assert failed['counts']['failures']==1
 runner.clients[(lane['id'],'destination')]=old;recovery=runner.run_once();assert recovery['counts']=={'processed':1,'failures':0}
 print(json.dumps({'result':'passed','objects':len(expected),'bytes':sum(len(v[1]) for v in expected.values()),'lanes':3,'objects_per_lane':1101,'baseline_seconds':round(baseline_s,3),'transfer_seconds':round(elapsed,3),'objects_per_second':round(len(expected)/elapsed,2),'all_hashes_and_metadata_matched':True,'restart_reprocessed':repeat['counts']['processed'],'connection_refusal_failures':failed['counts']['failures'],'recovered_objects':recovery['counts']['processed'],'max_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,'scope':'Synthetic S3Mock, single polling worker; not production throughput certification'}),flush=True)
