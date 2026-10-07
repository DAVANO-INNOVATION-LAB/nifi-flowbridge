"""Run and fence the actual NiFi source before native Camel K takes ownership."""
import hashlib,json,os,sys,time
from pathlib import Path
sys.path.insert(0,'/work/tests/integration/continuous')
from setup import setup,client
from api import NiFi
sys.path.insert(0,'/work')
out=Path('/tmp/output');out.mkdir(exist_ok=True); configuration=setup();n=NiFi(os.environ['CONTINUOUS_TEST_PASSWORD']);source=client('source-s3');target=client('target-s3')
for lane in configuration['lanes']:
 n.configure(lane['processors'][-1],{**{k:"${literal('')}" for k in ('FullControl User List','Read Permission User List','Read ACL User List','Write ACL User List','Canned ACL')},'Content Type':'${mime.type}'},('success','failure'))
 target.put_bucket_versioning(Bucket=lane['destination'],VersioningConfiguration={'Status':'Enabled'})
 for index in range(2):
  value=(lane['kind']+' native initial '+str(index)).encode();source.put_object(Bucket=lane['source'],Key='incoming/before-'+str(index),Body=value,ContentType='application/octet-stream')
 for pid in reversed(lane['processors']):n.state(pid,'RUNNING')
def counts():return sum(target.list_objects_v2(Bucket=l['destination']).get('KeyCount',0) for l in configuration['lanes'])
end=time.monotonic()+90
while counts()!=6:
 if time.monotonic()>end:raise RuntimeError('native_source_timeout')
 time.sleep(1)
for lane in configuration['lanes']:
 for obj in target.list_objects_v2(Bucket=lane['destination']).get('Contents',[]):
  a=source.get_object(Bucket=lane['source'],Key=obj['Key']);b=target.get_object(Bucket=lane['destination'],Key=obj['Key'])
  assert a['Body'].read()==b['Body'].read();assert b['Metadata']['media_type']==lane['kind'];a['Body'].close();b['Body'].close()
configuration['document']=n.call('/process-groups/'+configuration['parent']+'/download')
from flowbridge.live.nifi_fence import NiFiFence
from urllib.parse import urlencode
class Shim:
 _base='https://localhost:8443/nifi-api'
 _timeout=30
 def request(self,method,path,document=None,query=None):
  if query:path+='?'+urlencode(query)
  return n.call(path,document,method)
fencer=NiFiFence(Shim(),out/'proofs')
proof=fencer.stop_and_drain(configuration['parent']);verified=fencer.revalidate(proof['id'])
(out/'shared-fence.json').write_text(json.dumps({'proof':proof,'revalidated':verified}))
queued=proof['queued_flowfiles'];active=proof['active_threads']
states=[n.call('/processors/'+pid)['component']['state'] for lane in configuration['lanes'] for pid in lane['processors']]
assert all(state=='STOPPED' for state in states)
(out/'native.json').write_text(json.dumps(configuration['document'],indent=2));(out/'setup.json').write_text(json.dumps(configuration))
(out/'fence.json').write_text(json.dumps({'native_transferred':6,'queued_flowfiles':queued,'active_threads':active,'processor_states':states,'verified_at':time.time()}))
print(json.dumps({'native_transferred':6,'source_stopped':True,'queues_drained':True}))
