import copy
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from flowbridge.media_runtime import MediaRunner, MediaError, inspect_media

class S3:
 def __init__(self):self.objects={};self.puts=0
 def put_object(self,Bucket,Key,Body,**kw):
  self.puts+=1;self.objects[(Bucket,Key)]=(bytes(Body),kw);return {}
 def list_objects_v2(self,Bucket,Prefix='',**kw):return {'Contents':[{'Key':key} for bucket,key in self.objects if bucket==Bucket and key.startswith(Prefix)]}
 def head_object(self,Bucket,Key):
  value=self.objects[(Bucket,Key)][0];return {'ETag':'"'+hashlib.md5(value).hexdigest()+'"'}
 def get_object(self,Bucket,Key,IfMatch=None,**kw):
  if IfMatch and IfMatch!=self.head_object(Bucket,Key)['ETag']:raise ValueError('changed')
  value=self.objects[(Bucket,Key)][0];return {'Body':io.BytesIO(value),'ContentLength':len(value)}

class MediaRuntimeTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
  self.blueprint=json.loads((Path(__file__).parent.parent/'examples/media-etl.json').read_text());self.s3=S3();self.now=[1000];self.events=[]
  self.lane=next(p for p in self.blueprint['pipelines'] if p['media_type']=='text')
  self.s3.put_object(Bucket=self.lane['source']['bucket'],Key=self.lane['source']['prefix']+'hello.txt',Body=b'hello durable world')
 def runner(self,processor=None,publisher=None):
  return MediaRunner(self.blueprint,Path(self.temp.name)/'jobs.db',self.s3,processor=processor,publisher=publisher or (lambda topic,event:self.events.append((topic,event))),clock=lambda:self.now[0])
 def test_success_restart_dedup_and_processing_result(self):
  runner=self.runner();self.assertEqual(runner.discover()['discovered'],1)
  self.assertEqual(runner.run_once()['counts']['complete'],1)
  puts=self.s3.puts;second=self.runner();self.assertEqual(second.discover()['discovered'],0)
  self.assertEqual(second.run_once()['counts']['complete'],1);self.assertEqual(self.s3.puts,puts);self.assertEqual(len(self.events),1)
  outputs=[json.loads(v[0]) for (bucket,key),v in self.s3.objects.items() if key.endswith('.flowbridge-result.json')]
  self.assertEqual(outputs[0]['words'],3)
 def test_processing_failure_retries_without_recopied_payload(self):
  calls=[]
  def flaky(job,content):
   calls.append(1)
   if len(calls)==1:raise MediaError('synthetic_processing_failure')
   return inspect_media(job,content)
  runner=self.runner(flaky);runner.discover();first=runner.run_once();self.assertEqual(first['counts']['retry'],1);self.assertEqual(first['jobs'][0]['copied'],1)
  puts=self.s3.puts;self.now[0]+=64
  self.assertEqual(runner.run_once()['counts']['complete'],1);self.assertEqual(self.s3.puts,puts+1);self.assertEqual(len(self.events),1)
 def test_poison_message_dead_letter_and_no_false_success(self):
  def bad(job,content):raise MediaError('synthetic_poison')
  runner=self.runner(bad);runner.discover()
  for _ in range(self.blueprint['delivery']['retries']+1):runner.run_once();self.now[0]+=64
  status=runner.status();self.assertEqual(status['counts']['dead_letter'],1);self.assertEqual(status['counts']['complete'],0)
  self.assertEqual(self.events[-1][0],self.lane['stream']['dead_letter_topic'])
 def test_kafka_failure_blocks_copy_and_is_retryable(self):
  def bad(topic,event):raise RuntimeError('secret not persisted')
  runner=self.runner(publisher=bad);runner.discover();status=runner.run_once()
  self.assertEqual(status['counts']['retry'],1);self.assertEqual(status['jobs'][0]['copied'],0)
  self.assertNotIn('secret not persisted',json.dumps(status))
 def test_changed_source_does_not_copy_different_bytes(self):
  runner=self.runner();runner.discover()
  self.s3.put_object(Bucket=self.lane['source']['bucket'],Key=self.lane['source']['prefix']+'hello.txt',Body=b'changed')
  result=runner.run_once();self.assertEqual(result['counts']['retry'],1);self.assertEqual(result['jobs'][0]['copied'],0)
 def test_live_lease_not_stolen_and_expired_lease_recovers(self):
  runner=self.runner();runner.discover()
  with runner.db() as db:db.execute("UPDATE jobs SET state='working',lease=1100")
  self.assertEqual(self.runner().run_once()['counts']['working'],1)
  self.now[0]=1200;self.assertEqual(self.runner().run_once()['counts']['complete'],1)
 def test_changed_source_version_creates_new_output_identity(self):
  runner=self.runner();runner.discover();runner.run_once()
  self.s3.put_object(Bucket=self.lane['source']['bucket'],Key=self.lane['source']['prefix']+'hello.txt',Body=b'new version')
  self.assertEqual(runner.discover()['discovered'],1);self.assertEqual(runner.run_once()['counts']['complete'],2)

class SeparateEndpointTests(unittest.TestCase):
 setUp=MediaRuntimeTests.setUp
 runner=MediaRuntimeTests.runner
 def configure(self):
  target=copy.deepcopy(self.blueprint['s3']);target['endpoint']='http://destination:9090'
  for lane in self.blueprint['pipelines']:lane['destination']['s3']=copy.deepcopy(target)
  self.destination=S3();self.calls=[]
  def factory(config):
   self.calls.append(config['endpoint'])
   return self.destination if config['endpoint']==target['endpoint'] else self.s3
  return factory
 def test_separate_endpoints_copy_only_to_destination_and_restart_dedup(self):
  factory=self.configure();source_before=copy.deepcopy(self.s3.objects)
  runner=MediaRunner(self.blueprint,Path(self.temp.name)/'separate.db',client_factory=factory,publisher=lambda t,e:self.events.append(e))
  self.assertEqual(len(self.calls),2);runner.discover();self.assertEqual(runner.run_once()['counts']['complete'],1)
  self.assertEqual(self.s3.objects,source_before);self.assertEqual(len(self.destination.objects),2)
  self.assertTrue(all(bucket==self.lane['destination']['bucket'] for bucket,key in self.destination.objects))
  self.assertEqual(self.events[0]['destination']['endpoint'],'http://destination:9090')
  restarted=MediaRunner(self.blueprint,Path(self.temp.name)/'separate.db',client_factory=factory)
  self.assertEqual(restarted.discover()['discovered'],0)
 def test_destination_outage_retries_without_source_writes(self):
  factory=self.configure();before=copy.deepcopy(self.s3.objects)
  def unavailable(**kwargs):raise RuntimeError('secret transport error')
  self.destination.put_object=unavailable
  runner=MediaRunner(self.blueprint,Path(self.temp.name)/'outage.db',client_factory=factory)
  runner.discover();status=runner.run_once()
  self.assertEqual(status['counts']['retry'],1);self.assertEqual(status['jobs'][0]['copied'],0)
  self.assertEqual(self.s3.objects,before);self.assertNotIn('secret transport error',json.dumps(status))
 def test_differing_endpoint_cannot_silently_use_single_client(self):
  self.configure()
  with self.assertRaisesRegex(MediaError,'requires_client_factory'):self.runner()
 def test_endpoint_change_cannot_reuse_existing_state(self):
  runner=self.runner();runner.discover();factory=self.configure()
  with self.assertRaisesRegex(MediaError,'state_blueprint_mismatch'):
   MediaRunner(self.blueprint,Path(self.temp.name)/'jobs.db',client_factory=factory)
 def test_source_endpoint_changes_identity_even_for_same_bucket_key_etag(self):
  first=self.runner();first.discover();old=first.status()['jobs'][0]['id']
  for lane in self.blueprint['pipelines']:
   lane['source']['s3']={**self.blueprint['s3'],'endpoint':'http://other-source:9090'}
  second=MediaRunner(self.blueprint,Path(self.temp.name)/'new.db',client_factory=lambda cfg:self.s3)
  second.discover();self.assertNotEqual(old,second.status()['jobs'][0]['id'])
