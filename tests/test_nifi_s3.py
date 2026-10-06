import copy
import io
import json
import tempfile
import unittest
import uuid
from pathlib import Path
from flowbridge.nifi_s3 import analyze_nifi_s3, export_nifi_s3, ContinuousS3Runner, MigrationError
from test_media_runtime import S3


def native():
 service=str(uuid.uuid4());groups=[]
 for kind in ('image','text','video'):
  gid=str(uuid.uuid4());nodes=[]
  for processor in ('ListS3','FetchS3Object','UpdateAttribute','PutS3Object'):
   props={'Bucket':kind+('-target' if processor=='PutS3Object' else '-source'),'Region':'us-east-1','Endpoint Override URL':'http://target:9090' if processor=='PutS3Object' else 'http://source:9090','Use Path Style Access':'true','AWS Credentials Provider Service':service}
   if processor=='ListS3':props.update({'Use Versions':'false','Listing Strategy':'timestamps'})
   elif processor=='UpdateAttribute':props={'media_type':kind,'Store State':'Do not store state'}
   else:props['Object Key']='${filename}'
   if processor=='PutS3Object':
    props['media_type']='${media_type}'
    props.update({key:"${literal('')}" for key in ('FullControl User List','Read Permission User List','Read ACL User List','Write ACL User List','Canned ACL')})
   namespace='org.apache.nifi.processors.attributes.' if processor=='UpdateAttribute' else 'org.apache.nifi.processors.aws.s3.'
   nodes.append({'identifier':str(uuid.uuid4()),'type':namespace+processor,'bundle':{'group':'org.apache.nifi','artifact':'nifi-update-attribute-nar' if processor=='UpdateAttribute' else 'nifi-aws-nar','version':'2.12.0'},'properties':props,'groupIdentifier':gid})
  edges=[{'identifier':str(uuid.uuid4()),'source':{'id':a['identifier']},'destination':{'id':b['identifier']},'selectedRelationships':['success']} for a,b in zip(nodes,nodes[1:])]
  groups.append({'identifier':gid,'processors':nodes,'connections':edges})
 return {'flowContents':{'identifier':str(uuid.uuid4()),'processGroups':groups,'controllerServices':[{'identifier':service,'type':'org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService','properties':{'Use Default Credentials':'true'}}]}}


class NativeParserTests(unittest.TestCase):
 def test_native_without_marker_parses_and_exports_executable(self):
  doc=native();result=analyze_nifi_s3(doc);self.assertTrue(result['report']['ok'],result)
  self.assertEqual(len(result['profile']['lanes']),3)
  package=export_nifi_s3(doc);self.assertIn('flowbridge/nifi_s3.py',package['files']);self.assertIn('--watch',package['files']['README.md'])
 def test_rejects_unknown_node_transform_property_and_relationship(self):
  mutations=[lambda d:d['flowContents']['processGroups'][0]['processors'][2]['properties'].update(media_type='${filename}'),lambda d:d['flowContents']['processGroups'][0]['processors'][1]['properties'].update(**{'Range Length':'32'}),lambda d:d['flowContents']['processGroups'][0]['connections'][0].update(selectedRelationships=['failure']),lambda d:d['flowContents']['processGroups'][0]['processors'][2].update(annotationData='advanced rules'),lambda d:d['flowContents']['processGroups'][0]['processors'][3]['properties'].update(**{'Object Key':'renamed'})]
  for mutate in mutations:
   doc=native();mutate(doc);self.assertFalse(analyze_nifi_s3(doc)['report']['ok'])
 def test_embedded_credentials_never_echo(self):
  doc=native();doc['flowContents']['controllerServices'][0]['properties']['Secret Key']='never-echo'
  result=export_nifi_s3(doc);self.assertFalse(result['report']['ok']);self.assertNotIn('never-echo',json.dumps(result));self.assertEqual(result['files'],{})
 def test_default_acl_mapping_and_missing_dynamic_metadata_block(self):
  for action in ('default_acl','missing_media'):
   doc=native();props=doc['flowContents']['processGroups'][0]['processors'][3]['properties']
   if action=='default_acl':props['Canned ACL']='${s3.permissions.cannedacl}'
   else:del props['media_type']
   self.assertFalse(export_nifi_s3(doc)['report']['ok'])
 def test_future_or_wrong_source_versions_block(self):
  from flowbridge.service import convert
  for source,version in [('nifi','3'),('nifi','1'),('kafka','auto')]:
   self.assertFalse(convert(native(),source,'continuous-worker',nifi_version=version)['report']['ok'])
 def test_real_native_fixture_assesses_without_false_credential_alarm(self):
  from flowbridge.service import assess
  fixture=Path(__file__).parent/'integration/continuous/native-fixture.json'
  doc=json.loads(fixture.read_text());result=assess(doc,'nifi','2')
  self.assertTrue(result['report']['ok'],result['report'])
  self.assertTrue(export_nifi_s3(doc)['report']['ok'])
 def test_service_routes_explicit_continuous_target(self):
  from flowbridge.service import convert
  self.assertTrue(convert(native(),'nifi','continuous-worker')['report']['ok'])


class ContinuousRuntimeTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
  result=analyze_nifi_s3(native());self.assertTrue(result['report']['ok'],result)
  self.profile=result['profile'];self.source=S3();self.target=S3();
  old_head=self.target.head_object
  self.target.head_object=lambda Bucket,Key:{**old_head(Bucket,Key),'Metadata':self.target.objects[(Bucket,Key)][1].get('Metadata',{})}
  self.lane=self.profile['lanes'][0]
  self.factory=lambda cfg:self.target if cfg['endpoint']=='http://target:9090' else self.source
 def runner(self):return ContinuousS3Runner(self.profile,Path(self.temp.name)/'state.db',self.factory)
 def seed(self,key,body):self.source.put_object(Bucket=self.lane['source']['bucket'],Key=key,Body=body)
 def test_requires_cutover_and_reconciles_baseline_without_recopy(self):
  self.seed('before.png',b'original');self.target.put_object(Bucket=self.lane['destination']['bucket'],Key='before.png',Body=b'original',Metadata={'media_type':'image'})
  runner=self.runner()
  with self.assertRaisesRegex(MigrationError,'cutover_not_established'):runner.run_once()
  with self.assertRaisesRegex(MigrationError,'source_stop_confirmation'):runner.establish_cutover()
  self.assertEqual(runner.establish_cutover(True)['verified_existing'],1)
  puts=self.target.puts;self.assertEqual(runner.run_once()['counts']['processed'],0);self.assertEqual(self.target.puts,puts)
 def test_missing_destination_needs_backfill_approval(self):
  self.seed('new.png',b'x');runner=self.runner()
  with self.assertRaisesRegex(MigrationError,'destination_missing'):runner.establish_cutover(True)
  self.assertEqual(runner.establish_cutover(True,True)['pending_backfill'],1)
  result=runner.run_once();self.assertEqual(result['counts']['processed'],1)
  value=self.target.objects[(self.lane['destination']['bucket'],'new.png')];self.assertEqual(value[0],b'x');self.assertEqual(value[1]['Metadata']['media_type'],'image')
 def test_new_arrivals_then_restart_then_changed_key_are_processed(self):
  runner=self.runner();runner.establish_cutover(True)
  self.seed('during.png',b'one');self.assertEqual(runner.run_once()['counts']['processed'],1)
  self.seed('after.png',b'two');self.assertEqual(runner.run_once()['counts']['processed'],1)
  restarted=self.runner();self.assertEqual(restarted.run_once()['counts']['processed'],0)
  self.seed('during.png',b'changed');self.assertEqual(restarted.run_once()['counts']['processed'],1)
  self.assertEqual(self.target.objects[(self.lane['destination']['bucket'],'during.png')][0],b'changed')
 def test_failed_put_not_checkpointed_and_retry_recovers(self):
  runner=self.runner();runner.establish_cutover(True);self.seed('retry.png',b'bytes')
  original=self.target.put_object
  def fail(**kwargs):raise RuntimeError('secret detail')
  self.target.put_object=fail;result=runner.run_once();self.assertEqual(result['counts']['failures'],1);self.assertNotIn('secret detail',json.dumps(result))
  self.target.put_object=original;self.assertEqual(runner.run_once()['counts']['processed'],1)
 def test_second_owner_refused_and_released_after_poll(self):
  runner=self.runner();runner.establish_cutover(True);other=self.runner()
  with runner.exclusive():
   with self.assertRaisesRegex(MigrationError,'another_worker_owns_state'):other.run_once()
  self.assertEqual(other.run_once()['counts']['processed'],0)
 def test_equal_content_wrong_metadata_requires_approval(self):
  self.seed('existing.png',b'bytes');self.target.put_object(Bucket=self.lane['destination']['bucket'],Key='existing.png',Body=b'bytes')
  with self.assertRaisesRegex(MigrationError,'metadata_differ'):self.runner().establish_cutover(True)
 def test_listing_bound_failure_is_visible(self):
  runner=self.runner();runner.establish_cutover(True)
  def invalid(**kwargs):return {'IsTruncated':True}
  self.source.list_objects_v2=invalid
  self.assertEqual(runner.run_once()['counts']['failures'],3)
 def test_different_profile_cannot_adopt_prior_ledger(self):
  self.runner();self.profile['lanes'][0]['destination']['bucket']='other-bucket'
  with self.assertRaisesRegex(MigrationError,'state_profile_mismatch'):self.runner()

class RecoveryRegressionTests(unittest.TestCase):
 setUp=ContinuousRuntimeTests.setUp
 runner=ContinuousRuntimeTests.runner
 seed=ContinuousRuntimeTests.seed
 def test_rebaseline_missing_destination_invalidates_old_checkpoint(self):
  runner=self.runner();runner.establish_cutover(True);self.seed('gone',b'data');runner.run_once()
  del self.target.objects[(self.lane['destination']['bucket'],'gone')]
  with self.assertRaises(MigrationError):runner.establish_cutover(True)
  with self.assertRaisesRegex(MigrationError,'cutover_not_established'):runner.run_once()
  self.assertEqual(runner.establish_cutover(True,True)['pending_backfill'],1)
  self.assertEqual(runner.run_once()['counts']['processed'],1)
 def test_unreadable_object_does_not_starve_later_objects(self):
  runner=self.runner();runner.establish_cutover(True);self.seed('bad',b'x');self.seed('good',b'y')
  original=self.source.head_object
  def head(Bucket,Key):
   if Key=='bad':raise RuntimeError('private-secret')
   return original(Bucket,Key)
  self.source.head_object=head;result=runner.run_once()
  self.assertEqual(result['counts'],{'processed':1,'failures':1});self.assertNotIn('private-secret',json.dumps(result))
  self.source.head_object=original;self.assertEqual(runner.run_once()['counts']['processed'],1)
 def test_repeating_pagination_token_terminates(self):
  runner=self.runner();runner.establish_cutover(True)
  self.source.list_objects_v2=lambda **kw:{'IsTruncated':True,'NextContinuationToken':'same','Contents':[]}
  result=runner.run_once();self.assertEqual(result['counts']['failures'],3)
  self.assertTrue(all(f['code']=='invalid_listing_page' for f in result['failures']))
 def test_crash_after_write_before_checkpoint_replays_safely(self):
  runner=self.runner();runner.establish_cutover(True);self.seed('crash',b'complete bytes')
  runner._checkpoint=lambda *a:(_ for _ in ()).throw(RuntimeError('synthetic crash window'))
  self.assertEqual(runner.run_once()['counts']['failures'],1)
  restarted=self.runner();self.assertEqual(restarted.run_once()['counts']['processed'],1)
  self.assertEqual(restarted.run_once()['counts']['processed'],0)
  self.assertEqual(self.target.objects[(self.lane['destination']['bucket'],'crash')][0],b'complete bytes')
 def test_oversized_object_is_reported_without_starving_valid_neighbor(self):
  from unittest.mock import patch
  runner=self.runner();runner.establish_cutover(True);self.seed('too-large',b'x'*9);self.seed('within-limit',b'x'*8)
  with patch('flowbridge.nifi_s3.MAX_OBJECT',8):result=runner.run_once()
  self.assertEqual(result['counts'],{'processed':1,'failures':1})
  self.assertEqual(result['failures'][0]['code'],'object_too_large')
 def test_changed_source_during_read_does_not_checkpoint_wrong_identity(self):
  runner=self.runner();runner.establish_cutover(True);self.seed('changing',b'before')
  original=self.source.get_object
  def racing(**kw):
   self.seed('changing',b'after');return original(**kw)
  self.source.get_object=racing;self.assertEqual(runner.run_once()['counts']['failures'],1)
  self.source.get_object=original;self.assertEqual(runner.run_once()['counts']['processed'],1)
  self.assertEqual(self.target.objects[(self.lane['destination']['bucket'],'changing')][0],b'after')
 def test_baseline_rejects_different_content_type(self):
  runner=self.runner();self.seed('typed',b'x');self.target.put_object(Bucket=self.lane['destination']['bucket'],Key='typed',Body=b'x',Metadata={'media_type':'image'})
  original=self.source.get_object
  self.source.get_object=lambda **kw:{**original(**kw),'ContentType':'text/plain'}
  with self.assertRaisesRegex(MigrationError,'metadata_differ'):runner.establish_cutover(True)
