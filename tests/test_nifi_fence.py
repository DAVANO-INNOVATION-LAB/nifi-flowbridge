import copy
import json
import tempfile
import unittest
from pathlib import Path
from flowbridge.live.nifi_fence import NiFiFence
from flowbridge.live.platforms import PlatformError

class Client:
 _base='https://nifi.example/nifi-api'
 def __init__(self):
  self.states={'source':'RUNNING','sink':'RUNNING'};self.writes=[];self.queued=0;self.active=0;self.unknown=False;self.config={'properties':{'topic':'original'}};self.service_properties={'bootstrap.servers':'one'};self.relationships=['success']
 def request(self,method,path,document=None,query=None):
  if path=='/process-groups/group':return {'component':{'id':'group'}}
  if path=='/flow/process-groups/group/controller-services':return {'controllerServices':[{'id':'service','revision':{'version':0},'component':{'id':'service','type':'Kafka3ConnectionService','properties':self.service_properties}}]}
  if path=='/flow/process-groups/root':return {'processGroupFlow':{'id':'group'}}
  if path=='/flow/process-groups/group':
   types={'source':'org.apache.nifi.kafka.processors.ConsumeKafka','sink':'org.apache.nifi.kafka.processors.PublishKafka'}
   if self.unknown:types['sink']='example.Unmapped'
   return {'processGroupFlow':{'id':'group','flow':{'processors':[{'id':pid,'component':{'id':pid,'type':kind,'config':self.config}} for pid,kind in types.items()],'connections':[{'id':'edge','component':{'source':{'id':'source'},'destination':{'id':'sink'},'selectedRelationships':self.relationships}}]}}}
  if path=='/flow/process-groups/group/status':return {'processGroupStatus':{'aggregateSnapshot':{'queuedCount':self.queued,'activeThreadCount':self.active}}}
  pid=path.split('/')[2]
  if method=='PUT':
   assert document['revision']=={'version':1};assert document['disconnectedNodeAcknowledged'] is False
   self.writes.append(pid);self.states[pid]='STOPPED';return {}
  return {'revision':{'version':1},'component':{'id':pid,'state':self.states[pid]}}

class FenceTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.client=Client();self.t=100
  self.fence=NiFiFence(self.client,self.temp.name,clock=lambda:self.t,sleep=self.advance)
 def advance(self,_):self.t+=1
 def test_source_first_persistent_proof_and_live_revalidation(self):
  proof=self.fence.stop_and_drain('group');self.assertEqual(self.client.writes,['source','sink'])
  file=Path(self.temp.name,proof['id']+'.json');self.assertEqual(file.stat().st_mode&0o777,0o600)
  self.assertNotIn('https://',file.read_text());self.assertFalse(proof['cutover_ready'])
  self.assertTrue(self.fence.revalidate(proof['id'])['source_stopped_and_drained'])
  self.client.states['source']='RUNNING'
  with self.assertRaisesRegex(PlatformError,'no longer'):self.fence.revalidate(proof['id'])
 def test_unknown_type_blocks_before_any_write(self):
  self.client.unknown=True
  with self.assertRaises(PlatformError):self.fence.stop_and_drain('group')
  self.assertEqual(self.client.writes,[]);self.assertEqual(list(Path(self.temp.name).iterdir()),[])
 def test_nonempty_queue_times_out_without_proof(self):
  self.client.queued=1
  with self.assertRaisesRegex(PlatformError,'deadline exceeded'):self.fence.stop_and_drain('group',timeout=1)
  self.assertEqual(self.client.writes,['source']);self.assertEqual(list(Path(self.temp.name).iterdir()),[])
 def test_missing_counter_is_not_zero(self):
  self.client.queued=None
  with self.assertRaisesRegex(PlatformError,'trustworthy'):self.fence.stop_and_drain('group')
  self.assertEqual(list(Path(self.temp.name).iterdir()),[])
 def test_proof_cannot_cross_endpoint_or_traverse_path(self):
  proof=self.fence.stop_and_drain('group');other=Client();other._base='https://other/nifi-api'
  with self.assertRaisesRegex(PlatformError,'another'):NiFiFence(other,self.temp.name).revalidate(proof['id'])
  with self.assertRaises(PlatformError):self.fence.revalidate('../proof')
 def test_graph_change_invalidates_past_proof(self):
  proof=self.fence.stop_and_drain('group');self.client.unknown=True
  with self.assertRaises(PlatformError):self.fence.revalidate(proof['id'])

 def test_root_alias_resolves_to_live_uuid(self):
  proof=self.fence.stop_and_drain('root');self.assertEqual(proof['group_id'],'group')
 def test_changed_configuration_invalidates_proof(self):
  proof=self.fence.stop_and_drain('group');self.client.config['properties']['topic']='other'
  with self.assertRaisesRegex(PlatformError,'graph changed'):self.fence.revalidate(proof['id'])
 def test_discovery_is_inside_total_deadline(self):
  original=self.client.request
  def delayed(*args,**kwargs):
   result=original(*args,**kwargs);self.t+=3;return result
  self.client.request=delayed
  with self.assertRaisesRegex(PlatformError,'deadline exceeded'):self.fence.stop_and_drain('group',timeout=1)
  self.assertEqual(self.client.writes,[])

 def test_proof_group_binding_and_controller_service_change(self):
  proof=self.fence.stop_and_drain('group')
  self.assertEqual(self.fence.revalidate(proof['id'],expected_group_id='root')['group_id'],'group')
  with self.assertRaisesRegex(PlatformError,'another process group'):self.fence.revalidate(proof['id'],expected_group_id='another')
  self.client.service_properties['bootstrap.servers']='changed'
  with self.assertRaisesRegex(PlatformError,'graph changed'):self.fence.revalidate(proof['id'],expected_group_id='group')

 def test_configuration_change_during_mapping_prevents_all_writes(self):
  def assess(group):
   self.assertEqual(group,'group');self.client.config['properties']['topic']='changed'
  with self.assertRaisesRegex(PlatformError,'during mapping'):self.fence.stop_and_drain('root',validate_scope=assess)
  self.assertEqual(self.client.writes,[])
 def test_mapping_rejection_prevents_all_writes(self):
  def assess(group):raise ValueError('unsupported mapping')
  with self.assertRaisesRegex(ValueError,'unsupported mapping'):self.fence.stop_and_drain('group',validate_scope=assess)
  self.assertEqual(self.client.writes,[])
  with self.assertRaisesRegex(PlatformError,'mapping rejected'):self.fence.stop_and_drain('group',validate_scope=lambda group:False)
  self.assertEqual(self.client.writes,[])

 def test_connection_relationship_change_invalidates_proof(self):
  proof=self.fence.stop_and_drain('group');self.client.relationships=['failure']
  with self.assertRaisesRegex(PlatformError,'graph changed'):self.fence.revalidate(proof['id'])

 def test_invalid_counters_cannot_prove_drain(self):
  for field in ('queued','active'):
   for value in (0.5,0.0,True,False,-1,'-1','0.5','1,00','0,000','1,,000',' 0','0 ','+0',None):
    with self.subTest(field=field,value=value):
     self.client.queued=0;self.client.active=0;setattr(self.client,field,value)
     with self.assertRaisesRegex(PlatformError,'trustworthy'):self.fence._quiet('group',[])
 def test_nonnegative_integer_and_grouped_counters_are_supported(self):
  for value in (0,'0','00'):
   self.client.queued=value;self.client.active=value
   self.assertEqual(self.fence._quiet('group',[]),(True,{}))
  for value in (1,1000,'1000','1,000','12,345,678'):
   self.client.queued=value;self.client.active=0
   self.assertEqual(self.fence._quiet('group',[]),(False,{}))
   self.client.queued=0;self.client.active=value
   self.assertEqual(self.fence._quiet('group',[]),(False,{}))
