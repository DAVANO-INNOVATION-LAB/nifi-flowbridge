import copy,json,unittest
from pathlib import Path
import test_server
from flowbridge.live.platforms import NiFiClient,PlatformError

class FleetHTTPTests(unittest.TestCase):
 setUpClass=classmethod(test_server.ServerTests.setUpClass.__func__)
 tearDownClass=classmethod(test_server.ServerTests.tearDownClass.__func__)
 request=test_server.ServerTests.request
 def test_sixty_pipeline_example_assessment_and_package(self):
  status,raw,_=self.request('GET','/api/example/fleet');self.assertEqual(status,200);doc=json.loads(raw)
  status,raw,_=self.request('POST','/api/fleet/assess',{'document':doc},{'Content-Type':'application/json'})
  self.assertEqual(status,200,raw);report=json.loads(raw);self.assertEqual(report['mapped'],60);self.assertEqual(report['unmapped'],0)
  status,raw,_=self.request('POST','/api/convert',{'document':doc,'source':'nifi','target':'s3-fleet'},{'Content-Type':'application/json'})
  self.assertEqual(status,200,raw);self.assertIn('s3-fleet-profile.json',json.loads(raw)['files'])
 def test_unknown_native_processor_blocks_whole_fleet(self):
  doc=json.loads(Path('examples/nifi-fleet-60.json').read_text())
  def corrupt(group):
   if group.get('processors'):group['processors'][0]['type']='org.example.Unsupported';return True
   return any(corrupt(g) for g in group.get('processGroups',[]))
  corrupt(doc['flowContents'])
  status,raw,_=self.request('POST','/api/fleet/assess',{'document':doc},{'Content-Type':'application/json'})
  self.assertEqual(status,422);self.assertIsNone(json.loads(raw)['profile']);self.assertFalse(json.loads(raw)['report']['ok'])

class DiscoveryTests(unittest.TestCase):
 def test_nested_discovery_counts_and_access_failure_remain_visible(self):
  class Client:
   def request(self,method,path,**kw):
    if path=='/flow/process-groups/root':return {'processGroupFlow':{'id':'root','flow':{'processGroups':[{'component':{'id':'allowed'}},{'component':{'id':'denied'}}]}}}
    if path=='/flow/process-groups/allowed':return {'processGroupFlow':{'id':'allowed','flow':{'processors':[{'component':{'id':'source','type':'GetHTTP'}}]}}}
    raise PlatformError('http_error','private detail')
  result=NiFiClient(Client()).discover('root')
  self.assertFalse(result['complete']);self.assertFalse(result['report']['ok']);self.assertFalse(result['cutover_ready']);self.assertNotIn('private detail',json.dumps(result))
 def test_metadata_only_group_cycle_is_not_complete(self):
  class Client:
   def request(self,*args,**kw):return {'processGroupFlow':{'id':'root','flow':{'processGroups':[{'component':{'id':'root'}}]}}}
  result=NiFiClient(Client()).discover();self.assertFalse(result['complete']);self.assertFalse(result['cutover_ready'])
