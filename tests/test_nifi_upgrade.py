import copy
import unittest
from flowbridge.nifi_upgrade import plan_upgrade

class UpgradeTests(unittest.TestCase):
 def fixture(self):
  return {'flowContents':{'processGroups':[{'processors':[{'type':'org.apache.nifi.processors.standard.JoltTransformJSON','bundle':{'artifact':'nifi-standard-nar','version':'1.28.1'},'properties':{'jolt-spec':'sensitive-content-not-for-report'}}]}]}}
 def test_nested_plan_preserves_original_and_never_echoes_values(self):
  data=self.fixture(); before=copy.deepcopy(data);plan=plan_upgrade(data)
  self.assertEqual(data,before);self.assertFalse(plan['ready_to_deploy']);self.assertEqual(len(plan['changes']),1)
  self.assertNotIn('sensitive-content-not-for-report',str(plan))
  self.assertTrue(any(x['op']=='move' for x in plan['changes'][0]['operations']))
 def test_property_collision_blocks_all_patches(self):
  data=self.fixture();data['flowContents']['processGroups'][0]['processors'][0]['properties']['Jolt Specification']='different'
  plan=plan_upgrade(data);self.assertTrue(plan['errors']);self.assertEqual(plan['changes'],[])
 def test_unexpected_bundle_blocks(self):
  data=self.fixture();data['flowContents']['processGroups'][0]['processors'][0]['bundle']['artifact']='custom'
  self.assertTrue(plan_upgrade(data)['errors'])
 def test_unknown_components_are_inventoried_not_changed(self):
  plan=plan_upgrade({'rootGroup':{'processors':[{'type':'custom.Processor'}]}})
  self.assertFalse(plan['inventory'][0]['rule_available']);self.assertEqual(plan['changes'],[])
 def test_cache_service_and_descriptor_rename(self):
  plan=plan_upgrade({'rootGroup':{'controllerServices':[{'type':'org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService'}]}})
  self.assertEqual(plan['changes'][0]['operations'][-1]['value'],'org.apache.nifi.distributed.cache.client.MapCacheClientService')
