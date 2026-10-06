import copy
import json
import tempfile
import unittest
import uuid
from pathlib import Path
from flowbridge.fleet import discover_fleet, export_fleet, remap_fleet
from flowbridge.nifi_s3 import ContinuousS3Runner, MigrationError, FLEET_SCHEMA, validate_profile
from test_nifi_s3 import native
from test_media_runtime import S3


def fleet(count=60):
 doc=native();root=doc['flowContents'];base=root['processGroups'][0];groups=[]
 for number in range(count):
  lane=copy.deepcopy(base);old_ids=[lane['identifier']]+[p['identifier'] for p in lane['processors']]+[c['identifier'] for c in lane['connections']]
  replacement={old:str(uuid.uuid5(uuid.NAMESPACE_URL,f'flowbridge-test/{number}/{index}')) for index,old in enumerate(old_ids)}
  encoded=json.dumps(lane)
  for before,after in replacement.items():encoded=encoded.replace(before,after)
  lane=json.loads(encoded);lane['name']=f'workflow-{number:03d}'
  for p in lane['processors']:
   if p['type'].endswith('.UpdateAttribute'):continue
   p['properties']['Bucket']=f'lane-{number:03d}-'+('target' if p['type'].endswith('.PutS3Object') else 'source')
  groups.append(lane)
 root['processGroups']=[{'identifier':str(uuid.uuid4()),'name':'department-a','processGroups':groups[:count//2]},{'identifier':str(uuid.uuid4()),'name':'department-b','processGroups':[{'identifier':str(uuid.uuid4()),'name':'nested','processGroups':groups[count//2:]}]}]
 return doc


class FleetTests(unittest.TestCase):
 def test_sixty_nested_workflows_discovered_no_media_type_uniqueness_limit(self):
  result=discover_fleet(fleet());self.assertTrue(result['report']['ok'],result['report']);self.assertEqual(result['mapped'],60);self.assertEqual(result['unmapped'],0)
  self.assertEqual(len(result['profile']['lanes']),60);self.assertEqual(result['profile']['schema'],FLEET_SCHEMA)
  self.assertTrue(any('nested' in row['path'] for row in result['inventory']))
  self.assertEqual(len({row['id'] for row in result['inventory']}),60)
 def test_root_lane_and_scoped_controller_are_discovered(self):
  doc=native();lane=copy.deepcopy(doc['flowContents']['processGroups'][0]);lane['controllerServices']=doc['flowContents']['controllerServices']
  result=discover_fleet({'flowContents':lane});self.assertTrue(result['report']['ok'],result['report']);self.assertEqual(result['mapped'],1)
 def test_discovery_stable_ids_and_paths(self):
  doc=fleet(4);self.assertEqual(discover_fleet(doc)['inventory'],discover_fleet(copy.deepcopy(doc))['inventory'])
 def test_unknown_processor_keeps_inventory_and_blocks_all_export(self):
  doc=fleet(4);lane=doc['flowContents']['processGroups'][0]['processGroups'][0];lane['processors'][1]['type']='org.example.Unknown'
  result=export_fleet(doc);self.assertFalse(result['report']['ok']);self.assertEqual(result['files'],{});self.assertEqual(result['mapped'],3);self.assertEqual(result['unmapped'],1);self.assertIsNone(result['profile'])
 def test_cross_group_dependency_ports_and_duplicates_block(self):
  for mutation in ('cross_edge','port','duplicate'):
   doc=fleet(4);lanes=doc['flowContents']['processGroups'][0]['processGroups']
   if mutation=='cross_edge':lanes[0]['connections'][0]['destination']['id']=lanes[1]['processors'][1]['identifier']
   elif mutation=='port':doc['flowContents']['inputPorts']=[{'identifier':str(uuid.uuid4()),'name':'upstream'}]
   else:lanes[1]['identifier']=lanes[0]['identifier']
   self.assertFalse(export_fleet(doc)['report']['ok'])
 def test_secrets_block_without_echo(self):
  doc=fleet(2);doc['flowContents']['controllerServices'][0]['properties']['Secret Access Key']='do-not-echo'
  result=export_fleet(doc);self.assertFalse(result['report']['ok']);self.assertNotIn('do-not-echo',json.dumps(result))
 def test_lane_count_bounds(self):
  for count in (0,257):self.assertFalse(discover_fleet(fleet(count))['report']['ok'])
  self.assertTrue(discover_fleet(fleet(1))['report']['ok'])
  self.assertTrue(discover_fleet(fleet(256))['report']['ok'])
 def test_shadow_requires_complete_unique_isolated_destinations(self):
  profile=discover_fleet(fleet(3))['profile'];mapping={lane['id']:{**lane['destination'],'bucket':f'shadow-{i:03d}'} for i,lane in enumerate(profile['lanes'])}
  shadow=remap_fleet(profile,mapping);validate_profile(shadow)
  for mode in ('missing','blue_overlap','source_overlap','duplicate'):
   changed=copy.deepcopy(mapping);ids=list(changed)
   if mode=='missing':changed.pop(ids[0])
   elif mode=='blue_overlap':changed[ids[0]]=profile['lanes'][0]['destination']
   elif mode=='source_overlap':changed[ids[0]]={k:v for k,v in profile['lanes'][0]['source'].items() if k!='prefix'}
   else:changed[ids[1]]=changed[ids[0]]
   with self.assertRaises(MigrationError):remap_fleet(profile,changed)
 def test_shadow_runner_copies_without_claiming_source_stop(self):
  profile=discover_fleet(fleet(2))['profile'];mapping={lane['id']:{**lane['destination'],'bucket':f'shadow-{i:03d}'} for i,lane in enumerate(profile['lanes'])};shadow=remap_fleet(profile,mapping)
  source=S3();target=S3();lane=profile['lanes'][0];source.put_object(Bucket=lane['source']['bucket'],Key='a.png',Body=b'payload')
  with tempfile.TemporaryDirectory() as folder:
   factory=lambda cfg:source if cfg['endpoint']==lane['source']['endpoint'] else target
   runner=ContinuousS3Runner(shadow,Path(folder)/'shadow.db',factory)
   self.assertEqual(runner.establish_shadow(profile),{'mode':'isolated_shadow','cutover':False})
   self.assertEqual(runner.run_once()['counts']['processed'],1)
   self.assertIn(('shadow-000','a.png'),target.objects);self.assertNotIn((lane['destination']['bucket'],'a.png'),target.objects)
   original=ContinuousS3Runner(profile,Path(folder)/'original.db',factory)
   with self.assertRaisesRegex(MigrationError,'overlaps_production'):original.establish_shadow(profile)
 def test_sixty_workflow_example_is_assessable(self):
  document=json.loads((Path(__file__).parent.parent/'examples/nifi-fleet-60.json').read_text())
  result=discover_fleet(document);self.assertTrue(result['report']['ok']);self.assertEqual(result['mapped'],60)
 def test_export_includes_full_inventory_and_runtime(self):
  result=export_fleet(fleet(4));self.assertTrue(result['report']['ok']);self.assertEqual(len(json.loads(result['files']['pipeline-inventory.json'])),4);self.assertIn('FLEET_SCHEMA',result['files']['flowbridge/nifi_s3.py'])
