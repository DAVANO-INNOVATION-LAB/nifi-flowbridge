import copy
import json
import unittest
from pathlib import Path
from flowbridge.targets.camel_k import export_camel_k

class CamelExportTests(unittest.TestCase):
 def setUp(self):self.document=json.loads((Path(__file__).parent.parent/'examples/nifi-continuous-media.json').read_text())
 def test_native_routes_are_camel_k_not_python_executor(self):
  result=export_camel_k(self.document);self.assertTrue(result['report']['ok'],result['report'])
  integration=json.loads(result['files']['camel-k-integration.json']);self.assertEqual(integration['kind'],'Integration');self.assertEqual(integration['spec']['replicas'],1)
  self.assertEqual(integration['spec']['traits']['deployment']['strategy'],'Recreate')
  self.assertEqual(integration['spec']['traits']['camel'],{'runtimeProvider':'plain-quarkus','runtimeVersion':'3.39.1'})
  java=result['files']['FlowbridgeS3.java'];self.assertEqual(java.count('.routeId('),3);self.assertIn('aws2-s3://',java);self.assertIn('deleteAfterRead=false',java);self.assertIn('.eager(false)',java);self.assertIn('setMaxFileStoreSize(0)',java);self.assertIn('durable_identity_capacity_reached',java)
  self.assertNotIn('media_runtime.py',str(result['files']));self.assertFalse(result['report']['runtime_verified']);self.assertFalse(result['report']['cutover_supported'])
 def test_unknown_transformation_never_exported(self):
  self.document['flowContents']['processGroups'][0]['processors'][0]['type']='org.example.Unknown'
  result=export_camel_k(self.document);self.assertFalse(result['report']['ok']);self.assertEqual(result['files'],{})
 def test_secret_never_copied(self):
  self.document['flowContents']['controllerServices'][0]['properties']['Secret Access Key']='never-echo'
  result=export_camel_k(self.document);self.assertFalse(result['report']['ok']);self.assertNotIn('never-echo',json.dumps(result))
 def test_large_fleet_explicitly_blocked(self):
  document=json.loads((Path(__file__).parent.parent/'examples/nifi-fleet-60.json').read_text())
  result=export_camel_k(document);self.assertFalse(result['report']['ok']);self.assertEqual(result['files'],{})

class ConnectedCamelInspectionTests(unittest.TestCase):
 def test_inspection_requires_matching_owned_running_resources(self):
  from flowbridge.targets.camel_k import inspect_camel_k
  class Client:
   def get(self,kind,namespace,name):
    if kind=='Integration':return {'metadata':{'uid':'integration-uid'},'status':{'phase':'Running'}}
    if name=='camel-k-operator':return {'status':{'readyReplicas':1}}
    return {'metadata':{'generation':2,'ownerReferences':[{'kind':'Integration','uid':'integration-uid'}]},'spec':{'replicas':1},'status':{'observedGeneration':2,'readyReplicas':1}}
  result=inspect_camel_k(Client(),'default','flowbridge-s3');self.assertTrue(result['runtime_observed']);self.assertFalse(result['cutover_supported']);self.assertFalse(result['data_equivalence_verified'])
 def test_inspection_sanitizes_client_failures(self):
  from flowbridge.targets.camel_k import inspect_camel_k
  class Client:
   def get(self,*args):raise RuntimeError('sensitive-token')
  result=inspect_camel_k(Client(),'default','flowbridge-s3');self.assertFalse(result['ok']);self.assertNotIn('sensitive-token',json.dumps(result))
