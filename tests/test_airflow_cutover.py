import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path
import json
from flowbridge.targets.airflow import _fingerprint
from flowbridge.targets.airflow import CONTRACT, export_airflow_fleet, prepare_cutover, run_lane
from test_nifi_s3 import native
from test_media_runtime import S3

class AirflowCutoverTests(unittest.TestCase):
 def test_explicit_contract_and_unsupported_graph_block(self):
  self.assertFalse(export_airflow_fleet(native())['report']['ok'])
  doc=native();doc['flowContents']['processGroups'][0]['processors'][0]['type']='custom.ListS3'
  result=export_airflow_fleet(doc,CONTRACT);self.assertFalse(result['report']['ok']);self.assertEqual(result['files'],{})
 def test_package_paused_and_has_executable_dependencies(self):
  result=export_airflow_fleet(native(),CONTRACT);self.assertTrue(result['report']['ok'],result)
  dag=result['files']['dags/flowbridge_s3.py'];compile(dag,'dag.py','exec')
  self.assertIn('is_paused_upon_creation=True',dag);self.assertIn('schedule=timedelta(minutes=1)',dag)
  self.assertIn('flowbridge/targets/airflow.py',result['files']);self.assertEqual(dag.count('migrate_lane.override'),3)
 def test_task_cannot_establish_cutover_and_preserves_durable_dedup(self):
  profile=export_airflow_fleet(native(),CONTRACT)['profile'];lane=profile['lanes'][0];source=S3();target=S3()
  factory=lambda config:target if config['endpoint']=='http://target:9090' else source
  source.put_object(Bucket=lane['source']['bucket'],Key='test.png',Body=b'bytes')
  with tempfile.TemporaryDirectory() as state:
   with self.assertRaisesRegex(Exception,'cutover_not_established'):run_lane(profile,lane['id'],state,factory)
   with self.assertRaisesRegex(ValueError,'source_stop_confirmation'):prepare_cutover(profile,state,client_factory=factory)
   prepare_cutover(profile,state,True,True,factory)
   self.assertEqual(run_lane(profile,lane['id'],state,factory)['processed'],1)
   self.assertEqual(run_lane(profile,lane['id'],state,factory)['processed'],0)
   with self.assertRaisesRegex(ValueError,'unknown_lane'):run_lane(profile,'unknown',state,factory)
 def test_failed_transfer_fails_airflow_task_instead_of_green(self):
  class Failed:
   def run_once(self):return {'counts':{'processed':0,'failures':1}}
  with tempfile.TemporaryDirectory() as state:
   Path(state,'cutover-approved.json').write_text(json.dumps({'profile_sha256':_fingerprint({})}))
   with patch('flowbridge.targets.airflow._runner',return_value=Failed()):
    with self.assertRaisesRegex(RuntimeError,'airflow_batch_failed'):run_lane({},'lane',state)

 def test_partial_baseline_never_authorizes_prepared_lane(self):
  profile=export_airflow_fleet(native(),CONTRACT)['profile']
  class Ready:
   def establish_cutover(self,*args):return {}
  with tempfile.TemporaryDirectory() as state:
   with patch('flowbridge.targets.airflow._runner',side_effect=[Ready(),ValueError('second_lane_failed')]):
    with self.assertRaisesRegex(ValueError,'second_lane_failed'):prepare_cutover(profile,state,True,True)
   self.assertFalse(Path(state,'cutover-approved.json').exists())
   with self.assertRaisesRegex(ValueError,'cutover_not_established'):run_lane(profile,profile['lanes'][0]['id'],state)
