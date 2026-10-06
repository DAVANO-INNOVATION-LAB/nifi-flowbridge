import copy
import json
from pathlib import Path
import unittest
from flowbridge.media import validate_media, export_nifi_media, import_nifi_media, NIFI_VERSION


class MediaContractTests(unittest.TestCase):
 def setUp(self):self.blueprint=json.loads((Path(__file__).resolve().parents[1]/'examples/media-etl.json').read_text())
 def test_three_lane_contract_validates_without_mutation(self):
  original=copy.deepcopy(self.blueprint);result=validate_media(self.blueprint)
  self.assertTrue(result['report']['ok']);self.assertEqual(result['blueprint'],original)
  result['blueprint']['name']='modified';self.assertEqual(self.blueprint,original)
 def test_rejects_missing_duplicate_and_feedback_lanes(self):
  for mutate in (lambda b:b['pipelines'].pop(),lambda b:b['pipelines'][1].update(media_type='image'),lambda b:b['pipelines'][1]['source'].update(bucket=b['pipelines'][0]['source']['bucket']),lambda b:b['pipelines'][0]['destination'].update(bucket=b['pipelines'][0]['source']['bucket']),lambda b:b['pipelines'][1]['stream'].update(topic=b['pipelines'][0]['stream']['topic'])):
   b=copy.deepcopy(self.blueprint);mutate(b);self.assertFalse(validate_media(b)['report']['ok'])
 def test_credentials_and_unmapped_fields_block_no_secret_echo(self):
  b=copy.deepcopy(self.blueprint);b['s3']['secret_key']='NEVER_ECHO_THIS'
  result=validate_media(b);self.assertFalse(result['report']['ok']);self.assertNotIn('NEVER_ECHO_THIS',json.dumps(result))
  b=copy.deepcopy(self.blueprint);b['s3']['endpoint']='https://user:NEVER_ECHO_THIS@host'
  self.assertNotIn('NEVER_ECHO_THIS',json.dumps(validate_media(b)))
 def test_unknown_runtime_semantics_block(self):
  for mutation in (lambda b:b['delivery'].update(checkpoint_backend='memory'),lambda b:b['delivery'].update(retries=0),lambda b:b['delivery'].update(deduplication='key-only'),lambda b:b['pipelines'][0]['processing'].update(method='DELETE'),lambda b:b['pipelines'][0]['source'].update(prefix='${untrusted}')):
   b=copy.deepcopy(self.blueprint);mutation(b);self.assertFalse(validate_media(b)['report']['ok'])
 def test_native_roundtrip_checks_actual_graph(self):
  exported=export_nifi_media(self.blueprint);native=json.loads(exported['files']['nifi-media-flow.json'])
  restored=import_nifi_media(native);self.assertTrue(restored['report']['ok']);self.assertEqual(restored['blueprint'],self.blueprint)
  for mutate in (lambda n:n['flowContents']['processGroups'][0]['processors'][0]['properties'].update(Bucket='unexpected-bucket'),lambda n:n['flowContents']['processGroups'][0]['connections'].pop(),lambda n:n['flowContents']['controllerServices'][0]['properties'].update(**{'Use Anonymous Credentials':'true'})):
   changed=copy.deepcopy(native);mutate(changed);self.assertFalse(import_nifi_media(changed)['report']['ok'])
 def test_native_template_has_three_stopped_lanes_and_metadata_streams(self):
  native=json.loads(export_nifi_media(self.blueprint)['files']['nifi-media-flow.json'])
  groups=native['flowContents']['processGroups'];self.assertEqual(len(groups),3)
  ids=set()
  for group in groups:
   types=[p['type'].rsplit('.',1)[-1] for p in group['processors']]
   for required in ('ListS3','FetchS3Object','PutS3Object','PublishKafka','ConsumeKafka','InvokeHTTP'):self.assertIn(required,types)
   for p in group['processors']:
    self.assertEqual(p['scheduledState'],'DISABLED');self.assertEqual(p['bundle']['version'],NIFI_VERSION);self.assertNotIn(p['identifier'],ids);ids.add(p['identifier'])
   self.assertEqual(len(group['outputPorts']),1)
 def test_native_runtime_validated_property_literals(self):
  native=json.loads(export_nifi_media(self.blueprint)['files']['nifi-media-flow.json'])
  for group in native['flowContents']['processGroups']:
   for processor in group['processors']:
    if processor['type'].endswith('.ListS3'):
     self.assertNotEqual(processor['properties'].get('Prefix'), '')
    if processor['type'].endswith('.InvokeHTTP'):
     self.assertEqual(processor['properties']['Response Redirects Enabled'],'False')
 def test_native_service_references_have_remapping_descriptors(self):
  native=json.loads(export_nifi_media(self.blueprint)['files']['nifi-media-flow.json'])
  service_ids={s['identifier'] for s in native['flowContents']['controllerServices']}
  for group in native['flowContents']['processGroups']:
   for processor in group['processors']:
    for key in ('AWS Credentials Provider Service','Kafka Connection Service'):
     if key in processor['properties']:
      self.assertIn(processor['properties'][key],service_ids)
      self.assertTrue(processor['propertyDescriptors'][key]['identifiesControllerService'])
 def test_failure_relationships_not_silently_terminated(self):
  native=json.loads(export_nifi_media(self.blueprint)['files']['nifi-media-flow.json'])
  for group in native['flowContents']['processGroups']:
   for p in group['processors']:
    self.assertFalse(set(p['autoTerminatedRelationships'])&{'failure','Failure','Retry','No Retry'})
   self.assertTrue(any('failure' in c['selectedRelationships'] for c in group['connections']))
 def test_report_explicitly_blocks_full_native_equivalence(self):
  report=export_nifi_media(self.blueprint)['report']
  self.assertFalse(report['ok']);self.assertFalse(report['full_contract_supported']);self.assertFalse(report['ready']);self.assertFalse(report['runtime_validated'])
  self.assertIn('native_media_semantics_incomplete',[x['code'] for x in report['errors']])
 def test_artifacts_contain_license_and_review_documentation(self):
  files=export_nifi_media(self.blueprint)['files'];self.assertIn('Apache License',files['LICENSE']);self.assertIn('Known semantic gaps',files['README-NIFI-MEDIA.md'])
 def test_native_emission_deterministic(self):self.assertEqual(export_nifi_media(self.blueprint),export_nifi_media(self.blueprint))
 def test_nonmedia_native_document_not_claimed(self):
  for value in (None,{},[],{'flowContents':{'comments':'arbitrary'}}):self.assertFalse(import_nifi_media(value)['report']['ok'])

if __name__=='__main__':unittest.main()
