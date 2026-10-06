import copy
import json
import unittest
from flowbridge.graph import analyze_graph


def processor(ident,type_name='InvokeHTTP',properties=None,version='2.9.0'):
 return {'identifier':ident,'name':ident,'type':'org.apache.nifi.processors.standard.'+type_name,'bundle':{'group':'org.apache.nifi','artifact':'nifi-standard-nar','version':version},'properties':properties or {}}

def connection(ident,left,right,relationships=None):
 return {'identifier':ident,'source':{'id':left,'type':'PROCESSOR'},'destination':{'id':right,'type':'PROCESSOR'},'selectedRelationships':relationships or ['success']}

def document():
 return {'flowEncodingVersion':'1.0','flowContents':{'identifier':'root','name':'API ingestion','processors':[processor('http',properties={'HTTP Method':'GET','HTTP URL':'https://example.org/data'}),processor('attributes','UpdateAttribute',{'source':'api'}),processor('json','EvaluateJsonPath',{'id':'$.id'})],'connections':[connection('a','http','attributes'),connection('b','attributes','json')],'processGroups':[]}}


class GraphTests(unittest.TestCase):
 def test_http_and_transformation_pipeline_inventoried(self):
  result=analyze_graph(document(),'2')
  self.assertTrue(result['ok'],result['diagnostics'])
  self.assertEqual(result['graph']['counts']['processors'],3)
  self.assertEqual([p['capability']['category'] for p in result['graph']['processors']],['http_api','attribute_transform','json_extract'])
  self.assertFalse(result['capabilities']['automatic_conversion'])
  self.assertNotIn('original',result)

 def test_encoding_or_bundle_does_not_claim_runtime_version(self):
  result=analyze_graph(document())
  self.assertEqual(result['source_version']['status'],'unverified')
  self.assertEqual(result['source_version']['detected_bundle_majors'],['2'])
  self.assertIn('version_unverified',[d['code'] for d in result['diagnostics']])

 def test_supported_legacy_config_aliases(self):
  value=document();group=value.pop('flowContents')
  for node in group['processors']:
   node['id']=node.pop('identifier');node['config']={'properties':node.pop('properties'),'schedulingStrategy':'TIMER_DRIVEN'}
   node['bundle']['version']='1.28.1'
  result=analyze_graph({'rootGroup':group},'1')
  self.assertTrue(result['ok'],result['diagnostics'])
  self.assertEqual(result['graph']['processors'][0]['properties']['HTTP Method'],'GET')

 def test_nested_groups_ports_funnels_remote_ports_and_fanout(self):
  value=document();root=value['flowContents']
  root['processGroups']=[{'identifier':'nested','name':'nested','processors':[processor('replace','ReplaceText')],'inputPorts':[{'identifier':'in'}],'outputPorts':[{'identifier':'out'}],'funnels':[{'identifier':'funnel'}],'connections':[connection('n1','in','replace'),connection('n2','replace','funnel'),connection('n3','funnel','out')]}]
  root['connections'] += [connection('n4','http','in'),connection('n5','out','json')]
  root['remoteProcessGroups']=[{'identifier':'remote','targetUri':'https://nifi.example.org','inputPorts':[{'identifier':'remote-in'}]}]
  root['connections'].append(connection('r1','json','remote-in'))
  result=analyze_graph(value,'2')
  self.assertTrue(result['ok'],result['diagnostics']);g=result['graph']
  self.assertEqual(g['counts']['groups'],2);self.assertEqual(g['counts']['processors'],4);self.assertEqual(g['counts']['ports'],3);self.assertEqual(g['counts']['funnels'],1)
  self.assertEqual(g['counts']['connections'],8)

 def test_dangling_duplicate_and_cycles_reported(self):
  for mutation,expected in ((lambda r:r['connections'].append(connection('bad','missing','http')),'dangling_connection'),(lambda r:r['processors'].append(processor('http')),'duplicate_identifier'),(lambda r:r['connections'].append(connection('loop','json','http')),'cycle_requires_semantics')):
   value=document();mutation(value['flowContents']);result=analyze_graph(value)
   self.assertFalse(result['ok']);self.assertIn(expected,[d['code'] for d in result['diagnostics']])

 def test_expression_preserved_and_not_executed(self):
  value=document();value['flowContents']['processors'][1]['properties']['x']='${filename:toUpper()}'
  result=analyze_graph(value)
  self.assertTrue(result['ok']);self.assertEqual(result['graph']['processors'][1]['properties']['x'],'${filename:toUpper()}')
  self.assertIn('expression_mapping_required',[d['code'] for d in result['diagnostics']])

 def test_controller_services_and_missing_references(self):
  value=document();root=value['flowContents']
  root['controllerServices']=[{'identifier':'ssl','type':'org.apache.nifi.StandardSSLContextService','properties':{}}]
  root['processors'][0]['properties']['SSL Context Service']='ssl'
  result=analyze_graph(value);self.assertTrue(result['ok'],result['diagnostics'])
  self.assertEqual(result['graph']['processors'][0]['service_references'],[{'property':'SSL Context Service','service_id':'ssl'}])
  root['processors'][0]['properties']['SSL Context Service']='missing'
  result=analyze_graph(value);self.assertFalse(result['ok']);self.assertIn('unresolved_controller_service',[d['code'] for d in result['diagnostics']])

 def test_parameters_and_context_assignment(self):
  value=document();value['parameterContexts']={'ctx':{'name':'parameters','parameters':[{'name':'endpoint','value':'https://example.org'}]}}
  value['flowContents']['parameterContextName']='parameters'
  value['flowContents']['processors'][0]['properties']['HTTP URL']='#{endpoint}'
  result=analyze_graph(value);self.assertTrue(result['ok'],result['diagnostics']);self.assertEqual(result['graph']['parameters'][0]['name'],'endpoint')
  value['flowContents']['processors'][0]['properties']['HTTP URL']='#{missing}'
  result=analyze_graph(value);self.assertFalse(result['ok']);self.assertIn('unresolved_parameter',[d['code'] for d in result['diagnostics']])

 def test_parameter_context_is_not_inherited_from_parent_group(self):
  value=document();value['parameterContexts']={'ctx':{'name':'ctx','parameters':[{'name':'endpoint','value':'https://example.org'}]}}
  value['flowContents']['parameterContextName']='ctx'
  value['flowContents']['processGroups']=[{'identifier':'child','processors':[processor('child-http',properties={'HTTP URL':'#{endpoint}'})]}]
  result=analyze_graph(value)
  self.assertFalse(result['ok']);self.assertIn('unresolved_parameter',[d['code'] for d in result['diagnostics']])
 def test_escaped_parameter_syntax_is_literal(self):
  value=document();value['flowContents']['processors'][1]['properties']['literal']='##{not-a-parameter}'
  result=analyze_graph(value);self.assertTrue(result['ok'],result['diagnostics'])
  self.assertEqual(result['graph']['processors'][1]['parameter_references'],[])
 def test_controller_service_scope_rejects_sibling_service(self):
  value=document();value['flowContents']['processGroups']=[{'identifier':'child','controllerServices':[{'identifier':'nested-ssl','type':'SSLService'}],'processors':[]}]
  value['flowContents']['processors'][0]['properties']['SSL Context Service']='nested-ssl'
  result=analyze_graph(value);self.assertFalse(result['ok']);self.assertIn('controller_service_scope',[d['code'] for d in result['diagnostics']])
 def test_sensitive_properties_parameters_and_urls_never_echoed(self):
  value=document();p=value['flowContents']['processors'][0]
  p['properties'].update({'Password':'SECRETVALUE1','Authorization':'SECRETVALUE2','HTTP URL':'https://user:SECRETVALUE3@example.org'})
  value['parameterContexts']={'ctx':{'name':'ctx','parameters':[{'name':'ordinary','sensitive':True,'value':'SECRETVALUE4'}]}}
  result=analyze_graph(value,include_original=True)
  self.assertFalse(result['ok']);self.assertTrue(result['original_redacted'])
  self.assertNotIn('SECRETVALUE',json.dumps(result));self.assertIn('secret_material',[d['code'] for d in result['diagnostics']])
  self.assertEqual(p['properties']['Password'],'SECRETVALUE1')

 def test_sensitive_descriptor_not_false_positive_when_no_secret(self):
  value=document();p=value['flowContents']['processors'][0]
  p['propertyDescriptors']={'Password':{'name':'Password','sensitive':True}};p['properties']['Password']=None
  result=analyze_graph(value);self.assertTrue(result['ok'],result['diagnostics'])

 def test_named_sensitive_descriptor_hides_opaque_property_value(self):
  value=document();p=value['flowContents']['processors'][0]
  p['propertyDescriptors']={'opaque':{'sensitive':True}};p['properties']['opaque']='SECRETVALUE'
  self.assertNotIn('SECRETVALUE',json.dumps(analyze_graph(value,include_original=True)))

 def test_public_aws_service_reference_and_boolean_are_not_secrets(self):
  value=document();node=value['flowContents']['processors'][0]
  node['properties']['AWS Credentials Provider Service']='12345678-1234-1234-1234-123456789abc'
  value['flowContents']['controllerServices']=[{'identifier':'12345678-1234-1234-1234-123456789abc','type':'AWSService','properties':{'Use Default Credentials':'true'}}]
  result=analyze_graph(value)
  self.assertTrue(result['ok'],result['diagnostics']);self.assertEqual(result['graph']['processors'][0]['properties']['AWS Credentials Provider Service'],'12345678-1234-1234-1234-123456789abc')
  value['flowContents']['controllerServices'][0]['properties'].update({'Access Key ID':'SECRETVALUE1','Secret Access Key':'SECRETVALUE2'})
  result=analyze_graph(value,include_original=True)
  self.assertFalse(result['ok']);self.assertNotIn('SECRETVALUE',json.dumps(result))
 def test_public_credential_setting_exemption_is_exact(self):
  value=document();value['flowContents']['processors'][0]['properties']['AWS Credentials Provider Service']='SECRETVALUE'
  self.assertNotIn('SECRETVALUE',json.dumps(analyze_graph(value,include_original=True)))
 def test_preserves_unknown_fields_in_redacted_original(self):
  value=document();value['customMetadata']={'unknown':'retained'}
  result=analyze_graph(value,include_original=True)
  self.assertEqual(result['original']['customMetadata'],{'unknown':'retained'})
  self.assertEqual(value,result['original'])
  result['original']['customMetadata']['unknown']='changed';self.assertEqual(value['customMetadata']['unknown'],'retained')

 def test_future_version_and_conflicting_declaration_block(self):
  value=document();self.assertFalse(analyze_graph(value,'3')['ok'])
  value['nifiVersion']='1.28.1';result=analyze_graph(value,'2')
  self.assertFalse(result['ok']);self.assertIn('version_conflict',[d['code'] for d in result['diagnostics']])

 def test_group_execution_policies_retained_for_target_guards(self):
  value=document();value['flowContents']['flowFileConcurrency']='SINGLE_BATCH';value['flowContents']['flowFileOutboundPolicy']='BATCH_OUTPUT'
  result=analyze_graph(value)
  self.assertEqual(result['graph']['groups'][0]['flowFileConcurrency'],'SINGLE_BATCH')
  self.assertEqual(result['graph']['groups'][0]['flowFileOutboundPolicy'],'BATCH_OUTPUT')
  self.assertIn('group_execution_mapping_required',[d['code'] for d in result['diagnostics']])
 def test_declared_new_runtime_does_not_upgrade_legacy_bundles(self):
  value=document()
  for p in value['flowContents']['processors']:p['bundle']['version']='1.28.1'
  result=analyze_graph(value,'2')
  self.assertIn('bundle_family_differs',[d['code'] for d in result['diagnostics']])
  self.assertEqual(result['graph']['processors'][0]['bundle']['version'],'1.28.1')
 def test_advanced_rules_preserved_for_airflow_guard(self):
  value=document();value['flowContents']['processors'][1]['annotationData']='<advancedRules />'
  result=analyze_graph(value);self.assertEqual(result['graph']['processors'][1]['annotation_data'],'<advancedRules />')
  self.assertIn('annotation_mapping_required',[d['code'] for d in result['diagnostics']])

 def test_xml_blocker_and_original_tree_never_echoed(self):
  value=document();value['_flowbridge_xml']={'export_blocked':True,'review_required':True,'original_tree':{'arbitrary':'SECRETVALUE'}}
  result=analyze_graph(value,include_original=True)
  self.assertFalse(result['ok']);self.assertNotIn('SECRETVALUE',json.dumps(result));self.assertIn('xml_export_blocked',[d['code'] for d in result['diagnostics']])

 def test_malformed_and_bounded_input_never_crashes(self):
  for value in (None,[],{}, {'flowContents':None}, {'flowContents':{'processors':'bad'}}, {'flowContents':{'processors':[None]}}):
   self.assertFalse(analyze_graph(value)['ok'])
  value={};root=value
  for i in range(60):value['child']={};value=value['child']
  self.assertFalse(analyze_graph(root)['ok'])

 def test_cycle_analysis_handles_long_nonrecursive_graph(self):
  nodes=[processor('node-'+str(i),'UpdateAttribute') for i in range(700)]
  edges=[connection('edge-'+str(i),'node-'+str(i),'node-'+str(i+1)) for i in range(699)]
  result=analyze_graph({'processors':nodes,'connections':edges},'2')
  self.assertTrue(result['ok'],result['diagnostics'][-2:]);self.assertEqual(result['graph']['cycles'],[])

if __name__=='__main__':unittest.main()
