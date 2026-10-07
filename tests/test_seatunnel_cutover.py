import unittest
from flowbridge.targets.seatunnel import export_seatunnel_kafka
from flowbridge.core import Invalid

class SeaTunnelCutoverExportTests(unittest.TestCase):
 def flow(self):return {'schema':'flowbridge/v1','name':'native-cutover','source':{'type':'kafka','brokers':'kafka:9092','topic':'source','group':'blue','offset':'earliest'},'sink':{'type':'kafka','brokers':'kafka:9092','topic':'destination'}}
 def test_explicit_boundary_and_native_engine_config(self):
  result=export_seatunnel_kafka(self.flow(),{1:9,0:4},'green')
  self.assertIn('"source-0" = 4',result['files']['seatunnel.conf']);self.assertIn('"source-1" = 9',result['files']['seatunnel.conf'])
  self.assertIn('format_error_handle_way = "fail"',result['files']['seatunnel.conf']);self.assertFalse(result['native_runtime_verified'])
 def test_no_implicit_or_invalid_checkpoint(self):
  for offsets in ({},{0:-1},{False:0},{0:True},{'0':3}):
   with self.assertRaises(Invalid):export_seatunnel_kafka(self.flow(),offsets,'green')
 def test_configuration_injection_and_feedback_rejected(self):
  with self.assertRaises(Invalid):export_seatunnel_kafka(self.flow(),{0:0},'x" }')
  flow=self.flow();flow['sink']['topic']='source'
  with self.assertRaises(Invalid):export_seatunnel_kafka(flow,{0:0},'green')

class SeaTunnelClientTests(unittest.TestCase):
 def test_inspection_submission_and_stop_remain_distinct(self):
  from flowbridge.targets.seatunnel import SeaTunnelNativeClient
  from flowbridge.live.platforms import PlatformError
  class Client:
   def __init__(self):self.calls=[]
   def request(self,method,path,document=None,query=None):
    self.calls.append((method,path,document,query))
    if path=='/overview':return {'projectVersion':'2.3.13'}
    if path.startswith('/job-info/'):return {'jobId':123,'jobStatus':'RUNNING','secret':'must not echo'}
    return {'jobId':123}
  transport=Client();client=SeaTunnelNativeClient(transport);flow=SeaTunnelCutoverExportTests().flow()
  with self.assertRaises(PlatformError):client.submit(flow,{0:4},'green')
  self.assertEqual(transport.calls,[])
  result=client.submit(flow,{0:4},'green',allow_apply=True)
  self.assertTrue(result['submitted']);self.assertFalse(result['running_verified'])
  self.assertEqual(client.inspect(123)['status'],'RUNNING')
  self.assertNotIn('secret',client.inspect(123))
  with self.assertRaises(PlatformError):client.stop(123)
  self.assertFalse(client.stop(123,allow_apply=True)['stopped_verified'])
 def test_bad_ids_and_versions_fail_closed(self):
  from flowbridge.targets.seatunnel import SeaTunnelNativeClient
  from flowbridge.live.platforms import PlatformError
  class Client:
   def request(self,*args,**kwargs):return {'projectVersion':'3.0.0'}
  client=SeaTunnelNativeClient(Client())
  for value in (True,'../other',0,2**64):
   with self.assertRaises(PlatformError):client.inspect(value)
  with self.assertRaises(PlatformError):client.submit(SeaTunnelCutoverExportTests().flow(),{0:0},'green',True)

class ModernNiFiProfileTests(unittest.TestCase):
 def fixture(self):
  import json
  from pathlib import Path
  return json.loads((Path(__file__).parent/'integration/seatunnel_cutover/native-export.json').read_text())
 def test_actual_fixture_can_be_prepared_without_manufacturing_offsets(self):
  from flowbridge.targets.seatunnel import flow_from_nifi,export_from_nifi
  self.assertEqual(flow_from_nifi(self.fixture())['source']['topic'],'source_topic')
  result=export_from_nifi(self.fixture(),{0:6},'green')
  self.assertTrue(result['source_fence_required']);self.assertTrue(result['data_contract']['keys_must_be_null'])
 def test_unknown_processor_transform_metadata_security_and_transactions_block(self):
  from flowbridge.targets.seatunnel import flow_from_nifi
  for field,value in [('Transactions Enabled','true'),('Kafka Key','${k}'),('FlowFile Attribute Header Pattern','.*'),('Message Demarcator','\\n'),('unknown-property','x')]:
   doc=self.fixture();processor=next(p for p in doc['flowContents']['processors'] if p['type'].endswith('PublishKafka'));processor['properties'][field]=value
   with self.assertRaises(Invalid):flow_from_nifi(doc)
  doc=self.fixture();doc['flowContents']['controllerServices'][0]['properties']['security.protocol']='SASL_SSL'
  with self.assertRaises(Invalid):flow_from_nifi(doc)
 def test_variable_parameter_references_block(self):
  from flowbridge.targets.seatunnel import flow_from_nifi
  for field in ('Topics','Group ID'):
   for value in ('${parameter}','#{parameter}'):
    doc=self.fixture();processor=next(p for p in doc['flowContents']['processors'] if p['type'].endswith('ConsumeKafka'));processor['properties'][field]=value
    with self.assertRaises(Invalid):flow_from_nifi(doc)
  doc=self.fixture();doc['flowContents']['controllerServices'][0]['properties']['bootstrap.servers']='${broker}:9092'
  with self.assertRaises(Invalid):flow_from_nifi(doc)
