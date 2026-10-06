import unittest
import json
from flowbridge.live.kafka import Record, KafkaBridgeError, endpoint_config, preflight, copy_snapshot, tail_step, translate_group_offsets, apply_group_offsets

class FakeKafka:
 def __init__(self,cluster,topic,records=None):
  self.cluster,self.topic=cluster,topic;self.data={0:list(records or [])};self.low={0:0};self.group_data={};self.states={};self.altered=[];self.deliveries=[];self.policy='delete';self.timestamp='CreateTime';self.fail_delivery=False
 def metadata(self,topic):
  if topic!=self.topic:raise KafkaBridgeError('topic_missing','Topic missing')
  return {'cluster_id':self.cluster,'partitions':{p:{'low':self.low.get(p,0),'high':max([r.offset+1 for r in rows],default=self.low.get(p,0))} for p,rows in self.data.items()},'cleanup_policy':self.policy,'timestamp_type':self.timestamp}
 def records(self,topic,partition,start,stop,timeout,cancelled):
  for record in self.data[partition]:
   if cancelled():return
   if start<=record.offset<stop:yield record
 def deliver(self,record,topic,partition,timeout):
  if self.fail_delivery:raise KafkaBridgeError('delivery_failed','Delivery failed')
  offset=max([r.offset+1 for r in self.data[partition]],default=self.low.get(partition,0))
  self.data[partition].append(Record(topic,partition,offset,record.key,record.value,list(record.headers),record.timestamp));self.deliveries.append((record,offset));return offset
 def group_state(self,group):return self.states.get(group,{'state':'EMPTY' if group in self.group_data else 'ABSENT','members':0})
 def group_offsets(self,group,topic):return dict(self.group_data.get(group,{}))
 def alter_group_offsets(self,group,topic,offsets):self.altered.append((group,topic,dict(offsets)));self.group_data[group]=dict(offsets)

def record(offset,value=b'payload',partition=0,topic='source'):
 return Record(topic,partition,offset,b'key',value,[('duplicate',b'one'),('duplicate',None)],1700000000123)

class LiveKafkaTests(unittest.TestCase):
 def setUp(self):
  self.source=FakeKafka('source-cluster','source',[record(0),record(1,None),record(2)])
  self.target=FakeKafka('target-cluster','target',[record(i,topic='target') for i in range(7)])
  self.plan=preflight(self.source,self.target,'source','target');self.mappings={}
 def save(self,m):self.mappings[(m['source_topic'],m['source_partition'],m['source_offset'])]=m
 def lookup(self,topic,partition,offset):return self.mappings.get((topic,partition,offset))
 def copy_all(self):return copy_snapshot(self.source,self.target,self.plan,on_delivery=self.save)
 def test_copy_preserves_bytes_tombstones_headers_timestamp(self):
  result=self.copy_all();self.assertTrue(result['complete']);self.assertEqual(result['positions'],{0:3})
  for before,after in zip(self.source.data[0],self.target.data[0][7:]):self.assertEqual((before.key,before.value,before.headers,before.timestamp),(after.key,after.value,after.headers,after.timestamp))
  self.assertEqual(self.lookup('source',0,0)['target_offset'],7);self.assertEqual(self.source.altered,[])
 def test_bounded_step_resumes(self):
  first=copy_snapshot(self.source,self.target,self.plan,on_delivery=self.save,max_records=1)
  self.assertFalse(first['complete']);self.assertEqual(first['positions'],{0:1})
  second=copy_snapshot(self.source,self.target,self.plan,positions=first['positions'],on_delivery=self.save)
  self.assertTrue(second['complete']);self.assertEqual(len(self.target.deliveries),3)
 def test_tail_refreshes_source_preserves_target_baseline(self):
  first=self.copy_all();self.source.data[0].append(record(3))
  tail=tail_step(self.source,self.target,self.plan,positions=first['positions'],on_delivery=self.save)
  self.assertTrue(tail['complete']);self.assertEqual(tail['plan']['partitions'][0]['source_high'],4);self.assertEqual(tail['plan']['partitions'][0]['target_high'],7)
 def test_translate_and_apply_use_delivery_mapping(self):
  self.copy_all();self.source.group_data['orders']={0:1};self.target.group_data['orders']={0:2}
  preview=translate_group_offsets(self.source,self.target,self.plan,['orders'],self.lookup)
  self.assertEqual(preview['groups'][0]['target_offsets'],{0:8});self.assertEqual(preview['groups'][0]['target_previous_offsets'],{0:2})
  self.assertEqual(apply_group_offsets(self.source,self.target,preview)['applied_groups'],['orders'])
  self.assertEqual(self.target.group_data['orders'],{0:8});self.assertEqual(self.source.group_data['orders'],{0:1});self.assertEqual(self.source.altered,[])
 def test_end_boundary_translation(self):
  self.copy_all();self.source.group_data['orders']={0:3}
  preview=translate_group_offsets(self.source,self.target,self.plan,['orders'],self.lookup);self.assertEqual(preview['groups'][0]['target_offsets'],{0:10})
 def test_missing_mapping_blocks_cutover(self):
  self.copy_all();self.source.group_data['orders']={0:3};del self.mappings[('source',0,1)]
  with self.assertRaisesRegex(KafkaBridgeError,'lacks a verified'):translate_group_offsets(self.source,self.target,self.plan,['orders'],self.lookup)
  self.assertEqual(self.target.altered,[])
 def test_active_source_or_target_group_blocks(self):
  self.copy_all();self.source.group_data['orders']={0:3}
  for adapter in (self.source,self.target):
   adapter.states['orders']={'state':'STABLE','members':1}
   with self.assertRaisesRegex(KafkaBridgeError,'active'):translate_group_offsets(self.source,self.target,self.plan,['orders'],self.lookup)
   adapter.states.clear()
 def test_source_or_target_external_writes_block(self):
  self.copy_all();self.source.group_data['orders']={0:3};self.source.data[0].append(record(3))
  with self.assertRaisesRegex(KafkaBridgeError,'high watermarks changed'):translate_group_offsets(self.source,self.target,self.plan,['orders'],self.lookup)
  self.source.data[0].pop();self.target.data[0].append(record(10,topic='target'))
  with self.assertRaisesRegex(KafkaBridgeError,'destination range changed'):translate_group_offsets(self.source,self.target,self.plan,['orders'],self.lookup)
 def test_source_gap_never_skipped(self):
  self.source.data[0].pop(1)
  with self.assertRaisesRegex(KafkaBridgeError,'gap'):self.copy_all()
  self.assertEqual(len(self.target.deliveries),1);self.assertIsNone(self.lookup('source',0,2))
 def test_delivery_failure_never_checkpoints(self):
  self.target.fail_delivery=True
  with self.assertRaises(KafkaBridgeError):self.copy_all()
  self.assertEqual(self.mappings,{})
 def test_checkpoint_failure_exposes_duplicate_risk_not_secret(self):
  def bad(mapping):raise OSError('SECRET not writable')
  with self.assertRaisesRegex(KafkaBridgeError,'duplicate') as raised:copy_snapshot(self.source,self.target,self.plan,on_delivery=bad)
  self.assertNotIn('SECRET',str(raised.exception));self.assertEqual(len(self.target.deliveries),1)
 def test_cancelled_copy_does_not_write(self):
  result=copy_snapshot(self.source,self.target,self.plan,on_delivery=self.save,cancelled=lambda:True)
  self.assertTrue(result['cancelled']);self.assertEqual(self.target.deliveries,[])
 def test_record_limit_before_write(self):
  self.source.data[0][0]=record(0,b'x'*(1024*1024+1))
  with self.assertRaisesRegex(KafkaBridgeError,'one-MiB'):self.copy_all()
  self.assertEqual(self.target.deliveries,[])
 def test_partition_mapping_and_feedback(self):
  with self.assertRaisesRegex(KafkaBridgeError,'same topic'):preflight(self.source,self.source,'source','source')
  self.target.data[1]=[]
  with self.assertRaisesRegex(KafkaBridgeError,'partition'):preflight(self.source,self.target,'source','target')
 def test_compaction_and_timestamp_policy(self):
  self.target.policy='compact'
  with self.assertRaisesRegex(KafkaBridgeError,'Compacted'):preflight(self.source,self.target,'source','target')
  self.target.policy='delete';self.target.timestamp='LogAppendTime'
  with self.assertRaisesRegex(KafkaBridgeError,'CreateTime'):preflight(self.source,self.target,'source','target')
 def test_group_movement_after_preview(self):
  self.copy_all();self.source.group_data['orders']={0:1}
  preview=translate_group_offsets(self.source,self.target,self.plan,['orders'],self.lookup);self.source.group_data['orders']={0:2}
  with self.assertRaisesRegex(KafkaBridgeError,'changed'):apply_group_offsets(self.source,self.target,preview)
  self.assertEqual(self.target.altered,[])
 def test_empty_group_list_validates_data_without_offsets(self):
  self.copy_all()
  preview=translate_group_offsets(self.source,self.target,self.plan,[],self.lookup)
  self.assertEqual(preview['groups'],[])
  self.assertEqual(apply_group_offsets(self.source,self.target,preview)['applied_groups'],[])
  del self.mappings[('source',0,1)]
  with self.assertRaises(KafkaBridgeError):translate_group_offsets(self.source,self.target,self.plan,[],self.lookup)
 def test_apply_preview_survives_json_storage(self):
  self.copy_all();self.source.group_data['orders']={0:1}
  preview=json.loads(json.dumps(translate_group_offsets(self.source,self.target,self.plan,['orders'],self.lookup)))
  apply_group_offsets(self.source,self.target,preview)
  self.assertEqual(self.target.group_data['orders'],{0:8})
 def test_empty_source_partition_translates_to_target_baseline(self):
  self.source.data[0]=[];self.source.group_data['orders']={0:0}
  plan=preflight(self.source,self.target,'source','target')
  preview=translate_group_offsets(self.source,self.target,plan,['orders'],self.lookup)
  self.assertEqual(preview['groups'][0]['target_offsets'],{0:7})
 def test_nonidentity_partition_mapping(self):
  self.source.data[1]=[record(0,partition=1)]
  self.target.data[1]=[]
  plan=preflight(self.source,self.target,'source','target',{0:1,1:0})
  result=copy_snapshot(self.source,self.target,plan,on_delivery=self.save)
  self.assertTrue(result['complete']);self.assertEqual(self.lookup('source',0,0)['target_partition'],1)
  self.source.group_data['orders']={0:1,1:1}
  preview=translate_group_offsets(self.source,self.target,plan,['orders'],self.lookup)
  self.assertEqual(preview['groups'][0]['target_offsets'],{1:1,0:8})
 def test_destination_duplicate_gap_blocks_cutover(self):
  self.target.deliver(record(0),'target',0,1)
  self.copy_all();self.source.group_data['orders']={0:3}
  with self.assertRaisesRegex(KafkaBridgeError,'duplicates or foreign'):translate_group_offsets(self.source,self.target,self.plan,['orders'],self.lookup)
 def test_endpoint_security_and_no_extra_settings(self):
  cfg=endpoint_config({'brokers':'broker.example:9093'});self.assertEqual(cfg['security.protocol'],'SSL');self.assertTrue(cfg['enable.ssl.certificate.verification'])
  sasl=endpoint_config({'brokers':'broker:9093','security_protocol':'SASL_SSL','username':'user','password':'private'});self.assertEqual(sasl['sasl.mechanism'],'SCRAM-SHA-512')
  for bad in ({'brokers':'broker:9092','security_protocol':'PLAINTEXT','password':'private'},{'brokers':'broker:9093','ssl.ca.location':'/etc/passwd'},{'brokers':'user:private@broker:9093'}):
   with self.assertRaises(KafkaBridgeError) as raised:endpoint_config(bad)
   self.assertNotIn('private',str(raised.exception))

if __name__=='__main__':unittest.main()
