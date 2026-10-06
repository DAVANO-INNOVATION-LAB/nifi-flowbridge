import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent))
from test_live_kafka import FakeKafka, record
from flowbridge.live.jobs import JobManager, MigrationError


class ClosableKafka(FakeKafka):
 def close(self):pass


class LiveJobTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory()
  self.source=ClosableKafka('source-cluster','source',[record(0),record(1)])
  self.target=ClosableKafka('target-cluster','target',[record(i,topic='target') for i in range(5)])
  self.factory=lambda settings:self.source if settings['brokers']=='source:9092' else self.target
  self.manager=JobManager(self.tmp.name,self.factory)
  self.request={'source':{'brokers':'source:9092','topic':'source','security_protocol':'SASL_SSL','username':'source-user','password':'SOURCE_SECRET_MUST_NOT_PERSIST'},'target':{'brokers':'target:9092','topic':'target','security_protocol':'SASL_SSL','username':'target-user','password':'TARGET_SECRET_MUST_NOT_PERSIST'},'groups':[]}

 def tearDown(self):
  for control in list(self.manager.running.values()):control['cancel'].set()
  deadline=time.monotonic()+5
  while self.manager.running and time.monotonic()<deadline:time.sleep(.02)
  self.tmp.cleanup()

 def start(self):
  plan=self.manager.assess(self.request)
  started=self.manager.start(plan['plan_id'],True)
  return plan,started['job_id']

 def wait(self,ident,states,timeout=6):
  deadline=time.monotonic()+timeout
  while time.monotonic()<deadline:
   status=self.manager.status(ident)
   if status['state'] in states:return status
   if status['state']=='failed':self.fail('Unexpected job failure: '+str(status))
   time.sleep(.02)
  self.fail('Job did not reach '+str(states)+': '+str(self.manager.status(ident)))

 def cutover_request(self,ident):
  return {'job_id':ident,'acknowledge':True,'producers_stopped':True,'consumers_stopped':True}

 def test_snapshot_and_continuous_mirroring(self):
  _,ident=self.start();first=self.wait(ident,{'mirroring'})
  self.assertEqual(first['copied'],2);self.assertEqual(first['lag'],0)
  self.source.data[0].append(record(2))
  deadline=time.monotonic()+4
  while self.manager.status(ident)['copied']<3 and time.monotonic()<deadline:time.sleep(.02)
  self.assertEqual(self.manager.status(ident)['copied'],3)
  self.assertEqual(len(self.target.data[0]),8);self.assertEqual(self.source.altered,[])

 def test_start_is_idempotent_and_credentials_never_persist(self):
  plan,ident=self.start();self.wait(ident,{'mirroring'})
  second=self.manager.start(plan['plan_id'],True)
  self.assertEqual(second['job_id'],ident)
  with self.manager.db() as db:
   self.assertEqual(db.execute('SELECT count(*) FROM jobs').fetchone()[0],1)
   persisted=json.dumps([dict(row) for row in db.execute('SELECT * FROM jobs')])
  self.assertNotIn('SOURCE_SECRET',persisted);self.assertNotIn('TARGET_SECRET',persisted)
  self.assertNotIn('SOURCE_SECRET',json.dumps(plan));self.assertNotIn('TARGET_SECRET',json.dumps(plan))
  for path in Path(self.tmp.name).iterdir():
   if path.is_file():
    self.assertNotIn(b'SOURCE_SECRET',path.read_bytes());self.assertNotIn(b'TARGET_SECRET',path.read_bytes())
  self.assertNotIn(plan['plan_id'],self.manager.plans)

 def test_copy_requires_acknowledgement(self):
  plan=self.manager.assess(self.request)
  with self.assertRaises(MigrationError):self.manager.start(plan['plan_id'],False)
  self.assertEqual(self.target.deliveries,[])

 def test_cancel_stops_mirroring_and_retains_delivered_records(self):
  _,ident=self.start();self.wait(ident,{'mirroring'})
  self.manager.cancel(ident);self.wait(ident,{'cancelled'})
  before=len(self.target.data[0]);self.source.data[0].append(record(2));time.sleep(.1)
  self.assertEqual(len(self.target.data[0]),before);self.assertEqual(self.source.altered,[])

 def test_cutover_requires_all_freeze_acknowledgements(self):
  _,ident=self.start();self.wait(ident,{'mirroring'})
  for key in ('acknowledge','producers_stopped','consumers_stopped'):
   request=self.cutover_request(ident);request[key]=False
   with self.assertRaises(MigrationError):self.manager.cutover(request)
  self.assertEqual(self.target.altered,[])

 def test_cutover_translates_offsets_and_never_changes_source(self):
  self.request['groups']=['orders'];self.source.group_data['orders']={0:1}
  _,ident=self.start();self.wait(ident,{'mirroring'})
  self.manager.cutover(self.cutover_request(ident))
  final=self.wait(ident,{'completed'},8)
  self.assertEqual(self.target.group_data['orders'],{0:6});self.assertEqual(self.source.group_data['orders'],{0:1})
  self.assertFalse(final['result']['source_offsets_modified']);self.assertEqual(self.source.altered,[])

 def test_empty_group_cutover_validates_data_only(self):
  _,ident=self.start();self.wait(ident,{'mirroring'})
  self.manager.cutover(self.cutover_request(ident));self.wait(ident,{'completed'},8)
  self.assertEqual(self.target.altered,[])

 def test_writes_during_freeze_window_refuse_cutover(self):
  _,ident=self.start();self.wait(ident,{'mirroring'})
  self.manager.cutover(self.cutover_request(ident));self.wait(ident,{'cutting_over'})
  self.source.data[0].append(record(2))
  final=self.wait(ident,{'failed'},8)
  self.assertIsNotNone(final['error']);self.assertEqual(self.target.altered,[])

 def test_only_one_running_transfer(self):
  _,ident=self.start();self.wait(ident,{'mirroring'})
  second=self.manager.assess(self.request)
  with self.assertRaises(MigrationError):self.manager.start(second['plan_id'],True)

if __name__=='__main__':unittest.main()
