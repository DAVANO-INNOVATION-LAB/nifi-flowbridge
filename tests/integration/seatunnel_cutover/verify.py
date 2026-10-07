import collections
import hashlib
import json
import time
from confluent_kafka import Consumer, TopicPartition
from flowbridge.live.platforms import JsonClient
from flowbridge.targets.seatunnel import SeaTunnelNativeClient


def read_topic(topic):
 c=Consumer({'bootstrap.servers':'kafka:9092','group.id':'seatunnel-proof-audit-'+topic,'enable.auto.commit':False})
 try:
  low,high=c.get_watermark_offsets(TopicPartition(topic,0),timeout=10)
  c.assign([TopicPartition(topic,0,low)]);rows=[];end=time.monotonic()+30
  while len(rows)<high-low and time.monotonic()<end:
   m=c.poll(1)
   if m is None:continue
   if m.error():raise RuntimeError('Kafka audit read failed')
   rows.append({'offset':m.offset(),'value':m.value().decode(),'key':m.key(),'headers':m.headers()})
  assert len(rows)==high-low
  return rows
 finally:c.close()

def main():
 source=read_topic('source_topic');target=read_topic('target_topic')
 # Producer may still append; test this observed source boundary against target.
 expected=[r['value'] for r in source]
 end=time.monotonic()+30
 while not set(expected)<=set(r['value'] for r in target) and time.monotonic()<end:
  time.sleep(.5);target=read_topic('target_topic')
 actual=[r['value'] for r in target]
 assert set(expected)<=set(actual)
 assert all(r['key'] is None and not r['headers'] for r in source)
 print(json.dumps({'source_count':len(source),'target_count':len(target),'boundary_all_present':True,'source_values':expected,'target_values':actual,'duplicates':sum(v-1 for v in collections.Counter(actual).values()),'source_sha256':hashlib.sha256('\n'.join(expected).encode()).hexdigest(),'target_sha256':hashlib.sha256('\n'.join(actual).encode()).hexdigest()}))
if __name__=='__main__':main()
