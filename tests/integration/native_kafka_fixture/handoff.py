"""Begin bounded arrivals; return only after actual native drain proof exists."""
import json,subprocess,time
from pathlib import Path
root=Path(__file__).resolve().parents[3];out=root/'local-output/native-kafka-fixture'
d=['docker','--config',str(root.parent/'local-workers/state/docker'),'--context','colima-dil']
subprocess.run(d+['exec','-d','flowbridge-native-kafka-helper','python','/tmp/run_source.py'],check=True)
deadline=time.monotonic()+100
while True:
 result=subprocess.run(d+['exec','flowbridge-native-kafka-helper','cat','/tmp/handoff.json'],capture_output=True)
 if result.returncode==0:
  evidence=json.loads(result.stdout);assert evidence['queued_flowfiles']==0 and evidence['active_threads']==0
  (out/'handoff.json').write_bytes(result.stdout)
  native=subprocess.check_output(d+['exec','flowbridge-native-kafka-helper','cat','/tmp/native-export.json'])
  (out/'native-export.json').write_bytes(native)
  print(json.dumps(evidence));break
 if time.monotonic()>deadline:raise RuntimeError('Native handoff proof not ready; inspect helper and clean up fixture')
 time.sleep(1)
