import json,time,sys
from pathlib import Path
sys.path.insert(0,'/work/tests/integration/continuous')
from setup import client
out=Path('/tmp/output');configuration=json.loads((out/'setup.json').read_text());source=client('source-s3');index=0
while not (out/'stop-producer').exists() and index<1200:
 for lane in configuration['lanes']:
  key='incoming/during-'+str(index);value=(lane['kind']+' continuous '+str(index)).encode()
  source.put_object(Bucket=lane['source'],Key=key,Body=value,ContentType='application/octet-stream')
 index+=1;(out/'producer-status.json').write_text(json.dumps({'batches':index,'objects':index*3,'last_arrival':time.time()}));time.sleep(2)
print(json.dumps({'batches':index,'objects':index*3}))
