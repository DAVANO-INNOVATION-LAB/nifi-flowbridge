"""Read-only reconciliation of native Camel output, not an executor."""
import hashlib,json,sys
from pathlib import Path
sys.path.insert(0,'/work/tests/integration/continuous')
from setup import client
state=Path('/tmp/output');config=json.loads((state/'setup.json').read_text());source=client('source-s3');target=client('target-s3')
def keys(c,bucket):
 return [row['Key'] for page in c.get_paginator('list_objects_v2').paginate(Bucket=bucket) for row in page.get('Contents',[])]
def versions(bucket):
 return sum(len(page.get('Versions',[])) for page in target.get_paginator('list_object_versions').paginate(Bucket=bucket))
rows=[]
for lane in config['lanes']:
 a=keys(source,lane['source']);b=keys(target,lane['destination']);row={'media_type':lane['kind'],'source_objects':len(a),'target_objects':len(b),'target_versions':versions(lane['destination'])}
 if '--check' in sys.argv:
  assert set(a)==set(b),(lane['kind'],len(a),len(b));hashes=[]
  for key in sorted(a):
   left=source.get_object(Bucket=lane['source'],Key=key);right=target.get_object(Bucket=lane['destination'],Key=key)
   av=left['Body'].read();bv=right['Body'].read();left['Body'].close();right['Body'].close()
   assert av==bv,key
   assert left['ContentType']==right['ContentType'],key
   assert right['Metadata'].get('media_type')==lane['kind'],key
   hashes.append([key,hashlib.sha256(av).hexdigest()])
  row.update({'matched':len(a),'checksum_set_sha256':hashlib.sha256(json.dumps(hashes).encode()).hexdigest(),'content_types_equal':True,'media_type_metadata_equal':True})
 rows.append(row)
result={'lanes':rows,'objects':sum(r['source_objects'] for r in rows),'target_objects':sum(r['target_objects'] for r in rows),'target_versions':sum(r['target_versions'] for r in rows)}
if '--check' in sys.argv:(state/'verification.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result))
