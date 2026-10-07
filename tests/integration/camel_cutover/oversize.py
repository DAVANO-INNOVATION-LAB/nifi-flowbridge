import json,sys
from pathlib import Path
sys.path.insert(0,'/work/tests/integration/continuous')
from setup import client
configuration=json.loads(Path('/tmp/output/setup.json').read_text());lane=configuration['lanes'][0];source=client('source-s3');target=client('target-s3');key='incoming/000-oversize'
if '--create' in sys.argv:
 with open('/tmp/oversize.bin','wb') as stream:stream.truncate(67108865)
 with open('/tmp/oversize.bin','rb') as stream:source.put_object(Bucket=lane['source'],Key=key,Body=stream,ContentLength=67108865,ContentType='application/octet-stream')
 print(json.dumps({'created_bytes':67108865,'key':key}))
elif '--remove' in sys.argv:
 source.delete_object(Bucket=lane['source'],Key=key);Path('/tmp/oversize.bin').unlink(missing_ok=True)
else:
 assert source.head_object(Bucket=lane['source'],Key=key)['ContentLength']==67108865
 try:target.head_object(Bucket=lane['destination'],Key=key)
 except target.exceptions.ClientError as error:assert error.response['ResponseMetadata']['HTTPStatusCode']==404
 else:raise AssertionError('Oversized object must not be copied')
 print(json.dumps({'oversized_source_retained':True,'oversized_target_absent':True}))
