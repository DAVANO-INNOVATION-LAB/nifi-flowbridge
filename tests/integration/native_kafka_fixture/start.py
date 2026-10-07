"""Start shared synthetic fixture; explicit stop.py cleans it after target proof."""
import io,json,secrets,subprocess,tarfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]
OUT=ROOT/'local-output/native-kafka-fixture';OUT.mkdir(parents=True,exist_ok=True)
D=['docker','--config',str(ROOT.parent/'local-workers/state/docker'),'--context','colima-dil']
def call(args,**kw):return subprocess.run(D+args,cwd=ROOT,check=True,timeout=kw.pop('timeout',250),**kw)
env=OUT/'test.env';env.write_text('CONTINUOUS_TEST_PASSWORD='+secrets.token_urlsafe(36)+'\n');env.chmod(0o600)
compose=['compose','--env-file',str(env),'-f','tests/integration/native_kafka_fixture/compose.yaml']
assert not call(compose+['ps','-q'],capture_output=True,text=True).stdout.strip(),'fixture already exists'
call(compose+['up','-d','--pull','never'])
call(['run','-d','--name','flowbridge-native-kafka-helper','--network','container:flowbridge-native-kafka-nifi-1','--memory','256m','--cpus','1','--cap-drop','ALL','--security-opt','no-new-privileges','--env-file',str(env),'--entrypoint','sleep','flowbridge-media-smoke:local','1800'],capture_output=True)
data=io.BytesIO()
with tarfile.open(fileobj=data,mode='w') as tar:
 tar.add(ROOT/'tests/integration/continuous/api.py',arcname='api.py')
 tar.add(ROOT/'tests/integration/native_kafka_fixture/setup.py',arcname='setup.py')
 tar.add(ROOT/'tests/integration/native_kafka_fixture/run_source.py',arcname='run_source.py')
call(['exec','-i','flowbridge-native-kafka-helper','tar','--no-same-owner','-xf','-','-C','/tmp'],input=data.getvalue())
result=call(['exec','flowbridge-native-kafka-helper','python','/tmp/setup.py'],capture_output=True)
(OUT/'setup.json').write_bytes(result.stdout)
call(['exec','-i','flowbridge-native-kafka-helper','python','-c',"import sys;open('/tmp/setup.json','w').write(sys.stdin.read())"],input=result.stdout)
doc=json.loads(result.stdout)['document'];(OUT/'native-export.json').write_text(json.dumps(doc,indent=2)+'\n')
print('Native Kafka fixture configured; setup and native export saved locally. Services remain isolated for target proof.')
