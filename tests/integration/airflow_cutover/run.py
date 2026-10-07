"""Isolated actual NiFi source and actual Airflow dag.test handoff proof."""
import argparse,io,json,os,secrets,subprocess,tarfile,tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]
def main():
 p=argparse.ArgumentParser();p.add_argument('--docker-config');p.add_argument('--context');a=p.parse_args()
 docker=['docker']
 if a.docker_config:docker+=['--config',a.docker_config]
 if a.context:docker+=['--context',a.context]
 def call(args,**kw):return subprocess.run(docker+args,cwd=ROOT,check=True,timeout=kw.pop('timeout',300),**kw)
 with tempfile.TemporaryDirectory() as temporary:
  env=Path(temporary)/'test.env';env.write_text('CONTINUOUS_TEST_PASSWORD='+secrets.token_urlsafe(36)+'\n');env.chmod(0o600)
  compose=['compose','--env-file',str(env),'-p','flowbridge-airflow-cutover','-f','tests/integration/continuous/compose.yaml']
  name='flowbridge-airflow-cutover-runner'
  if call(compose+['ps','-q'],capture_output=True,text=True).stdout.strip():raise RuntimeError('Test project already active')
  try:
   call(compose+['up','-d','--pull','never'])
   call(['run','-d','--name',name,'--network','container:flowbridge-airflow-cutover-nifi-1','--memory','1200m','--cpus','2','--cap-drop','ALL','--security-opt','no-new-privileges','--env-file',str(env),'-e','AIRFLOW_HOME=/tmp/airflow','-e','AIRFLOW__CORE__LOAD_EXAMPLES=False','-e','PYTHONPATH=/tmp/app:/tmp/continuous','-e','AWS_ACCESS_KEY_ID=synthetic-only','-e','AWS_SECRET_ACCESS_KEY=synthetic-only','-e','AWS_DEFAULT_REGION=us-east-1','--entrypoint','sleep','apache/airflow:3.1.8-python3.12','900'],capture_output=True)
   data=io.BytesIO()
   with tarfile.open(fileobj=data,mode='w') as archive:
    for source,destination in [('flowbridge','app/flowbridge'),('LICENSE','app/LICENSE'),('tests/integration/continuous','continuous'),('tests/integration/media/fixtures','fixtures'),('tests/integration/airflow_cutover','proof')]:archive.add(ROOT/source,arcname=destination,filter=lambda x:None if '__pycache__' in x.name else x)
   call(['exec','-i',name,'tar','--no-same-owner','-xf','-','-C','/tmp'],input=data.getvalue())
   call(['exec',name,'airflow','db','migrate'],capture_output=True)
   setup=call(['exec',name,'python','/tmp/continuous/setup.py'],capture_output=True,timeout=220).stdout
   output=ROOT/'local-output/airflow-cutover';output.mkdir(parents=True,exist_ok=True)
   with (output/'events.jsonl').open('wb') as log:call(['exec','-i',name,'python','/tmp/proof/smoke.py'],input=setup,stdout=log,timeout=520)
   rows=[json.loads(line) for line in (output/'events.jsonl').read_text().splitlines() if line.startswith('{')]
   proof=next(row['proof'] for row in rows if 'proof' in row);assert proof['result']=='passed'
   proof['limitations']=['Airflow dag.test executes real DagRun and task lifecycle locally; production scheduler and executor deployment were not exercised.','One task per mapped S3 lane invokes the packaged Flowbridge worker, not individual translated NiFi operators.','Scheduled microbatches replace continuous NiFi scheduling; no queue/state migration or zero-downtime claim.','Isolated synthetic S3Mock endpoints; production TLS/IAM and scale are not certified.']
   (ROOT/'docs/airflow-cutover-evidence.json').write_text(json.dumps(proof,indent=2)+'\n')
   print(json.dumps({'result':'passed','total':proof['total_produced'],'runs':proof['airflow_runs']}))
  finally:
   subprocess.run(docker+['rm','-f',name],capture_output=True)
   call(compose+['down','--volumes'],timeout=90)
if __name__=='__main__':main()
