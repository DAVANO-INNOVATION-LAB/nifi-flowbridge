"""Offline-capable real S3 fleet proof orchestration; no image pulls."""
import argparse
import json
from pathlib import Path
import subprocess
ROOT=Path(__file__).resolve().parents[3]
def main():
 parser=argparse.ArgumentParser();parser.add_argument('--docker-config');parser.add_argument('--context');parser.add_argument('--output',default='local-output/fleet-proof.json');args=parser.parse_args()
 docker=['docker']
 if args.docker_config:docker+=['--config',args.docker_config]
 if args.context:docker+=['--context',args.context]
 compose=['compose','-f','tests/integration/fleet/compose.yaml']
 def run(command,**kw):return subprocess.run(docker+command,cwd=ROOT,check=True,timeout=kw.pop('timeout',300),**kw)
 if run(compose+['ps','-q'],capture_output=True,text=True).stdout.strip():raise RuntimeError('Existing fleet proof containers: clean up this test project before rerunning')
 output=ROOT/args.output;output.parent.mkdir(parents=True,exist_ok=True)
 try:
  run(['build','--network=none','--pull=false','-f','tests/integration/fleet/Dockerfile','-t','flowbridge-fleet-smoke:local','.'])
  run(compose+['up','-d','--pull','never'])
  with output.open('wb') as handle:
   run(['run','--rm','--network','flowbridge-fleet-proof_default','--memory','384m','--cpus','1','--cap-drop','ALL','--security-opt','no-new-privileges','flowbridge-fleet-smoke:local'],stdout=handle,timeout=270)
  evidence=json.loads(output.read_text());assert evidence['result']=='passed'
  print(json.dumps({'result':'passed','pipelines':evidence['pipelines'],'objects':evidence['total_produced']}))
 finally:run(compose+['down','--volumes'],timeout=90)
if __name__=='__main__':main()
