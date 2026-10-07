"""Launch a synthetic native target after the separate source fencing step.

Input JSON: {flow: canonical-flow, start_offsets: {partition: next-offset},
consumer_group: string}. Caller supplies the private test Docker network.
This helper is not the application's production deployment adapter.
"""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile
from flowbridge.targets.seatunnel import export_seatunnel_kafka


def main():
    p=argparse.ArgumentParser();p.add_argument('checkpoint');p.add_argument('--network',required=True);p.add_argument('--name',default='flowbridge-seatunnel-native-proof');p.add_argument('--docker-config');p.add_argument('--context');a=p.parse_args()
    if not a.network.startswith('flowbridge-') or not a.name.startswith('flowbridge-'):
        raise ValueError('Only dedicated Flowbridge test resources are allowed')
    record=json.loads(Path(a.checkpoint).read_text())
    artifact=export_seatunnel_kafka(record['flow'],{int(k):v for k,v in record['start_offsets'].items()},record['consumer_group'])
    docker=['docker']
    if a.docker_config:docker+=['--config',a.docker_config]
    if a.context:docker+=['--context',a.context]
    def run(args):return subprocess.run(docker+args,check=True,capture_output=True,text=True,timeout=60)
    created=False
    try:
        run(['create','--name',a.name,'--network',a.network,'--memory','1g','--cpus','1','--cap-drop','ALL','--security-opt','no-new-privileges','apache/seatunnel:2.3.13','bash','./bin/seatunnel.sh','-m','local','-c','/tmp/seatunnel.conf'])
        created=True
        with tempfile.TemporaryDirectory(prefix='seatunnel-proof-') as folder:
            config=Path(folder)/'seatunnel.conf';config.write_text(artifact['files']['seatunnel.conf'])
            run(['cp',str(config),a.name+':/tmp/seatunnel.conf'])
        run(['start',a.name])
        print(json.dumps({'container':a.name,'submitted':True,'verified':False}))
    except Exception:
        if created:run(['rm','-f',a.name])
        raise

if __name__=='__main__':main()
