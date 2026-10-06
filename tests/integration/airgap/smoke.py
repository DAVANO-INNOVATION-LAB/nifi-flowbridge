"""Offline deployment proof, not physical airgap or production certification."""
import collections
import copy
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid

import boto3
from botocore.config import Config
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic


def s3_client(config):
    return boto3.client('s3',endpoint_url=config['endpoint'],region_name=config['region'],
        aws_access_key_id='isolated-fixture',aws_secret_access_key='isolated-fixture',
        config=Config(s3={'addressing_style':'path'},connect_timeout=3,read_timeout=5,retries={'max_attempts':1}))


def config(host):
    return {'endpoint':f'http://{host}:9090','region':'us-east-1','path_style_access':True}


def wait(call):
    deadline=time.monotonic()+60
    while True:
        try:return call()
        except Exception:
            if time.monotonic()>deadline:raise
            time.sleep(1)


def worker(path,port,advance,outage=False):
    from flowbridge.media_runtime import MediaRunner, MediaError, inspect_media
    blueprint=json.loads(Path(path,'blueprint.json').read_text())
    producer=Producer({'bootstrap.servers':'kafka:9092','enable.idempotence':True,'acks':'all'})
    def publish(topic,event):
        if outage:raise RuntimeError('injected publication outage')
        outcome=[]
        producer.produce(topic,key=event['job_id'].encode(),value=json.dumps(event).encode(),on_delivery=lambda error,message:outcome.append(error))
        if producer.flush(12) or not outcome or outcome[0] is not None:raise RuntimeError('Kafka publication failed')
    def process(job,content):
        metadata=inspect_media(job,content)
        client=http.client.HTTPConnection('127.0.0.1',port,timeout=3)
        try:
            client.request('POST','/process',json.dumps({'key':job['source_key']}),{'Content-Type':'application/json'})
            response=client.getresponse();response.read()
            if response.status!=200:raise MediaError('processing_503')
        finally:client.close()
        return metadata
    runner=MediaRunner(blueprint,str(Path(path,'ledger.sqlite')),client_factory=s3_client,processor=process,publisher=publish,clock=lambda:time.time()+advance)
    discovered=runner.discover()
    status=runner.run_once()
    print(json.dumps({'pid':os.getpid(),'discovery':discovered,'status':status}))


def main():
    signal.signal(signal.SIGALRM,lambda *_:(_ for _ in ()).throw(TimeoutError('Offline smoke exceeded 180s')))
    signal.alarm(180)
    # Check several public numeric destinations without external DNS lookup.
    blocked=[]
    for address in ('1.1.1.1','8.8.8.8'):
        try:
            with socket.create_connection((address,443),timeout=2):pass
        except OSError:blocked.append(address)
        else:raise AssertionError('External TCP egress unexpectedly succeeded')
    source=s3_client(config('source-s3'));target=s3_client(config('target-s3'))
    wait(source.list_buckets);wait(target.list_buckets)
    admin=AdminClient({'bootstrap.servers':'kafka:9092'});wait(lambda:admin.list_topics(timeout=3))
    tag=uuid.uuid4().hex[:10];pipelines=[];expected={}
    for kind,filename in [('image','image.png'),('text','note.txt'),('video','clip.mp4')]:
        incoming=f'fb-{tag}-{kind}-in';outgoing=f'fb-{tag}-{kind}-out'
        source.create_bucket(Bucket=incoming);target.create_bucket(Bucket=outgoing)
        value=Path('/demo/fixtures',filename).read_bytes();key='incoming/'+filename
        source.put_object(Bucket=incoming,Key=key,Body=value);expected[key]=value
        pipelines.append({'id':kind,'media_type':kind,'source':{'bucket':incoming,'prefix':'incoming/','s3':config('source-s3')},
            'destination':{'bucket':outgoing,'prefix':'processed/','s3':config('target-s3')},
            'stream':{'topic':f'fb-{tag}-{kind}','dead_letter_topic':f'fb-{tag}-{kind}-dlq'},
            'processing':{'engine':'http','url':'https://processor.internal/process','method':'POST'}})
    source.put_object(Bucket=pipelines[0]['source']['bucket'],Key='incoming/bad.png',Body=b'corrupt image')
    source.put_object(Bucket=pipelines[1]['source']['bucket'],Key='incoming/transient.txt',Body=b'recovered offline')
    expected['incoming/transient.txt']=b'recovered offline'
    for future in admin.create_topics([NewTopic(p['stream'][key],num_partitions=1,replication_factor=1) for p in pipelines for key in ('topic','dead_letter_topic')]).values():future.result(timeout=20)
    blueprint={'schema':'flowbridge/media-etl/v1','name':'offline-media-proof','s3':config('source-s3'),'kafka':{'brokers':'kafka:9092'},
               'delivery':{'checkpoint_backend':'sqlite','deduplication':'bucket-key-version-etag','retries':3},'pipelines':pipelines}
    # Real application service calls run with egress blocked. Package import
    # preserves the manifest; no target runtime is installed or executed here.
    from flowbridge import service
    from flowbridge.media import import_nifi_media
    from flowbridge.media_targets import import_media_package
    assert service.assess(blueprint)['report']['ok']
    packages={}
    for name in ('nifi','kafka','airflow','camel-k','seatunnel'):
        result=service.convert(blueprint,target=name)
        distinct_endpoint_blocked = name in ('nifi','camel-k','seatunnel')
        package_blueprint=blueprint
        if distinct_endpoint_blocked:
            assert not result['report']['ok'] and not result['files'],(name,result['report'])
            package_blueprint=copy.deepcopy(blueprint)
            for lane in package_blueprint['pipelines']:
                lane['source'].pop('s3');lane['destination'].pop('s3')
            result=service.convert(package_blueprint,target=name)
        assert result['files'],(name,result['report'])
        if name=='nifi':recovered=import_nifi_media(json.loads(result['files']['nifi-media-flow.json']))
        else:recovered=import_media_package(result['files'],name)
        assert recovered['report']['ok'],(name,recovered)
        assert recovered['blueprint']==package_blueprint,(name,'blueprint changed')
        packages[name]={'generated':True,'reimported':True,'ready':result['report'].get('ready',False),'native_runtime_executed':False,'distinct_endpoint_blocked':distinct_endpoint_blocked,'package_scope':'single_endpoint_fixture' if distinct_endpoint_blocked else 'two_endpoint_fixture'}
    calls=collections.Counter()
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_POST(self):
            key=json.loads(self.rfile.read(int(self.headers['Content-Length'])))['key'];calls[key]+=1
            self.send_response(503 if key.endswith('transient.txt') and calls[key]==1 else 200)
            self.end_headers();self.wfile.write(b'{}')
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    with tempfile.TemporaryDirectory() as state:
        Path(state,'blueprint.json').write_text(json.dumps(blueprint))
        snapshots=[]
        for index in range(4):
            command=[sys.executable,__file__,'--worker',state,str(server.server_port),str(index*65)]
            if index==0:command.append('--outage')
            result=subprocess.run(command,capture_output=True,text=True,check=True,timeout=40)
            snapshots.append(json.loads(result.stdout))
        assert len({s['pid'] for s in snapshots})==4
        assert snapshots[0]['discovery']['discovered']==5
        assert all(s['discovery']['discovered']==0 for s in snapshots[1:])
        assert all(j['copied']==0 and j['published']==0 for j in snapshots[0]['status']['jobs'])
        final=snapshots[-1]['status'];assert final['counts']['complete']==4 and final['counts']['dead_letter']==1,final
        assert all(j['published'] for j in final['jobs'])
        assert all(j['dlq_published'] for j in final['jobs'] if j['state']=='dead_letter')
        import sqlite3
        with sqlite3.connect(Path(state,'ledger.sqlite')) as db:
            db.row_factory=sqlite3.Row;rows=list(db.execute("SELECT * FROM jobs WHERE state='complete'"))
        for job in rows:
            item=target.get_object(Bucket=job['destination_bucket'],Key=job['destination_key'])
            content=item['Body'].read();item['Body'].close();assert content==expected[job['source_key']]
            assert item['Metadata']['flowbridge-job-id']==job['id']
            result=target.get_object(Bucket=job['destination_bucket'],Key=job['destination_key']+'.flowbridge-result.json')
            metadata=json.loads(result['Body'].read());result['Body'].close()
            assert metadata['sha256']==hashlib.sha256(content).hexdigest()
        # Distinct object stores, not two names for one endpoint.
        assert not ({b['Name'] for b in source.list_buckets()['Buckets']} & {b['Name'] for b in target.list_buckets()['Buckets']})
    consumer=Consumer({'bootstrap.servers':'kafka:9092','group.id':str(uuid.uuid4()),'auto.offset.reset':'earliest','enable.auto.commit':False})
    consumer.subscribe([p['stream'][k] for p in pipelines for k in ('topic','dead_letter_topic')])
    messages=[];deadline=time.monotonic()+20
    try:
        while len(messages)<6 and time.monotonic()<deadline:
            message=consumer.poll(1)
            if message is not None and not message.error():messages.append(json.loads(message.value()))
    finally:consumer.close()
    assert len(messages)==6
    assert all(m['source']['endpoint']=='http://source-s3:9090' and m['destination']['endpoint']=='http://target-s3:9090' for m in messages)
    server.shutdown();server.server_close();thread.join(timeout=2)
    print(json.dumps({'result':'passed','external_tcp_probes_blocked':blocked,'internal_s3_endpoints':2,'source_buckets':3,'destination_buckets':3,
        'completed':4,'dead_letter':1,'kafka_events_consumed':6,'worker_process_starts':4,'persistent_ledger_reused':True,
        'http_503_recovered':True,'simulated_publisher_failure_recovered':True,'offline_assessment':True,'offline_packages':packages,
        'runtime_dependency_downloads':False,'tls_verified':False,'production_airgap_certification':False}))

if __name__=='__main__':
    if len(sys.argv)>1 and sys.argv[1]=='--worker':worker(sys.argv[2],int(sys.argv[3]),int(sys.argv[4]),'--outage' in sys.argv)
    else:main()
