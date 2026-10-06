"""Import a generated stopped flow into disposable NiFi; never starts processors.

Run from repository root. Requires Docker and the official NiFi image. Temporary
single-user credentials travel through a private env file/stdin, never stdout.
"""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import uuid

INSIDE = r'''
import hashlib,json,ssl,sys,time,urllib.request,urllib.parse,urllib.error,uuid
payload=json.load(sys.stdin)
base='https://localhost:8443/nifi-api'
until=time.monotonic()+150
while True:
    try:
        # Bootstrap trust from the dedicated container's own loopback listener.
        # Subsequent API calls validate this exact self-signed certificate.
        pem=ssl.get_server_certificate(('localhost',8443),timeout=3)
        context=ssl.create_default_context(cadata=pem)
        context.check_hostname=False
        opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),urllib.request.HTTPSHandler(context=context))
        request=urllib.request.Request(base+'/access/token',data=urllib.parse.urlencode({'username':payload['username'],'password':payload['password']}).encode(),headers={'Content-Type':'application/x-www-form-urlencoded'},method='POST')
        with opener.open(request,timeout=5) as response:token=response.read().decode()
        break
    except Exception:
        if time.monotonic()>until:raise RuntimeError('NiFi readiness/authentication deadline exceeded') from None
        time.sleep(3)
def api(path,body=None,content_type='application/json',method=None):
    request=urllib.request.Request(base+path,data=body,headers={'Authorization':'Bearer '+token,'Content-Type':content_type},method=method or ('POST' if body is not None else 'GET'))
    try:
        with opener.open(request,timeout=60) as response:return json.load(response)
    except urllib.error.HTTPError as error:
        text=error.read(4096).decode(errors='replace')
        # Generated fixture only; redact credentials defensively.
        text=text.replace(payload['password'],'[REDACTED]').replace(token,'[REDACTED]')
        raise RuntimeError('NiFi HTTP '+str(error.code)+': '+text) from None
root=api('/flow/process-groups/root')['processGroupFlow']['id']
native_root=api('/process-groups/'+root+'/download')
boundary='flowbridge-'+uuid.uuid4().hex
parts=[]
for name,value in [('groupName','flowbridge-media-native-import'),('positionX','0'),('positionY','0'),('clientId',str(uuid.uuid4()))]:
    parts.append(('--'+boundary+'\r\nContent-Disposition: form-data; name="'+name+'"\r\n\r\n'+value+'\r\n').encode())
parts.append(('--'+boundary+'\r\nContent-Disposition: form-data; name="file"; filename="flow.json"\r\nContent-Type: application/json\r\n\r\n').encode()+json.dumps(payload['flow']).encode()+b'\r\n')
parts.append(('--'+boundary+'--\r\n').encode())
created=api('/process-groups/'+root+'/process-groups/upload',b''.join(parts),'multipart/form-data; boundary='+boundary)
group=created.get('id') or created['component']['id']
processors=[];services=[];groups=[];connections=[]
def inspect(gid):
    groups.append(gid)
    flow=api('/flow/process-groups/'+gid)['processGroupFlow']['flow']
    connections.extend(flow.get('connections',[]))
    for entity in flow.get('processors',[]):
        component=api('/processors/'+entity['id'])['component']
        processors.append({'id':component['id'],'name':component['name'],'type':component['type'],'state':component.get('state'),'validationStatus':component.get('validationStatus'),'validationErrors':component.get('validationErrors',[])})
    data=api('/flow/process-groups/'+gid+'/controller-services')
    for entity in data.get('controllerServices',[]):
        component=entity['component']
        services.append({'id':component['id'],'id':component['id'],'name':component['name'],'type':component['type'],'state':component.get('state'),'validationStatus':component.get('validationStatus'),'validationErrors':component.get('validationErrors',[])})
    for entity in flow.get('processGroups',[]):inspect(entity['id'])
# Allow asynchronous validation to settle, then inspect without enabling anything.
time.sleep(10)
inspect(group)
processor_ids=[p['id'] for p in processors]
for pid in processor_ids:
    entity=api('/processors/'+pid)
    api('/processors/'+pid+'/run-status',json.dumps({'revision':entity['revision'],'state':'STOPPED','disconnectedNodeAcknowledged':False}).encode(),method='PUT')
time.sleep(5)
processors.clear();services.clear();groups.clear();connections.clear()
inspect(group)
for pid in processor_ids:
    entity=api('/processors/'+pid)
    api('/processors/'+pid+'/run-status',json.dumps({'revision':entity['revision'],'state':'DISABLED','disconnectedNodeAcknowledged':False}).encode(),method='PUT')
assert processors and all(p['state'] in ('STOPPED','DISABLED') for p in processors), 'Processors must stay stopped'
print(json.dumps({'result':'imported_stopped','nifi_version':'2.12.0','native_import_executed':True,'native_etl_executed':False,'validation_refreshed_while_stopped':True,'final_processors_disabled':True,'tls':'loopback certificate pinned; hostname check skipped only for isolated test','certificate_sha256':hashlib.sha256(ssl.PEM_cert_to_DER_cert(pem)).hexdigest(),'root_export_schema':{k:v for k,v in native_root.items() if k!='flowContents'},'groups':len(groups),'connections':len(connections),'processors':processors,'controller_services':list({s['id']:s for s in services}.values())}))
'''

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--docker-config')
    parser.add_argument('--context')
    parser.add_argument('--reuse-container',help='Reuse a previously retained disposable test instance')
    parser.add_argument('--keep-on-failure',action='store_true',help='Retain this disposable instance for explicit debugging; caller must remove it')
    parser.add_argument('--output', default='docs/nifi-native-import-evidence.json')
    args=parser.parse_args()
    docker=['docker']
    if args.docker_config:docker+=['--config',args.docker_config]
    if args.context:docker+=['--context',args.context]
    sys.path.insert(0,str(Path(__file__).resolve().parents[3]))
    from flowbridge.media import export_nifi_media
    blueprint=json.loads(Path('examples/media-etl.json').read_text())
    result=export_nifi_media(blueprint)
    assert 'nifi-media-flow.json' in result['files'],result['report']
    tag=uuid.uuid4().hex[:10];name='flowbridge-nifi-import-'+tag;network=name+'-net'
    if args.reuse_container:name=args.reuse_container;network=name+'-net'
    payload={'username':'flowbridge-demo','password':secrets.token_urlsafe(30),'flow':json.loads(result['files']['nifi-media-flow.json'])}
    def run(command,**kwargs):return subprocess.run(docker+command,check=True,capture_output=True,text=True,**kwargs)
    try:
        if args.reuse_container:
            existing=json.loads(run(['inspect',name]).stdout)[0]['Config']['Env']
            settings=dict(v.split('=',1) for v in existing if '=' in v)
            payload['username']=settings['SINGLE_USER_CREDENTIALS_USERNAME'];payload['password']=settings['SINGLE_USER_CREDENTIALS_PASSWORD']
        else:
            run(['network','create','--internal',network])
        with tempfile.TemporaryDirectory() as directory:
            env=Path(directory,'nifi.env')
            env.write_text('SINGLE_USER_CREDENTIALS_USERNAME='+payload['username']+'\nSINGLE_USER_CREDENTIALS_PASSWORD='+payload['password']+'\nNIFI_JVM_HEAP_INIT=256m\nNIFI_JVM_HEAP_MAX=512m\nNIFI_WEB_HTTPS_HOST=localhost\n')
            env.chmod(0o600)
            if not args.reuse_container:run(['run','-d','--name',name,'--network',network,'--memory','1g','--cpus','2','--cap-drop','ALL','--security-opt','no-new-privileges','--env-file',str(env),'apache/nifi:2.12.0'])
        completed=run(['run','--rm','-i','--network','container:'+name,'--read-only','--tmpfs','/tmp:rw,nosuid,nodev,size=16m','--memory','128m','--cap-drop','ALL','--security-opt','no-new-privileges','python:3.12-slim','python','-c',INSIDE],input=json.dumps(payload),timeout=210)
        proof=json.loads(completed.stdout)
        proof['recorded_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
        proof['fixture']='examples/media-etl.json'
        proof['generated_artifact_sha256']=hashlib.sha256(result['files']['nifi-media-flow.json'].encode()).hexdigest()
        proof['schema']='flowbridge/nifi-native-import-evidence/v1'
        proof['native_import_attempted']=True
        proof['resource_limits']={'memory':'1GiB','java_heap':'512MiB','host_ports':0}
        Path(args.output).write_text(json.dumps(proof,indent=2)+'\n')
        print(json.dumps({'result':proof['result'],'processors':len(proof['processors']),'groups':proof['groups'],'proof':args.output}))
    except subprocess.CalledProcessError as error:
        text=(error.stderr or '')[-5000:]
        text=text.replace(payload['password'],'[REDACTED]')
        try: logs=run(['exec',name,'tail','-n','1000','/opt/nifi/nifi-current/logs/nifi-app.log']).stdout
        except subprocess.CalledProcessError: logs=''
        details='\n'.join(line for line in logs.splitlines() if any(word in line for word in ('JsonMapping','Unrecognized','Deserializ','Cannot deserialize','through reference chain','flowEncodingVersion','Exception','ERROR','at org.apache.nifi','Caused by')))
        failure={'schema':'flowbridge/nifi-native-import-evidence/v1','recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'result':'import_failed','nifi_version':'2.12.0','native_import_attempted':True,'native_import_executed':False,'native_etl_executed':False,'error':text,'diagnostics':details.replace(payload['password'],'[REDACTED]'),'fixture':'examples/media-etl.json'}
        Path(args.output).write_text(json.dumps(failure,indent=2)+'\n')
        raise RuntimeError('Isolated NiFi import failed; sanitized evidence saved to '+args.output) from None
    finally:
        if args.keep_on_failure:
            print('Disposable debug instance: '+name+' network: '+network,file=sys.stderr)
        else:
            subprocess.run(docker+['rm','-fv',name],capture_output=True)
            subprocess.run(docker+['network','rm',network],capture_output=True)

if __name__=='__main__':main()
