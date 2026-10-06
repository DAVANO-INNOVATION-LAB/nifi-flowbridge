"""Bounded load against an ephemeral local server, never the user's live jobs."""
import concurrent.futures,json,statistics,threading,time,urllib.request,urllib.error
from http.server import ThreadingHTTPServer
from pathlib import Path
from flowbridge.server import Handler
server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
doc=json.loads(Path('examples/nifi-continuous-media.json').read_text());base=f'http://127.0.0.1:{server.server_port}'
def run(i):
 path=['/api/assess','/api/convert','/api/download','/api/convert'][i%4]
 body={'document':doc if i%4!=3 else {'flowContents':{'processGroups':'invalid'}},'source':'nifi','target':'continuous-worker','nifi_version':'2'}
 req=urllib.request.Request(base+path,data=json.dumps(body).encode(),headers={'Content-Type':'application/json'})
 start=time.monotonic()
 try:
  with urllib.request.urlopen(req,timeout=30) as response:status=response.status;data=response.read()
 except urllib.error.HTTPError as response:status=response.code;data=response.read()
 assert status==(422 if i%4==3 else 200),(status,data[:200])
 if path!='/api/download':assert json.loads(data)['report']['ok']==(i%4!=3)
 else:assert data.startswith(b'PK')
 return time.monotonic()-start
try:
 start=time.monotonic()
 with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:times=list(pool.map(run,range(480)))
 elapsed=time.monotonic()-start
 print(json.dumps({'result':'passed','requests':len(times),'concurrent_clients':16,'expected_rejections':120,'unexpected_errors':0,'elapsed_seconds':round(elapsed,3),'requests_per_second':round(len(times)/elapsed,2),'p95_seconds':round(sorted(times)[int(len(times)*.95)],3),'max_seconds':round(max(times),3),'scope':'Ephemeral local HTTP server, mixed assessment/conversion/download/malformed flows'}))
finally:server.shutdown();server.server_close();thread.join()
