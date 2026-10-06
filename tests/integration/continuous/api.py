import json
import ssl
import time
import urllib.request
import urllib.parse

class NiFi:
    def __init__(self, password):
        deadline=time.monotonic()+150
        while True:
            try:
                pem=ssl.get_server_certificate(('localhost',8443),timeout=3)
                context=ssl.create_default_context(cadata=pem);context.check_hostname=False
                self.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),urllib.request.HTTPSHandler(context=context))
                request=urllib.request.Request('https://localhost:8443/nifi-api/access/token',data=urllib.parse.urlencode({'username':'flowbridge-test','password':password}).encode(),headers={'Content-Type':'application/x-www-form-urlencoded'})
                with self.opener.open(request,timeout=5) as response:self.token=response.read().decode()
                break
            except Exception:
                if time.monotonic()>deadline:raise RuntimeError('NiFi startup/authentication timeout') from None
                time.sleep(2)
    def call(self,path,data=None,method=None):
        request=urllib.request.Request('https://localhost:8443/nifi-api'+path,data=None if data is None else json.dumps(data).encode(),headers={'Authorization':'Bearer '+self.token,'Content-Type':'application/json'},method=method or ('POST' if data is not None else 'GET'))
        with self.opener.open(request,timeout=30) as response:return json.load(response)
    def create(self,group,kind,name,bundle='nifi-aws-nar'):
        result=self.call('/process-groups/'+group+'/processors',{'revision':{'version':0},'component':{'type':kind,'name':name,'bundle':{'group':'org.apache.nifi','artifact':bundle,'version':'2.12.0'},'position':{'x':0,'y':0}}})
        return result['id']
    def configure(self,pid,properties,terminate=(),period='0 sec'):
        result=self.call('/processors/'+pid);component=result['component'];descriptors=component['config']['descriptors']
        by_name={v['displayName']:k for k,v in descriptors.items()}
        configured={by_name.get(k,k):v for k,v in properties.items()}
        return self.call('/processors/'+pid,{'revision':result['revision'],'component':{'id':pid,'config':{'properties':configured,'autoTerminatedRelationships':list(terminate),'schedulingPeriod':period}}},'PUT')
    def state(self,pid,state):
        result=self.call('/processors/'+pid)
        return self.call('/processors/'+pid+'/run-status',{'revision':result['revision'],'state':state,'disconnectedNodeAcknowledged':False},'PUT')
