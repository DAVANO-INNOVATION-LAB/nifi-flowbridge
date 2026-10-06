import json,os,time,uuid
import boto3
from botocore.config import Config
from api import NiFi

def client(host):return boto3.client('s3',endpoint_url='http://'+host+':9090',region_name='us-east-1',aws_access_key_id='synthetic-only',aws_secret_access_key='synthetic-only',config=Config(s3={'addressing_style':'path'},connect_timeout=3,read_timeout=5,retries={'max_attempts':1}))
def setup():
 n=NiFi(os.environ['CONTINUOUS_TEST_PASSWORD']);root=n.call('/flow/process-groups/root')['processGroupFlow']['id'];tag=uuid.uuid4().hex[:8]
 parent=n.call('/process-groups/'+root+'/process-groups',{'revision':{'version':0},'component':{'name':'continuous-three-lane-'+tag,'position':{'x':0,'y':0}}})['id']
 svc=n.call('/process-groups/'+parent+'/controller-services',{'revision':{'version':0},'component':{'name':'Deployment identity only','type':'org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService','bundle':{'group':'org.apache.nifi','artifact':'nifi-aws-nar','version':'2.12.0'}}})
 sid=svc['id'];n.call('/controller-services/'+sid,{'revision':svc['revision'],'component':{'id':sid,'properties':{'Use Default Credentials':'true'}}},'PUT')
 svc=n.call('/controller-services/'+sid);n.call('/controller-services/'+sid+'/run-status',{'revision':svc['revision'],'state':'ENABLED','disconnectedNodeAcknowledged':False},'PUT')
 lanes=[]
 for index,kind in enumerate(('image','text','video')):
  gid=n.call('/process-groups/'+parent+'/process-groups',{'revision':{'version':0},'component':{'name':kind,'position':{'x':index*650,'y':0}}})['id']
  source='fb-'+tag+'-'+kind+'-in';dest='fb-'+tag+'-'+kind+'-out';client('source-s3').create_bucket(Bucket=source);client('target-s3').create_bucket(Bucket=dest)
  base={'Region':'us-east-1','AWS Credentials Provider Service':sid,'Use Path Style Access':'true'}
  ids=[]
  for offset,(kindshort,class_name,bundle) in enumerate([('List','org.apache.nifi.processors.aws.s3.ListS3','nifi-aws-nar'),('Fetch','org.apache.nifi.processors.aws.s3.FetchS3Object','nifi-aws-nar'),('Process','org.apache.nifi.processors.attributes.UpdateAttribute','nifi-update-attribute-nar'),('Put','org.apache.nifi.processors.aws.s3.PutS3Object','nifi-aws-nar')]):
   pid=n.create(gid,class_name,kind+' '+kindshort,bundle);ids.append(pid)
   if kindshort=='List':props={**base,'Bucket':source,'Endpoint Override URL':'http://source-s3:9090','Prefix':'incoming/'};terminated=();period='1 sec'
   elif kindshort=='Fetch':props={**base,'Bucket':source,'Endpoint Override URL':'http://source-s3:9090','Object Key':'${filename}'};terminated=('failure',);period='0 sec'
   elif kindshort=='Process':props={'media_type':kind,'Store State':'Do not store state'};terminated=();period='0 sec'
   else:props={**base,'Bucket':dest,'Endpoint Override URL':'http://target-s3:9090','Object Key':'${filename}','media_type':'${media_type}'};terminated=('success','failure');period='0 sec'
   n.configure(pid,props,terminated,period)
   existing=n.call('/processors/'+pid);n.call('/processors/'+pid,{'revision':existing['revision'],'component':{'id':pid,'position':{'x':0,'y':offset*180}}},'PUT')
  for left,right in zip(ids,ids[1:]):n.call('/process-groups/'+gid+'/connections',{'revision':{'version':0},'component':{'source':{'id':left,'groupId':gid,'type':'PROCESSOR'},'destination':{'id':right,'groupId':gid,'type':'PROCESSOR'},'selectedRelationships':['success']}})
  lanes.append({'kind':kind,'group':gid,'processors':ids,'source':source,'destination':dest})
 time.sleep(3)
 export=n.call('/process-groups/'+parent+'/download')
 return {'parent':parent,'lanes':lanes,'document':export}
if __name__=='__main__':print(json.dumps(setup()))
