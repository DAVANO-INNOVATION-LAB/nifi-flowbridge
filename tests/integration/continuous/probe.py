import os,json
from api import NiFi
n=NiFi(os.environ['CONTINUOUS_TEST_PASSWORD']);root=n.call('/flow/process-groups/root')['processGroupFlow']['id']
group=n.call('/process-groups/'+root+'/process-groups',{'revision':{'version':0},'component':{'name':'continuous-probe','position':{'x':0,'y':0}}})['id']
for name,kind,bundle in [('List','org.apache.nifi.processors.aws.s3.ListS3','nifi-aws-nar'),('Fetch','org.apache.nifi.processors.aws.s3.FetchS3Object','nifi-aws-nar'),('Put','org.apache.nifi.processors.aws.s3.PutS3Object','nifi-aws-nar'),('Update','org.apache.nifi.processors.attributes.UpdateAttribute','nifi-update-attribute-nar')]:
 pid=n.create(group,kind,name,bundle);c=n.call('/processors/'+pid)['component']['config'];print(json.dumps({'processor':name,'properties':c['properties'],'descriptors':{k:{'name':v['displayName'],'required':v['required'],'default':v.get('defaultValue'),'allowed':v.get('allowableValues')} for k,v in c['descriptors'].items()}}))
