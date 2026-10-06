import ast
import copy
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

from flowbridge.media_targets import export_media, import_media_package


def blueprint():
    return {'schema':'flowbridge/media-etl/v1','name':'media-demo','s3':{'endpoint':'https://s3.example.test','region':'us-east-1','path_style_access':True},'kafka':{'brokers':'kafka.example.test:9093'},'delivery':{'checkpoint_backend':'sqlite','deduplication':'bucket-key-version-etag','retries':3},'pipelines':[{'id':kind,'media_type':kind,'source':{'bucket':kind+'-source','prefix':'input/'},'destination':{'bucket':kind+'-target','prefix':'output/'},'stream':{'topic':'media-'+kind,'dead_letter_topic':'media-'+kind+'-dead'},'processing':{'engine':'http','url':'https://processor.example.test/'+kind,'method':'POST'}} for kind in ('image','text','video')]}


class MediaTargetTests(unittest.TestCase):
    def test_distinct_endpoints_block_native_but_preserve_worker_packages(self):
        data=blueprint();data['pipelines'][0]['destination']['s3']={**data['s3'],'endpoint':'https://other.example.test'}
        for target in ('camel-k','seatunnel'):
            result=export_media(data,target);self.assertFalse(result['report']['ok']);self.assertEqual(result['files'],{})
        for target in ('airflow','kafka'):
            result=export_media(data,target);self.assertTrue(result['report']['ok']);self.assertIn('client_factory',result['files']['flowbridge/media_runtime.py'])

    def test_airflow_exports_actual_three_lane_executor_dag(self):
        result=export_media(blueprint(),'airflow')
        self.assertTrue(result['report']['ok'],result['report'])
        code=result['files']['dags/flowbridge_media.py'];ast.parse(code)
        self.assertEqual(code.count('process_lane.override('),3)
        self.assertIn('from flowbridge.media_runtime import execute_pipeline',code)
        self.assertIn('return execute_pipeline(',code)
        self.assertIn('is_paused_upon_creation=True',code)
        self.assertIn('FLOWBRIDGE_MEDIA_STATE_DIR',code)
        self.assertIn('retries=0',code)
        self.assertIn('show_return_value_in_logs=False',code)
        self.assertIn('flowbridge/media_runtime.py',result['files'])
        self.assertIn('flowbridge/media.py',result['files'])
        self.assertNotIn('EmptyOperator',code)

    def test_kafka_package_is_real_worker_not_mislabeled_streams(self):
        result=export_media(blueprint(),'kafka')
        self.assertTrue(result['report']['ok'],result['report'])
        self.assertIn('from flowbridge.media_runtime import main',result['files']['run_worker.py'])
        self.assertIn('Kafka receives durable object-reference events',result['files']['README.md'])
        self.assertIn('SQLite remains the authoritative',result['files']['README.md'])
        self.assertIn('--pipeline PIPELINE_ID',result['files']['README.md'])
        self.assertNotIn('pom.xml',result['files'])

    def test_native_copy_drafts_expose_complete_contract_blockers(self):
        for target,prefix in [('seatunnel','seatunnel/'),('camel-k','camel-k/')]:
            with self.subTest(target=target):
                result=export_media(blueprint(),target)
                self.assertFalse(result['report']['ok'])
                self.assertEqual(result['report']['completeness'],'partial_native_copy_draft')
                self.assertTrue(result['report']['errors'])
                self.assertFalse(result['report']['native_runtime_verified'])
                configs=[json.loads(v) for k,v in result['files'].items() if k.startswith(prefix)]
                self.assertEqual(len(configs),3)
                if target=='seatunnel':
                    self.assertTrue(all(c['source']['S3File']['file_format_type']=='binary' for c in configs))
                    self.assertTrue(all(c['sink']['S3File']['file_format_type']=='binary' for c in configs))
                else:
                    self.assertTrue(all(c['kind']=='Integration' for c in configs))
                    self.assertTrue(all(c['spec']['flows'][0]['from']['parameters']['deleteAfterRead'] is False for c in configs))

    def test_all_packages_roundtrip_manifest_without_executing_artifacts(self):
        original=blueprint()
        for target in ('airflow','kafka','camel-k','seatunnel'):
            with self.subTest(target=target):
                exported=export_media(original,target)
                imported=import_media_package(exported['files'],target)
                self.assertTrue(imported['report']['ok'],imported)
                self.assertEqual(imported['blueprint'],original)
        self.assertEqual(original,blueprint())

    def test_modified_missing_extra_or_cross_target_artifacts_cannot_claim_roundtrip(self):
        package=export_media(blueprint(),'airflow')['files']
        for change in ('modified','missing','extra'):
            with self.subTest(change=change):
                files=dict(package)
                if change=='modified':files['dags/flowbridge_media.py']+='\n# changed\n'
                if change=='missing':del files['dags/flowbridge_media.py']
                if change=='extra':files['additional.py']='raise SystemExit(1)'
                self.assertFalse(import_media_package(files)['report']['ok'])
        self.assertFalse(import_media_package(package,'kafka')['report']['ok'])

    def test_invalid_contract_never_emits_target_files(self):
        invalid=blueprint();invalid['pipelines'][0]['source']['bucket']=''
        for target in ('airflow','kafka','camel-k','seatunnel'):
            self.assertEqual(export_media(invalid,target)['files'],{})

    def test_import_revalidates_even_self_consistent_manifest(self):
        files=export_media(blueprint(),'kafka')['files'];invalid=blueprint();invalid['pipelines'][0]['source']['bucket']=''
        import hashlib
        files['media-etl.json']=json.dumps(invalid)
        metadata=json.loads(files['media-package.json'])
        metadata['file_hashes']['media-etl.json']=hashlib.sha256(files['media-etl.json'].encode()).hexdigest()
        files['media-package.json']=json.dumps(metadata)
        self.assertFalse(import_media_package(files)['report']['ok'])

    def test_packaged_worker_runs_without_repository_imports(self):
        package=export_media(blueprint(),'kafka')['files']
        script=r'''
import io,json,pathlib
from flowbridge.media_runtime import MediaRunner
import flowbridge
assert pathlib.Path(flowbridge.__file__).resolve().parent==pathlib.Path.cwd()/'flowbridge'
model=json.loads(pathlib.Path('media-etl.json').read_text())
class S3:
 def __init__(self):self.output={}
 def list_objects_v2(self,**kw):return {'Contents':[{'Key':kw['Prefix']+'item.bin'}]}
 def head_object(self,**kw):return {'ETag':'"fixture"','VersionId':'version-1'}
 def get_object(self,**kw):return {'Body':io.BytesIO(b'synthetic-media'),'ContentLength':15,'ContentType':'application/octet-stream'}
 def put_object(self,**kw):self.output[(kw['Bucket'],kw['Key'])]=kw['Body']
s3=S3();events=[]
worker=MediaRunner(model,'state/jobs.db',s3,processor=lambda job,body:{'bytes':len(body)},publisher=lambda topic,event:events.append((topic,event)))
assert worker.discover()['discovered']==3
status=worker.run_once()
assert status['counts']['complete']==3,status
assert len(events)==3
assert len(s3.output)==6
assert worker.discover()['discovered']==0
assert worker.run_once()['counts']['complete']==3
assert len(events)==3
assert {bucket for bucket,key in s3.output}=={lane['destination']['bucket'] for lane in model['pipelines']}
print('Packaged runtime: three lanes copied, processed, checkpointed; repeat discovery deduplicated')
'''
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            for name,value in package.items():
                path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text(value)
            environment=dict(os.environ);environment.pop('PYTHONPATH',None)
            result=subprocess.run([sys.executable,'-c',script],cwd=directory,env=environment,capture_output=True,text=True,timeout=30)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            self.assertIn('Packaged runtime: three lanes',result.stdout)

    def test_package_files_contain_no_embedded_credentials(self):
        for target in ('airflow','kafka','camel-k','seatunnel'):
            result=export_media(blueprint(),target)
            canonical=json.loads(result['files']['media-etl.json'])
            self.assertEqual(canonical,blueprint())
            self.assertNotIn('aws_access_key_id',json.dumps(canonical))
            self.assertNotIn('password',json.dumps(canonical))


if __name__=='__main__':unittest.main()
