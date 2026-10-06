"""Media target capability checks and integrity-checked generated package imports.

Native connector support alone does not prove the complete media ETL contract.
"""
import copy
import hashlib
import json
from pathlib import Path

PACKAGE_SCHEMA = 'flowbridge/media-package/v1'


def _dump(value):
    return json.dumps(value, indent=2, sort_keys=True) + '\n'


def _digest(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def _validate(data):
    from .media import validate_media
    return validate_media(data)


def _block(target, message, code='media.target_not_supported'):
    return {'report': {'ok': False, 'target': target, 'errors': [{'code': code, 'message': message}], 'warnings': []}, 'files': {}}


def import_media_package(files, target=None):
    """Import only complete unchanged generated packages, never arbitrary DAG code.

    No input Python, Camel route, connector code or shell content is executed.
    The manifest is authoritative only when every packaged artifact matches it.
    """
    try:
        if not isinstance(files,dict) or len(files)>100 or any(not isinstance(k,str) or not isinstance(v,str) for k,v in files.items()):
            raise ValueError()
        package=json.loads(files['media-package.json'])
        if not isinstance(package,dict):
            raise ValueError()
        if package.get('schema')!=PACKAGE_SCHEMA or package.get('target') not in ('airflow','kafka','camel-k','seatunnel'):
            raise ValueError()
        if target is not None and target!=package['target']:
            raise ValueError()
        hashes=package['file_hashes']
        if not isinstance(hashes,dict) or set(hashes)!=set(files)-{'media-package.json'}:
            raise ValueError()
        for name, expected in hashes.items():
            path=Path(name)
            if path.is_absolute() or '..' in path.parts or '\\' in name or _digest(files[name])!=expected:
                raise ValueError()
        result=_validate(json.loads(files['media-etl.json']))
        if not result['report']['ok']:
            return result
        export_report=json.loads(files['compatibility-report.json'])
        if not isinstance(export_report,dict):
            raise ValueError()
        warnings=[{'code':'media.package_import','message':'Imported the checksum-consistent package manifest. This validates manifest preservation, not target runtime equivalence or artifact provenance. Arbitrary native projects are not reverse-translated.'}]
        if export_report.get('completeness')=='partial_native_copy_draft':
            warnings.append({'code':'media.partial_native_import','message':'The recovered package remains a partial native copy draft with unresolved ETL blockers. Import success does not resolve those blockers.'})
        return {'report': {'ok':True,'source':package['target'],'errors':[],'warnings':warnings,'native_runtime_verified':False,'export_completeness':export_report.get('completeness')},'blueprint':result['blueprint']}
    except (KeyError,ValueError,TypeError):
        return {'report':{'ok':False,'errors':[{'code':'media.package_integrity','message':'Supply a complete unchanged Flowbridge media package. Missing, modified or unrecognized artifacts cannot be imported as equivalent.'}],'warnings':[]},'blueprint':None}


def _package(blueprint,target,files,report):
    files=dict(files)
    files['media-etl.json']=_dump(blueprint)
    files['compatibility-report.json']=_dump(report)
    files['LICENSE']=Path(__file__).parent.parent.joinpath('LICENSE').read_text()
    files['media-package.json']=_dump({'schema':PACKAGE_SCHEMA,'target':target,'file_hashes':{name:_digest(text) for name,text in files.items()}})
    return {'report':report,'files':files,'blueprint':copy.deepcopy(blueprint)}


def export_media(data,target):
    checked=_validate(data)
    if not checked['report']['ok']:
        return {'report':checked['report'],'files':{}}
    if target in ('seatunnel','camel-k'):
        from .media import s3_config
        if any(s3_config(data,lane,side)!=data['s3'] for lane in data['pipelines'] for side in ('source','destination')):
            return _block(target,'Native media drafts do not support distinct S3 endpoint settings; use the reference worker.','media.multiple_s3_endpoints_unsupported')
        return _native_draft(checked['blueprint'],target,checked['report'])
    if target not in ('airflow','kafka'):
        return _block(target,'Choose a supported media package target.', 'media.target')
    return _export_runtime(checked['blueprint'],target,checked['report'])


def _export_runtime(blueprint,target,validation_report):
    root=Path(__file__).parent.parent
    if not root.joinpath('flowbridge/media_runtime.py').is_file():
        return _block(target,'The shared media executor is not installed.', 'media.runtime_unavailable')
    files={}
    for name in ('media.py','media_runtime.py'):
        source=root/'flowbridge'/name
        if not source.exists(): return _block(target,'The shared media runtime is incomplete.','media.runtime_unavailable')
        files['flowbridge/'+name]=source.read_text()
    files['flowbridge/__init__.py']=''

    requirements=root/'requirements-media.txt'
    files['requirements.txt']=requirements.read_text() if requirements.exists() else 'boto3==1.42.49\nconfluent-kafka==2.15.1\n'
    if target=='airflow':
        ident='flowbridge_media_'+_digest(_dump(blueprint))[:16]
        files['dags/flowbridge_media.py']=_airflow_dag(blueprint,ident)
    else:
        files['run_worker.py']="from flowbridge.media_runtime import main\nif __name__ == '__main__':\n    main()\n"
    report=copy.deepcopy(validation_report)
    report.update({'ok':True,'target':target,'completeness':'reference_executor','native_runtime_verified':False,'runtime':'Python S3 reference executor with durable Kafka event journal'})
    report['warnings']=report.get('warnings',[])+[
        {'code':'media.kafka_journal','message':'Kafka stores durable object-reference events; SQLite is the work queue and checkpoint authority. This is not a Kafka-consuming distributed worker or Kafka Streams topology.'},
        {'code':'media.executor_validation','message':'Install and test the packaged executor with isolated buckets, topics and a processing endpoint before running. Airflow is manual and paused; source objects are not deleted.'},
    ]
    files['README.md']=_runtime_readme(target)
    return _package(blueprint,target,files,report)


def _airflow_dag(blueprint,ident):
    # Pipeline IDs are data, never Python identifiers. Hashes also avoid path traversal.
    lines=['"""Manual S3 media batches; persistent state and external credentials required."""',
           'import json','import os','from pathlib import Path','from datetime import datetime, timedelta, timezone',
           'from airflow.sdk import DAG, task','from flowbridge.media_runtime import execute_pipeline',
           'BLUEPRINT=json.loads('+repr(json.dumps(blueprint))+')','',
           '@task(retries=0, execution_timeout=timedelta(minutes=30), show_return_value_in_logs=False)',
           'def process_lane(pipeline_id, state_name):',
           '    directory=Path(os.environ["FLOWBRIDGE_MEDIA_STATE_DIR"])',
           '    return execute_pipeline(BLUEPRINT,pipeline_id,str(directory/state_name))','',
           'with DAG(dag_id='+repr(ident)+', schedule=None, start_date=datetime(2025,1,1,tzinfo=timezone.utc), catchup=False, is_paused_upon_creation=True, max_active_runs=1, tags=["flowbridge","media"]) as dag:']
    for lane in blueprint['pipelines']:
        key=_digest(lane['id'])[:16]
        lines.append('    process_lane.override(task_id='+repr('lane_'+key)+')('+repr(lane['id'])+','+repr('lane_'+key+'.db')+')')
    return '\n'.join(lines)+'\n'


def _runtime_readme(target):
    return """# Media reference executor package

This package executes the three-lane S3 media contract through Flowbridge's Python reference executor. It is not a native Kafka Streams implementation. Kafka receives durable object-reference events before S3 copy/processing; SQLite remains the authoritative work queue and checkpoint store. Media bytes remain in S3, not Kafka messages or Airflow XCom.

Install the packaged requirements in an isolated environment and make the included `flowbridge` package importable. Supply AWS credentials through the SDK's normal external provider chain; never edit secrets into the manifest. Provision source/destination buckets, Kafka topics and the HTTP processing endpoint first. Review media-etl.json and compatibility-report.json. The HTTP processor receives a JSON object reference after destination copy, not the media payload. Processing must honor idempotency keys; neither retries nor cancellation can undo external effects.

For the worker, run `python -m flowbridge.media_runtime --blueprint media-etl.json --pipeline PIPELINE_ID --state /data/PIPELINE_ID.db --once`. Replace PIPELINE_ID with one of the manifest pipeline IDs and run each lane with its own state path. For continuous polling, replace `--once` with `--watch --poll-seconds 15`; retries use the same durable ledger and failures remain visible. Keep `/data` persistent and private. Re-run only with the same reviewed manifest/state; inspect retry/DLQ results rather than treating process completion as successful processing.

For Airflow, install the package/requirements on every task worker, copy dags/flowbridge_media.py into an Airflow 3 DAG bundle, and set FLOWBRIDGE_MEDIA_STATE_DIR to persistent writable storage visible consistently to each lane across runs. Each lane has its own ledger. The DAG is paused and manually triggered; it is not a continuously streaming scheduler. Do not run the same lane concurrently through another deployment. execute_pipeline fails if pending retries or failed/DLQ jobs remain; operators must inspect the recorded result. No media body is intentionally returned to XCom.

Package import verifies every artifact digest and restores the canonical manifest. It does not parse arbitrary native code or infer modifications. A manifest round trip does not establish runtime or data equivalence. Validate representative images, text and videos plus failure/retry behavior against your own endpoints before deployment.
"""


def _native_draft(blueprint,target,validation_report):
    files={};s3=blueprint['s3']
    for lane in blueprint['pipelines']:
        ident='lane-'+_digest(lane['id'])[:12]
        if target=='seatunnel':
            def connector(location):
                return {'bucket':'s3a://'+location['bucket'],'path':'/'+location['prefix'].lstrip('/'),'file_format_type':'binary','fs.s3a.endpoint':s3['endpoint'],'fs.s3a.aws.credentials.provider':'com.amazonaws.auth.InstanceProfileCredentialsProvider','hadoop_s3_properties':{'fs.s3a.path.style.access':str(s3['path_style_access']).lower()}}
            source=connector(lane['source']);source['plugin_output']=ident
            sink=connector(lane['destination']);sink.update({'plugin_input':ident,'tmp_path':'/.flowbridge-tmp/'+ident})
            files['seatunnel/'+ident+'.json']=_dump({'env':{'job.mode':'BATCH','parallelism':1},'source':{'S3File':source},'sink':{'S3File':sink}})
        else:
            source={'region':s3['region'],'prefix':lane['source']['prefix'],'deleteAfterRead':False,'includeBody':True,'useDefaultCredentialsProvider':True,'autoCreateBucket':False,'overrideEndpoint':True,'uriEndpointOverride':s3['endpoint'],'forcePathStyle':s3['path_style_access']}
            sink={'region':s3['region'],'useDefaultCredentialsProvider':True,'autoCreateBucket':False,'overrideEndpoint':True,'uriEndpointOverride':s3['endpoint'],'forcePathStyle':s3['path_style_access']}
            route={'from':{'uri':'aws2-s3://'+lane['source']['bucket'],'parameters':source,'steps':[
                {'setHeader':{'name':'CamelAwsS3Key','simple':lane['destination']['prefix']+'${header.CamelAwsS3Key}'}},
                {'to':{'uri':'aws2-s3://'+lane['destination']['bucket'],'parameters':sink}},
            ]}}
            files['camel-k/'+ident+'.json']=_dump({'apiVersion':'camel.apache.org/v1','kind':'Integration','metadata':{'name':ident},'spec':{'flows':[route]}})
    report=copy.deepcopy(validation_report)
    report.update({'ok':False,'target':target,'completeness':'partial_native_copy_draft','native_runtime_verified':False})
    report['errors']=report.get('errors',[])+[{'code':'media.native_semantics_gap','message':'These native S3 copy drafts do not implement the complete Kafka object-reference journal, SQLite deduplication/checkpoints, per-lane dead-letter handling or post-copy HTTP processing contract. Native key/version and binary reconstruction behavior need runtime validation. Do not deploy them as an equivalent migration.'}]
    report['warnings']=report.get('warnings',[])+[{'code':'media.native_execution','message':'Native artifacts can begin copying when deployed. No deployment was performed; this is a review-only partial package with unresolved blockers.'}]
    files['README.md']=_native_readme(target)
    return _package(blueprint,target,files,report)


def _native_readme(target):
    detail = ('SeaTunnel 2.3.13 S3File supports binary media, including images/video and chunked reads. These separate per-lane JSON jobs exercise that native copy stage. Credentials use the external instance-profile provider. Validate binary reconstruction, destination filenames, connector dependencies and S3 endpoint compatibility. Source: https://seatunnel.apache.org/docs/2.3.13/connectors/source/S3File/.' if target=='seatunnel' else 'Camel AWS2 S3 supports object retrieval/upload. These Camel K Integration resources show the native copy stage using external default AWS credentials. The destination prefix is prepended to the full source key. No durable idempotent repository is configured: polling may copy an object repeatedly. Source: https://camel.apache.org/components/4.18.x/aws2-s3-component.html.')
    return '# Partial native copy draft — NOT equivalent media ETL\n\n'+detail+'\n\nThe complete requested contract is blocked: Kafka object-reference publication, checkpoint authority, duplicate control, DLQ/retry handling and HTTP processing are not implemented by these native drafts. compatibility-report.json has ok=false intentionally. Do not treat a successful import, parse or connector copy as complete migration validation. Deploying these resources may copy data; Flowbridge does not deploy them.\n\nThe unchanged complete package can be imported back to its canonical media manifest by checksum verification. This proves manifest preservation only, not reverse engineering arbitrary native configuration or delivery semantics.\n'
