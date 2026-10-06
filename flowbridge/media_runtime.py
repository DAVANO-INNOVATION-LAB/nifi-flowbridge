"""Bounded S3 media worker with a durable SQLite job ledger and Kafka event journal.

Credentials come from the S3 SDK provider chain. They never enter the ledger.
Kafka publication is at least once; downstream consumers must deduplicate job_id.
"""
import argparse
import hashlib
import json
import os
import sqlite3
import struct
import time
import urllib.request
import urllib.error
from contextlib import contextmanager
from pathlib import Path

MAX_OBJECT = 64 * 1024 * 1024

class MediaError(ValueError):
    pass


def inspect_media(job, content):
    """Deterministic demonstration processing, not image/video transcoding."""
    kind=job['media_type']; result={'sha256':hashlib.sha256(content).hexdigest(),'bytes':len(content),'media_type':kind}
    if kind=='text':
        try: text=content.decode('utf-8')
        except UnicodeError: raise MediaError('invalid_utf8') from None
        result.update({'characters':len(text),'words':len(text.split())})
    elif kind=='image':
        if len(content)<33 or content[:8]!=b'\x89PNG\r\n\x1a\n' or content[12:16]!=b'IHDR':raise MediaError('unsupported_image_demo_format')
        width,height=struct.unpack('>II',content[16:24])
        if not width or not height:raise MediaError('invalid_image_dimensions')
        result.update({'format':'png','width':width,'height':height,'inspection':'header_only'})
    elif kind=='video':
        if len(content)<16 or content[4:8]!=b'ftyp':raise MediaError('unsupported_video_demo_format')
        result.update({'format':'iso_bmff','brand':content[8:12].decode('ascii',errors='replace'),'inspection':'container_header_only'})
    else:raise MediaError('unsupported_media_type')
    return result


class MediaRunner:
    def __init__(self, blueprint, state_path, s3_client, processor=None, publisher=None, clock=time.time):
        from .media import validate_media
        validation=validate_media(blueprint)
        if not validation['report']['ok']:raise MediaError('invalid_blueprint')
        self.blueprint=validation['blueprint'];self.s3=s3_client;self.processor=processor or inspect_media;self.publisher=publisher;self.clock=clock
        self.path=Path(state_path);self.path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        if self.path.is_symlink():raise MediaError('state_path_must_not_be_symlink')
        descriptor=os.open(self.path,os.O_CREAT|os.O_RDWR,0o600);os.close(descriptor)
        self.pipelines={p['id']:p for p in self.blueprint['pipelines']}
        with self.db() as db:
            db.executescript('''PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY,lane TEXT,source_bucket TEXT,source_key TEXT,source_version TEXT,etag TEXT,destination_bucket TEXT,destination_key TEXT,media_type TEXT,state TEXT,attempts INTEGER DEFAULT 0,available REAL DEFAULT 0,lease REAL DEFAULT 0,published INTEGER DEFAULT 0,copied INTEGER DEFAULT 0,error TEXT,result TEXT,dlq_published INTEGER DEFAULT 0);
            ''')
        self.path.chmod(0o600)
    @contextmanager
    def db(self):
        db=sqlite3.connect(self.path,timeout=15);db.row_factory=sqlite3.Row
        try:
            with db:yield db
        finally:db.close()
    def discover(self):
        added=0; scanned=0
        for pipeline in self.pipelines.values():
            source=pipeline['source']; destination=pipeline['destination']; token=None
            while True:
                args={'Bucket':source['bucket'],'Prefix':source.get('prefix',''),'MaxKeys':1000}
                if token:args['ContinuationToken']=token
                try: page=self.s3.list_objects_v2(**args)
                except Exception: raise MediaError('source_listing_failed') from None
                for obj in page.get('Contents',[]):
                    scanned+=1
                    if scanned>10000:raise MediaError('discovery_limit')
                    key=obj['Key']
                    try:head=self.s3.head_object(Bucket=source['bucket'],Key=key)
                    except Exception:raise MediaError('source_head_failed') from None
                    version=head.get('VersionId');etag=head.get('ETag')
                    if not isinstance(etag,str):raise MediaError('source_identity_missing')
                    identity=[pipeline['id'],source['bucket'],key,version,etag]
                    ident=hashlib.sha256(json.dumps(identity,separators=(',',':')).encode()).hexdigest()
                    relative=key[len(source.get('prefix','')):]
                    outkey=destination.get('prefix','')+ident+'/'+relative.lstrip('/')
                    with self.db() as db:
                        added+=db.execute('INSERT OR IGNORE INTO jobs(id,lane,source_bucket,source_key,source_version,etag,destination_bucket,destination_key,media_type,state) VALUES(?,?,?,?,?,?,?,?,?,?)',(ident,pipeline['id'],source['bucket'],key,version,etag,destination['bucket'],outkey,pipeline['media_type'],'queued')).rowcount
                if not page.get('IsTruncated'):break
                token=page.get('NextContinuationToken')
                if not token:raise MediaError('invalid_listing_page')
        return {'discovered':added,'scanned':scanned}
    def _event(self,job):
        return {'schema':'flowbridge/media-event/v1','job_id':job['id'],'pipeline_id':job['lane'],'media_type':job['media_type'],'source':{'bucket':job['source_bucket'],'key':job['source_key'],'version':job['source_version'],'etag':job['etag']},'destination':{'bucket':job['destination_bucket'],'key':job['destination_key']}}
    def _dlq(self,job):
        if not self.publisher:return
        event=self._event(job);event['error_code']=job['error'];event['attempts']=job['attempts']
        self.publisher(self.pipelines[job['lane']]['stream']['dead_letter_topic'],event)
        with self.db() as db:db.execute('UPDATE jobs SET dlq_published=1 WHERE id=?',(job['id'],))
    def run_once(self):
        # A crash leaves a lease; it becomes claimable after expiry without stealing
        # another process's live work. Checkpointed copies/publications are retained.
        with self.db() as db:
            db.execute("UPDATE jobs SET state='retry' WHERE state='working' AND lease<?",(self.clock(),))
            pending=[dict(r) for r in db.execute("SELECT * FROM jobs WHERE state='dead_letter' AND dlq_published=0") if r['lane'] in self.pipelines]
        for job in pending:
            try:self._dlq(job)
            except Exception:pass # Durable pending DLQ row remains retryable.
        for _ in range(100):
            with self.db() as db:
                db.execute('BEGIN IMMEDIATE')
                marks=','.join('?' for _ in self.pipelines)
                row=db.execute(f"SELECT * FROM jobs WHERE state IN ('queued','retry') AND available<=? AND lane IN ({marks}) ORDER BY rowid LIMIT 1",(self.clock(),*self.pipelines)).fetchone()
                if row is None:break
                job=dict(row);db.execute("UPDATE jobs SET state='working',lease=?,attempts=attempts+1 WHERE id=?",(self.clock()+180,job['id']))
            try:
                event=self._event(job)
                if not job['published']:
                    if self.publisher:self.publisher(self.pipelines[job['lane']]['stream']['topic'],event)
                    with self.db() as db:db.execute('UPDATE jobs SET published=1 WHERE id=?',(job['id'],))
                get={'Bucket':job['source_bucket'],'Key':job['source_key'],'IfMatch':job['etag']}
                if job['source_version']:get['VersionId']=job['source_version']
                response=self.s3.get_object(**get)
                body=response['Body']
                try:
                    if response.get('ContentLength',0)>MAX_OBJECT:raise MediaError('object_too_large')
                    content=body.read(MAX_OBJECT+1)
                    if len(content)>MAX_OBJECT:raise MediaError('object_too_large')
                finally:body.close()
                if not job['copied']:
                    self.s3.put_object(Bucket=job['destination_bucket'],Key=job['destination_key'],Body=content,Metadata={'flowbridge-job-id':job['id'],'source-etag':job['etag'].strip('"')},ContentType=response.get('ContentType','application/octet-stream'))
                    with self.db() as db:db.execute('UPDATE jobs SET copied=1 WHERE id=?',(job['id'],))
                result=self.processor(job,content)
                encoded=json.dumps(result,allow_nan=False)
                if len(encoded.encode())>65536:raise MediaError('processing_result_too_large')
                self.s3.put_object(Bucket=job['destination_bucket'],Key=job['destination_key']+'.flowbridge-result.json',Body=encoded.encode(),ContentType='application/json',Metadata={'flowbridge-job-id':job['id']})
                with self.db() as db:db.execute("UPDATE jobs SET state='complete',result=?,error=NULL,lease=0 WHERE id=?",(encoded,job['id']))
            except Exception as exc:
                attempts=job['attempts']+1;limit=self.blueprint['delivery']['retries']+1
                state='dead_letter' if attempts>=limit else 'retry'
                code=str(exc) if isinstance(exc,MediaError) else 'operation_failed'
                with self.db() as db:db.execute('UPDATE jobs SET state=?,error=?,available=?,lease=0 WHERE id=?',(state,code,self.clock()+min(60,2**attempts),job['id']))
                if state=='dead_letter':
                    job.update(error=code,attempts=attempts)
                    try:self._dlq(job)
                    except Exception:pass
        return self.status()
    def status(self):
        with self.db() as db:
            jobs=[{k:r[k] for k in ('id','lane','state','attempts','error','published','copied','dlq_published')} for r in db.execute('SELECT * FROM jobs ORDER BY rowid') if r['lane'] in self.pipelines]
        counts={state:sum(j['state']==state for j in jobs) for state in ('queued','working','retry','complete','dead_letter')}
        return {'counts':counts,'jobs':jobs,'delivery':'at_least_once','checkpoint_backend':'sqlite','kafka_role':'durable_event_journal'}


def execute_pipeline(blueprint,pipeline_id,state_path):
    """Entrypoint for generated workers; incomplete batches fail orchestration."""
    import copy
    import boto3
    from botocore.config import Config
    from confluent_kafka import Producer
    selected=copy.deepcopy(blueprint)
    # Validate the complete three-lane contract before selecting a lane.
    runner=MediaRunner(selected,state_path,boto3.client('s3',endpoint_url=selected['s3']['endpoint'],region_name=selected['s3']['region'],config=Config(signature_version='s3v4',s3={'addressing_style':'path' if selected['s3']['path_style_access'] else 'virtual'},connect_timeout=10,read_timeout=30,retries={'max_attempts':2})))
    if pipeline_id not in runner.pipelines:raise MediaError('unknown_pipeline')
    runner.pipelines={pipeline_id:runner.pipelines[pipeline_id]}
    # Only explicit local plaintext or TLS configurations are accepted by librdkafka.
    extra=json.loads(os.environ.get('FLOWBRIDGE_KAFKA_CONFIG','{}'))
    allowed={'security.protocol','sasl.mechanism','sasl.username','sasl.password','ssl.ca.location','ssl.certificate.location','ssl.key.location','ssl.key.password'}
    if not isinstance(extra,dict) or set(extra)-allowed:raise MediaError('unsupported_kafka_security_config')
    producer=Producer({'security.protocol':'SSL',**extra,'bootstrap.servers':selected['kafka']['brokers'],'enable.idempotence':True,'acks':'all','delivery.timeout.ms':30000})
    def publish(topic,event):
        outcome=[]
        producer.produce(topic,key=event['job_id'].encode(),value=json.dumps(event).encode(),on_delivery=lambda error,message:outcome.append(error))
        if producer.flush(35) or not outcome or outcome[0] is not None:raise MediaError('kafka_delivery_failed')
    runner.publisher=publish
    pipeline=runner.pipelines[pipeline_id]
    def process(job,content):
        url=pipeline['processing']['url']
        if not url.startswith('https://'):raise MediaError('processing_https_required')
        event=runner._event(job)
        event['content_sha256']=hashlib.sha256(content).hexdigest()
        request=urllib.request.Request(url,data=json.dumps(event).encode(),method='POST',headers={'Content-Type':'application/json','Idempotency-Key':job['id']})
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self,*args,**kwargs):raise MediaError('processing_redirect_refused')
        opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
        try:
            with opener.open(request,timeout=30) as response:
                result=response.read(65537)
                if len(result)>65536:raise MediaError('processing_result_too_large')
                return json.loads(result)
        except MediaError:raise
        except Exception:raise MediaError('processing_failed') from None
    runner.processor=process
    runner.discover();status=runner.run_once()
    if any(status['counts'][k] for k in ('queued','working','retry','dead_letter')):raise MediaError('batch_incomplete_review_ledger')
    return status


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--blueprint',required=True);parser.add_argument('--state',required=True);parser.add_argument('--pipeline',required=True)
    mode=parser.add_mutually_exclusive_group();mode.add_argument('--once',action='store_true');mode.add_argument('--watch',action='store_true')
    parser.add_argument('--poll-seconds',type=int,default=15);args=parser.parse_args()
    if not 1<=args.poll_seconds<=3600:parser.error('poll-seconds must be between 1 and 3600')
    blueprint=json.loads(Path(args.blueprint).read_text())
    while True:
        try:print(json.dumps(execute_pipeline(blueprint,args.pipeline,args.state)),flush=True)
        except MediaError as exc:
            print(json.dumps({'status':'needs_review','error':str(exc)}),flush=True)
            if not args.watch:raise SystemExit(2) from None
        if not args.watch:break
        time.sleep(args.poll_seconds)
if __name__=='__main__':main()
