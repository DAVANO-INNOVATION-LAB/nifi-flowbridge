"""Strict native NiFi S3 migration profile and continuously polling target.

This accepts a bounded four-processor lane, never arbitrary NiFi execution.
The source must be stopped before baseline/backfill is explicitly approved.
"""
import argparse
import copy
import fcntl
import os
import hashlib
import json
import re
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit

SCHEMA = 'flowbridge/continuous-s3/v1'
FLEET_SCHEMA = 'flowbridge/continuous-s3-fleet/v1'
MAX_OBJECT = 64 * 1024 * 1024

class MigrationError(ValueError):
    pass


def need(ok, code):
    if not ok:
        raise MigrationError(code)


def _properties(processor):
    config = processor.get('config') or {}
    return processor.get('properties', config.get('properties', {}))


def _id(component):
    return component.get('identifier', component.get('id'))


def _endpoint(properties):
    value = properties.get('Endpoint Override URL')
    need(isinstance(value, str), 'explicit_s3_endpoint_required')
    parsed = urlsplit(value)
    need(parsed.scheme in ('http', 'https') and parsed.hostname and not parsed.username and not parsed.password and not parsed.query and not parsed.fragment, 'invalid_s3_endpoint')
    region = properties.get('Custom Region') if properties.get('Region') == 'use-custom-region' else properties.get('Region')
    need(isinstance(region, str) and re.fullmatch(r'[a-z0-9-]+', region), 'explicit_s3_region_required')
    style = properties.get('Use Path Style Access', 'false')
    need(style in ('true', 'false'), 'unsupported_addressing_style')
    return {'endpoint': value, 'region': region, 'path_style_access': style == 'true'}


# Nonempty settings are permitted only when their behavior is represented here.
COMMON_DEFAULTS = {'Communications Timeout': '30 secs', 'Multipart Threshold': '5 GB',
    'Multipart Part Size': '5 GB', 'Multipart Upload AgeOff Interval': '60 min',
    'Multipart Upload Max Age Threshold': '7 days', 'SSL Context Service': None,
    'Proxy Host': None, 'Proxy Host Port': None, 'proxy-configuration-service': None,
    'Requester Pays': 'false', 'Signer Override': 'Default Signature',
    'FullControl User List': "${literal('')}", 'Read Permission User List': "${literal('')}",
    'Write Permission User List': "${literal('')}", 'Read ACL User List': "${literal('')}",
    'Write ACL User List': "${literal('')}", 'Owner': "${literal('')}",
    'Storage Class': 'STANDARD', 'Server Side Encryption': 'None',
    'Canned ACL': "${literal('')}", 'Content Type': '${mime.type}',
    'Object Tags Prefix': None, 'Remove Tag Prefix': 'false',
    'Resource Transfer Source':'FLOWFILE_CONTENT', 'Temporary Directory Multipart State':'${java.io.tmpdir}', 'Use Chunked Encoding':'true', 's3-object-tags-prefix': None, 's3-object-remove-tags-prefix': 'false'}
S3_KEYS = {'Bucket', 'Region', 'Custom Region', 'Endpoint Override URL', 'Use Path Style Access', 'AWS Credentials Provider Service'}


def _check_properties(properties, required, defaults):
    need(isinstance(properties, dict), 'invalid_properties')
    for key, value in properties.items():
        if key in required:
            continue
        if value in (None, ''):
            continue
        need(key in defaults and value == defaults[key], 'unsupported_processor_property')


def analyze_nifi_s3(document, _fleet=False):
    report = {'ok': False, 'errors': [], 'warnings': []}
    try:
        need(isinstance(document, dict) and len(json.dumps(document)) <= 2_000_000, 'invalid_native_document')
        need(document.get('nifiVersion','2.12.0') in ('2','2.12.0'),'unsupported_nifi_version')
        from .graph import analyze_graph
        inspection = copy.deepcopy(document)
        for service in (inspection.get('flowContents') or {}).get('controllerServices', []):
            props = _properties(service)
            if props.get('Use Anonymous Credentials') == 'false':
                del props['Use Anonymous Credentials']
        inventory = analyze_graph(inspection)
        need(not any(d['severity'] == 'error' for d in inventory['diagnostics']), 'unsafe_or_invalid_native_graph')
        root = document.get('flowContents')
        need(isinstance(root, dict), 'native_flow_contents_required')
        need(not document.get('parameterContexts') and not document.get('parameterProviders') and not document.get('externalControllerServices'),'unsupported_external_configuration')
        need(not root.get('processors') and not root.get('connections'), 'root_must_only_contain_lanes')
        groups = root.get('processGroups', [])
        need(1 <= len(groups) <= 256 if _fleet else len(groups) == 3, 'unsupported_lane_count')
        services = root.get('controllerServices', [])
        service_ids = set()
        for service in services:
            need(service.get('type') == 'org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService', 'unsupported_controller_service')
            props = _properties(service)
            need(props.get('Use Default Credentials') == 'true', 'deployment_identity_required')
            _check_properties(props, {'Use Default Credentials'}, {'Use Anonymous Credentials':'false','Assume Role STS Region':'us-west-2','Assume Role Session Time':'3600'})
            service_ids.add(_id(service))
        lanes = []
        for group in [root] + groups:
            for field in ('inputPorts', 'outputPorts', 'funnels', 'remoteProcessGroups', 'parameterProviders'):
                need(not group.get(field), 'unsupported_group_structure')
            need(not group.get('parameterContextName') and not group.get('variables'), 'unsupported_group_parameters')
            need(group.get('flowFileConcurrency', 'UNBOUNDED') == 'UNBOUNDED' and group.get('flowFileOutboundPolicy', 'STREAM_WHEN_AVAILABLE') == 'STREAM_WHEN_AVAILABLE', 'unsupported_group_policy')
        for group in groups:
            need(not group.get('processGroups') and not group.get('controllerServices'), 'unsupported_nested_structure')
            processors = group.get('processors', [])
            need(len(processors) == 4, 'four_processors_per_lane_required')
            by_type = {p.get('type', '').rsplit('.', 1)[-1]: p for p in processors}
            need(set(by_type) == {'ListS3', 'FetchS3Object', 'UpdateAttribute', 'PutS3Object'}, 'unsupported_lane_processors')
            expected_types = {'ListS3':'org.apache.nifi.processors.aws.s3.ListS3','FetchS3Object':'org.apache.nifi.processors.aws.s3.FetchS3Object','PutS3Object':'org.apache.nifi.processors.aws.s3.PutS3Object','UpdateAttribute':'org.apache.nifi.processors.attributes.UpdateAttribute'}
            for kind, processor in by_type.items():
                need(processor.get('type') == expected_types[kind], 'unsupported_processor_type')
                bundle=processor.get('bundle') or {}
                need(bundle.get('group')=='org.apache.nifi' and bundle.get('version')=='2.12.0' and bundle.get('artifact')==('nifi-update-attribute-nar' if kind=='UpdateAttribute' else 'nifi-aws-nar'),'unsupported_native_bundle')
                need(not processor.get('annotationData') and not (processor.get('config') or {}).get('annotationData'), 'advanced_rules_unsupported')
            ordered = [by_type[kind] for kind in ('ListS3', 'FetchS3Object', 'UpdateAttribute', 'PutS3Object')]
            expected = {(_id(a), _id(b)) for a, b in zip(ordered, ordered[1:])}
            edges = group.get('connections', [])
            need(len(edges) == 3 and {((e.get('source') or {}).get('id'), (e.get('destination') or {}).get('id')) for e in edges} == expected, 'unsupported_lane_edges')
            for edge in edges:
                need(edge.get('selectedRelationships') == ['success'] and edge.get('flowFileExpiration', '0 sec') == '0 sec', 'unsupported_relationship_or_expiration')
                need(edge.get('loadBalanceStrategy', 'DO_NOT_LOAD_BALANCE') == 'DO_NOT_LOAD_BALANCE', 'unsupported_load_balancing')
            listing, fetch, update, put = [_properties(p) for p in ordered]
            for props in (listing, fetch, put):
                need(props.get('AWS Credentials Provider Service') in service_ids, 'unresolved_credentials_service')
            _check_properties(listing, S3_KEYS | {'Prefix', 'Listing Strategy', 'Use Versions'}, {**COMMON_DEFAULTS,'Minimum Object Age':'0 sec','Maximum Object Age':None,'Write Object Tags':'false','Record Writer':None,'List Type':'1','Listing Batch Size':'100','Write User Metadata':'false','Entity Tracking Time Window':'3 hours','Entity Tracking Initial Listing Target':'all'})
            need(listing.get('Use Versions', 'false') == 'false' and listing.get('Listing Strategy', 'timestamps') == 'timestamps', 'only_latest_object_listing_supported')
            _check_properties(fetch, S3_KEYS | {'Object Key', 'Version'}, COMMON_DEFAULTS)
            need(fetch.get('Object Key') == '${filename}' and not fetch.get('Version'), 'fetch_key_or_version_unsupported')
            _check_properties(put, S3_KEYS | {'Object Key','media_type'}, COMMON_DEFAULTS)
            need(put.get('media_type')=='${media_type}','media_type_metadata_mapping_required')
            need(all(put.get(key)=="${literal('')}" for key in ('FullControl User List','Read Permission User List','Read ACL User List','Write ACL User List','Canned ACL')),'explicit_empty_acl_required')
            need(put.get('Object Key') == '${filename}', 'destination_key_must_be_preserved')
            _check_properties(update, {'media_type'}, {'Store State':'Do not store state','canonical-value-lookup-cache-size':'100','Cache Value Lookup Cache Size':'100'})
            need(update.get('media_type') in ('image', 'text', 'video'), 'literal_media_type_required')
            source = _endpoint(listing); destination = _endpoint(put)
            need(source == _endpoint(fetch) and listing.get('Bucket') == fetch.get('Bucket'), 'fetch_must_match_listing_source')
            for props in (listing, put):
                need(isinstance(props.get('Bucket'), str) and re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', props['Bucket']), 'literal_bucket_required')
            prefix = listing.get('Prefix') or ''
            need(isinstance(prefix,str) and not any(c in prefix for c in ('$','#','{','}')), 'literal_prefix_required')
            need((source['endpoint'], listing['Bucket']) != (destination['endpoint'], put['Bucket']), 'source_destination_feedback')
            lanes.append({'id':_id(group),'media_type':update['media_type'],'source':{**source,'bucket':listing['Bucket'],'prefix':prefix},'destination':{**destination,'bucket':put['Bucket']}})
        need(_fleet or len({lane['media_type'] for lane in lanes}) == 3, 'unique_media_lanes_required')
        source_locations={(lane['source']['endpoint'],lane['source']['bucket']) for lane in lanes}
        dest_locations={(lane['destination']['endpoint'],lane['destination']['bucket']) for lane in lanes}
        need(len(source_locations)==len(lanes) and len(dest_locations)==len(lanes) and not source_locations & dest_locations,'cross_lane_feedback_or_overlap')
        profile={'schema':FLEET_SCHEMA if _fleet else SCHEMA,'lanes':lanes,'delivery':'at_least_once','maximum_object_bytes':MAX_OBJECT}
        report.update(ok=True,warnings=[{'code':'cutover_required','message':'Stop the NiFi source and drain its queues before establishing the target baseline. Producer arrivals may continue; missing destinations need explicit backfill approval. NiFi listing state is not copied.'},{'code':'bounded_semantics','message':'Latest objects only, byte-preserving keys, literal media_type processing and SHA256 audit. Deletions, historical versions, custom ACL/encryption/tags and arbitrary transformations are unsupported.'}])
        report.update(target='continuous-worker',native_version='2.12.0',runtime_validated=False)
        return {'report':report,'profile':profile,'migration_plan':{'source_state_imported':False,'source_stop_and_queue_drain_required':True,'producer_pause_required':False,'baseline':'Compare current source and destination bytes plus media_type metadata before checkpoint adoption.','backfill':'Missing or different destination objects require explicit overwrite/backfill approval.','ownership':'Only one target owner per persistent ledger; never run NiFi and target simultaneously.','rollback':'Stop target first, reconcile destination state and restore the saved NiFi configuration/state; rollback is not automatic.','limits':{'latest_objects_only':True,'max_objects_per_lane_per_poll':10000,'max_object_bytes':MAX_OBJECT,'deletions_propagated':False,'poll_retry_policy':'Failed objects are retried on subsequent scans; no success checkpoint is recorded.'}}}
    except (MigrationError, KeyError, TypeError, ValueError, AttributeError) as exc:
        report['errors']=[{'code':str(exc) if isinstance(exc,MigrationError) else 'unsupported_continuous_s3_profile','message':'Native flow is outside the verified continuous S3 profile; no executable migration was produced.'}]
        return {'report':report,'profile':None}


def export_nifi_s3(document):
    result=analyze_nifi_s3(document);result['files']={}
    if result['report']['ok']:
        result['files']={'continuous-s3-profile.json':json.dumps(result['profile'],indent=2)+'\n','flowbridge/nifi_s3.py':Path(__file__).read_text(),'flowbridge/__init__.py':'','requirements.txt':(Path(__file__).parent.parent/'requirements-media.txt').read_text(),'LICENSE':(Path(__file__).parent.parent/'LICENSE').read_text(),'README.md':'# Bounded continuous S3 migration\n\nStop and drain NiFi; snapshot/back up its state. Run `python -m flowbridge.nifi_s3 --profile continuous-s3-profile.json --state /persistent/continuous.db --source-stopped --accept-backfill --watch` only after approving latest-object backfill. Omit --accept-backfill to require every source object already equals its destination. Keep the SQLite ledger on persistent storage. Credentials use the SDK provider chain. Object keys and bytes are preserved; metadata media_type is literal. At-least-once, not exactly-once. Stop target before any rollback; NiFi state is not translated. Never run both owners simultaneously.\n\nEach poll is bounded to 10,000 listed objects per lane and 64 MiB per object. A listing_bound_exceeded or object_too_large failure requires reducing the scope or object size; it is not a completed migration. Other failed objects retry on later polls, and one failed object does not block its neighbors. A one-shot invocation exits nonzero when any transfer fails. Reconciliation checks ContentType as well as bytes and media_type; a failed recheck disables readiness until reconciliation succeeds again.\n'}
    return result


def validate_profile(profile):
    need(isinstance(profile,dict) and set(profile)=={'schema','lanes','delivery','maximum_object_bytes'},'invalid_profile')
    need(profile['schema'] in (SCHEMA,FLEET_SCHEMA) and profile['delivery']=='at_least_once' and profile['maximum_object_bytes']==MAX_OBJECT,'invalid_profile')
    need(isinstance(profile['lanes'],list) and (1<=len(profile['lanes'])<=256 if profile['schema']==FLEET_SCHEMA else len(profile['lanes'])==3),'invalid_profile')
    ids=set();kinds=set();sources=set();destinations=set()
    for lane in profile['lanes']:
        need(isinstance(lane,dict) and set(lane)=={'id','media_type','source','destination'},'invalid_profile_lane')
        need(isinstance(lane['id'],str) and re.fullmatch(r'[A-Za-z0-9-]{1,128}',lane['id']) and lane['id'] not in ids,'invalid_lane_identifier');ids.add(lane['id'])
        need(lane['media_type'] in ('image','text','video') and (profile['schema']==FLEET_SCHEMA or lane['media_type'] not in kinds),'invalid_media_type');kinds.add(lane['media_type'])
        for side,locations in (('source',sources),('destination',destinations)):
            config=lane[side]
            need(isinstance(config,dict) and set(config)==({'endpoint','region','path_style_access','bucket','prefix'} if side=='source' else {'endpoint','region','path_style_access','bucket'}),'invalid_endpoint_profile')
            _endpoint({'Endpoint Override URL':config['endpoint'],'Region':config['region'],'Use Path Style Access':str(config['path_style_access']).lower()})
            need(type(config['path_style_access']) is bool,'invalid_addressing_style')
            need(isinstance(config['bucket'],str) and re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]',config['bucket']),'invalid_bucket')
            location=(config['endpoint'],config['bucket']);need(location not in locations,'overlapping_lanes');locations.add(location)
            if side=='source':need(isinstance(config['prefix'],str) and len(config['prefix'])<=1024,'invalid_prefix')
    need(not sources & destinations,'source_destination_feedback')


class ContinuousS3Runner:
    def __init__(self,profile,state_path,client_factory):
        validate_profile(profile)
        self.profile=copy.deepcopy(profile);self.clients={};cache={}
        for lane in self.profile['lanes']:
            for side in ('source','destination'):
                config={k:lane[side][k] for k in ('endpoint','region','path_style_access')};key=json.dumps(config,sort_keys=True)
                if key not in cache:
                    try:cache[key]=client_factory(config)
                    except Exception:raise MigrationError('s3_client_creation_failed') from None
                self.clients[(lane['id'],side)]=cache[key]
        self.path=Path(state_path);self.path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        need(not self.path.is_symlink(),'symlink_state_refused')
        descriptor=os.open(self.path,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600);os.close(descriptor)
        with self.db() as db:
            db.executescript('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS config(id INTEGER PRIMARY KEY CHECK(id=1),fingerprint TEXT,ready INTEGER); CREATE TABLE IF NOT EXISTS copied(lane TEXT,key TEXT,identity TEXT,sha256 TEXT,PRIMARY KEY(lane,key));')
            fingerprint=hashlib.sha256(json.dumps(profile,sort_keys=True).encode()).hexdigest();row=db.execute('SELECT fingerprint,ready FROM config WHERE id=1').fetchone()
            need(not row or row[0]==fingerprint,'state_profile_mismatch')
            if not row:db.execute('INSERT OR IGNORE INTO config VALUES(1,?,0)',(fingerprint,))
            need(db.execute('SELECT fingerprint FROM config WHERE id=1').fetchone()[0]==fingerprint,'state_profile_mismatch')
        self.path.chmod(0o600)
    @contextmanager
    def db(self):
        db=sqlite3.connect(self.path,timeout=15)
        try:
            with db:yield db
        finally:db.close()
    def _keys(self,lane):
        client=self.clients[(lane['id'],'source')];token=None;count=0;seen=set()
        while True:
            args={'Bucket':lane['source']['bucket'],'Prefix':lane['source']['prefix'],'MaxKeys':1000}
            if token:args['ContinuationToken']=token
            page=client.list_objects_v2(**args)
            for obj in page.get('Contents',[]):
                count+=1;need(count<=10000,'listing_bound_exceeded');key=obj['Key']
                yield key
            if not page.get('IsTruncated'):break
            token=page.get('NextContinuationToken');need(isinstance(token,str) and token and token not in seen,'invalid_listing_page');seen.add(token)
    def _objects(self,lane):
        for key in self._keys(lane):
            head=self.clients[(lane['id'],'source')].head_object(Bucket=lane['source']['bucket'],Key=key)
            need(isinstance(head.get('ETag'),str),'missing_object_identity')
            yield key,head
    def _read(self,client,bucket,key,head=None):
        args={'Bucket':bucket,'Key':key}
        if head:
            args['IfMatch']=head['ETag']
            if head.get('VersionId'):args['VersionId']=head['VersionId']
        response=client.get_object(**args);body=response['Body']
        try:
            need(response.get('ContentLength',0)<=MAX_OBJECT,'object_too_large');content=body.read(MAX_OBJECT+1);need(len(content)<=MAX_OBJECT,'object_too_large')
            return content,response.get('ContentType','application/octet-stream')
        finally:body.close()
    @contextmanager
    def exclusive(self):
        descriptor=os.open(str(self.path)+'.lock',os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
        try:
            try:fcntl.flock(descriptor,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:raise MigrationError('another_worker_owns_state') from None
            yield
        finally:os.close(descriptor)
    def establish_shadow(self,production_profile):
        validate_profile(production_profile)
        expected={lane['id']:lane for lane in production_profile['lanes']}
        need(set(expected)=={lane['id'] for lane in self.profile['lanes']},'shadow_lane_mismatch')
        protected={lane[side]['bucket'] for lane in production_profile['lanes'] for side in ('source','destination')}
        for lane in self.profile['lanes']:
            need(lane['source']==expected[lane['id']]['source'] and lane['media_type']==expected[lane['id']]['media_type'],'shadow_source_or_transform_mismatch')
            need(lane['destination']['bucket'] not in protected,'shadow_destination_overlaps_production')
        with self.exclusive():
            with self.db() as db:db.execute('UPDATE config SET ready=1')
        return {'mode':'isolated_shadow','cutover':False}
    def establish_cutover(self,confirmed_source_stopped=False,backfill_existing=False):
        with self.exclusive():
            try:return self._establish_cutover(confirmed_source_stopped,backfill_existing)
            except MigrationError:raise
            except Exception:raise MigrationError('cutover_reconciliation_failed') from None
    def _establish_cutover(self,confirmed_source_stopped=False,backfill_existing=False):
        need(confirmed_source_stopped,'source_stop_confirmation_required');verified=0;pending=0
        # A failed revalidation must never leave an old ready flag or stale adoption.
        with self.db() as db:db.execute('UPDATE config SET ready=0')
        for lane in self.profile['lanes']:
            for key,head in self._objects(lane):
                content,content_type=self._read(self.clients[(lane['id'],'source')],lane['source']['bucket'],key,head)
                try:
                    target=self.clients[(lane['id'],'destination')]
                    target_head=target.head_object(Bucket=lane['destination']['bucket'],Key=key)
                    destination,target_type=self._read(target,lane['destination']['bucket'],key,target_head)
                except Exception:
                    need(backfill_existing,'destination_missing_or_unreadable');self._forget(lane,key);pending+=1;continue
                if content!=destination or content_type!=target_type or target_head.get('Metadata',{}).get('media_type')!=lane['media_type']:
                    need(backfill_existing,'destination_bytes_or_metadata_differ');self._forget(lane,key);pending+=1;continue
                self._checkpoint(lane,key,head,content);verified+=1
        with self.db() as db:db.execute('UPDATE config SET ready=1')
        return {'verified_existing':verified,'pending_backfill':pending,'source_state_imported':False}
    def _forget(self,lane,key):
        with self.db() as db:db.execute('DELETE FROM copied WHERE lane=? AND key=?',(lane['id'],key))
    def _checkpoint(self,lane,key,head,content):
        identity=json.dumps([head.get('VersionId'),head['ETag']])
        with self.db() as db:db.execute('INSERT OR REPLACE INTO copied VALUES(?,?,?,?)',(lane['id'],key,identity,hashlib.sha256(content).hexdigest()))
    def run_once(self):
        with self.exclusive():return self._run_once()
    def _run_once(self):
        with self.db() as db:need(db.execute('SELECT ready FROM config').fetchone()[0],'cutover_not_established')
        processed=[];failures=[]
        for lane in self.profile['lanes']:
            try:
                for key in self._keys(lane):
                    try:
                        head=self.clients[(lane['id'],'source')].head_object(Bucket=lane['source']['bucket'],Key=key)
                        need(isinstance(head.get('ETag'),str),'missing_object_identity')
                        with self.db() as db:row=db.execute('SELECT identity FROM copied WHERE lane=? AND key=?',(lane['id'],key)).fetchone()
                        if row and row[0]==json.dumps([head.get('VersionId'),head['ETag']]):continue
                        content,content_type=self._read(self.clients[(lane['id'],'source')],lane['source']['bucket'],key,head)
                        self.clients[(lane['id'],'destination')].put_object(Bucket=lane['destination']['bucket'],Key=key,Body=content,ContentType=content_type,Metadata={'media_type':lane['media_type']})
                        self._checkpoint(lane,key,head,content);processed.append({'lane':lane['id'],'key':key,'sha256':hashlib.sha256(content).hexdigest(),'bytes':len(content)})
                    except Exception as exc:failures.append({'lane':lane['id'],'object_id':hashlib.sha256(key.encode()).hexdigest()[:16],'code':str(exc) if isinstance(exc,MigrationError) else 'object_transfer_failed'})
            except Exception as exc:failures.append({'lane':lane['id'],'code':str(exc) if isinstance(exc,MigrationError) else 'source_scan_failed'})
        return {'counts':{'processed':len(processed),'failures':len(failures)},'processed':processed,'failures':failures,'delivery':'at_least_once'}


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--profile',required=True);parser.add_argument('--state',required=True);parser.add_argument('--source-stopped',action='store_true');parser.add_argument('--accept-backfill',action='store_true');parser.add_argument('--watch',action='store_true');parser.add_argument('--poll-seconds',type=int,default=5);args=parser.parse_args()
    need(1<=args.poll_seconds<=3600,'invalid_poll_interval')
    import boto3
    from botocore.config import Config
    def factory(config):return boto3.client('s3',endpoint_url=config['endpoint'],region_name=config['region'],config=Config(s3={'addressing_style':'path' if config['path_style_access'] else 'virtual'},connect_timeout=10,read_timeout=30,retries={'max_attempts':2}))
    runner=ContinuousS3Runner(json.loads(Path(args.profile).read_text()),args.state,factory)
    if args.source_stopped:print(json.dumps(runner.establish_cutover(True,args.accept_backfill)),flush=True)
    while True:
        result=runner.run_once();print(json.dumps(result),flush=True)
        if not args.watch:
            raise SystemExit(1 if result['counts']['failures'] else 0)
        time.sleep(args.poll_seconds)

if __name__=='__main__':main()
