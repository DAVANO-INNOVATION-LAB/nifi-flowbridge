"""Server-observed NiFi stop/drain proofs; never frontend readiness booleans.

A persisted proof records a past observation. Revalidate immediately before any
promotion; this adapter cannot prevent an external operator restarting NiFi.
"""
import hashlib
import json
import os
import re
import time
import uuid
from pathlib import Path
from .platforms import _identifier, fail

SOURCE_TYPES = {'org.apache.nifi.processors.aws.s3.ListS3', 'org.apache.nifi.kafka.processors.ConsumeKafka'}
OTHER_TYPES = {'org.apache.nifi.processors.aws.s3.FetchS3Object', 'org.apache.nifi.processors.aws.s3.PutS3Object', 'org.apache.nifi.processors.attributes.UpdateAttribute', 'org.apache.nifi.kafka.processors.PublishKafka'}


def _counter(value):
    if type(value) is int and value >= 0:
        return value
    if isinstance(value,str) and re.fullmatch(r'(?:[0-9]+|[1-9][0-9]{0,2}(?:,[0-9]{3})+)',value):
        return int(value.replace(',',''))
    raise ValueError('invalid_counter')


class NiFiFence:
    def __init__(self, client, proof_dir, clock=time.time, sleep=time.sleep):
        self.client=client;self.directory=Path(proof_dir);self.clock=clock;self.sleep=sleep
        self._deadline=None
        self.identity=hashlib.sha256(client._base.encode()).hexdigest()
        self.directory.mkdir(parents=True,exist_ok=True,mode=0o700)
        if self.directory.is_symlink():fail('unsafe_proof_store','Proof storage cannot be a symbolic link.')

    def _request(self,*args,**kwargs):
        remaining=self._deadline-self.clock() if self._deadline is not None else 30
        if remaining<=0:fail('fence_timeout','NiFi fencing deadline exceeded; no promotion is authorized.')
        previous=getattr(self.client,'_timeout',None)
        if previous is not None:self.client._timeout=min(previous,remaining)
        try:result=self.client.request(*args,**kwargs)
        finally:
            if previous is not None:self.client._timeout=previous
        if self._deadline is not None and self.clock()>self._deadline:fail('fence_timeout','NiFi fencing deadline exceeded; no promotion is authorized.')
        return result

    def _group(self,group_id):
        group_id=_identifier(group_id)
        if group_id!='root':return group_id
        data=self._request('GET','/flow/process-groups/root')
        actual=_identifier(data.get('processGroupFlow',{}).get('id'))
        if actual=='root':fail('identity_mismatch','NiFi did not resolve the root group identity.')
        return actual

    def _discover(self, group_id):
        pending=[_identifier(group_id)];groups=[];processors={};connections=[];configurations={};services={};group_settings={};connection_settings={}
        while pending:
            gid=pending.pop()
            if gid in groups or len(groups)>=256:fail('invalid_hierarchy','NiFi hierarchy is cyclic or exceeds the supported bound.')
            groups.append(gid)
            metadata=self._request('GET','/process-groups/'+gid).get('component',{})
            if metadata.get('id')!=gid:fail('identity_mismatch','NiFi group metadata identity differs.')
            settings={key:metadata.get(key) for key in ('parameterContext','variables','flowFileConcurrency','flowFileOutboundPolicy','executionEngine')}
            group_settings[gid]=hashlib.sha256(json.dumps(settings,sort_keys=True).encode()).hexdigest()
            service_data=self._request('GET','/flow/process-groups/'+gid+'/controller-services',query={'includeAncestorGroups':'true','includeDescendantGroups':'false'})
            if not isinstance(service_data.get('controllerServices'),list):fail('missing_services','Controller service inventory was not readable.')
            for wrapper in service_data['controllerServices']:
                component=wrapper.get('component',{});sid=_identifier(wrapper.get('id'))
                if component.get('id')!=sid or not isinstance(component.get('properties'),dict):fail('missing_services','Controller service configuration was not readable.')
                digest=hashlib.sha256(json.dumps({'type':component.get('type'),'properties':component['properties'],'revision':wrapper.get('revision')},sort_keys=True).encode()).hexdigest()
                if sid in services and services[sid]!=digest:fail('services_changed','Controller services changed during discovery.')
                services[sid]=digest
            envelope=self._request('GET','/flow/process-groups/'+gid)
            group=envelope.get('processGroupFlow',{})
            if group.get('id')!=gid:fail('identity_mismatch','NiFi returned another process group.')
            flow=group.get('flow',{})
            if any(flow.get(k) for k in ('inputPorts','outputPorts','remoteProcessGroups','funnels')):fail('unsupported_boundary','Ports, remotes and funnels require an explicit fence mapping.')
            for wrapper in flow.get('processors',[]):
                component=wrapper.get('component',{});pid=_identifier(wrapper.get('id'))
                if component.get('id')!=pid or component.get('type') not in SOURCE_TYPES|OTHER_TYPES or pid in processors:fail('unsupported_processor','Live processor identity/type is unsupported.')
                processors[pid]=component['type']
                config=component.get('config')
                if not isinstance(config,dict):config=self._state(pid)['component'].get('config')
                if not isinstance(config,dict):fail('missing_configuration','NiFi processor configuration was not readable.')
                configurations[pid]=hashlib.sha256(json.dumps(config,sort_keys=True).encode()).hexdigest()
            for wrapper in flow.get('connections',[]):
                component=wrapper.get('component',{});source=component.get('source',{});target=component.get('destination',{})
                cid=_identifier(wrapper.get('id'))
                connections.append((cid,source.get('id'),target.get('id')))
                settings={key:component.get(key) for key in ('selectedRelationships','flowFileExpiration','prioritizers','loadBalanceStrategy','loadBalanceCompression','loadBalancePartitionAttribute','backPressureObjectThreshold','backPressureDataSizeThreshold','labelIndex')}
                connection_settings[cid]=hashlib.sha256(json.dumps(settings,sort_keys=True).encode()).hexdigest()
            pending.extend(_identifier(item.get('id')) for item in flow.get('processGroups',[]))
            if len(processors)>1024:fail('too_many_processors','NiFi processor bound exceeded.')
        if not processors or not any(kind in SOURCE_TYPES for kind in processors.values()):fail('source_not_found','No supported ingestion source exists in this scope.')
        if any(a not in processors or b not in processors for _,a,b in connections):fail('external_connection','Connections leave the assessed processor scope.')
        return {'groups':sorted(groups),'processors':dict(sorted(processors.items())),'configuration_sha256':dict(sorted(configurations.items())),'controller_service_sha256':dict(sorted(services.items())),'group_settings_sha256':dict(sorted(group_settings.items())),'connection_settings_sha256':dict(sorted(connection_settings.items())),'connections':[list(item) for item in sorted(connections)]}

    def _state(self, pid):
        data=self._request('GET','/processors/'+pid)
        if data.get('component',{}).get('id')!=pid:fail('identity_mismatch','Processor readback identity differs.')
        return data

    def _stop(self,pid):
        data=self._state(pid)
        if data['component'].get('state') in ('STOPPED','DISABLED'):return
        revision=data.get('revision')
        if not isinstance(revision,dict) or type(revision.get('version')) is not int:fail('missing_revision','Processor stop requires a live revision.')
        self._request('PUT','/processors/'+pid+'/run-status',{'revision':revision,'state':'STOPPED','disconnectedNodeAcknowledged':False})

    def _quiet(self,group_id,processors):
        snapshot=self._request('GET','/flow/process-groups/'+group_id+'/status',query={'recursive':'true'}).get('processGroupStatus',{}).get('aggregateSnapshot',{})
        # Missing counters are unknown, never interpreted as zero.
        queued=snapshot.get('flowFilesQueued',snapshot.get('queuedCount'))
        active=snapshot.get('activeThreadCount')
        try:
            queued=_counter(queued);active=_counter(active)
        except (ValueError,TypeError):fail('unknown_drain','NiFi did not return trustworthy drain counters.')
        states={pid:self._state(pid)['component'].get('state') for pid in processors}
        return queued==0 and active==0,states

    def stop_and_drain(self,group_id,timeout=30,validate_scope=None):
        if not isinstance(timeout,(int,float)) or not 0<timeout<=60:fail('invalid_timeout','Drain timeout must be at most 60 seconds.')
        self._deadline=self.clock()+timeout
        group_id=self._group(group_id)
        graph=self._discover(group_id);deadline=self._deadline
        if validate_scope is not None:
            if not callable(validate_scope):fail('invalid_scope_validator','Scope validation callback is invalid.')
            if validate_scope(group_id) is False:fail('scope_rejected','Fresh target mapping rejected this source scope.')
            if graph!=self._discover(group_id):fail('graph_changed','NiFi changed during mapping validation; no processors were stopped.')
        source_ids=[pid for pid,kind in graph['processors'].items() if kind in SOURCE_TYPES]
        for pid in source_ids:self._stop(pid)
        while True:
            quiet,states=self._quiet(group_id,source_ids)
            if quiet and all(state in ('STOPPED','DISABLED') for state in states.values()):break
            if self.clock()>=deadline:fail('drain_timeout','NiFi has not stopped and drained; target promotion remains blocked.')
            self.sleep(.25)
        for pid in graph['processors']:self._stop(pid)
        while True:
            quiet,states=self._quiet(group_id,graph['processors'])
            if quiet and all(state in ('STOPPED','DISABLED') for state in states.values()):break
            if self.clock()>=deadline:fail('stop_timeout','Not every processor is stopped; target promotion remains blocked.')
            self.sleep(.25)
        if graph!=self._discover(group_id):fail('graph_changed','NiFi changed during fencing; reassessment is required.')
        proof={'schema':'flowbridge/nifi-fence/v1','id':uuid.uuid4().hex,'endpoint_sha256':self.identity,'group_id':group_id,'graph':graph,'observed_at':self.clock(),'queued_flowfiles':0,'active_threads':0,'states':states,'cutover_ready':False,'requires_revalidation':True}
        fd=os.open(self.directory/(proof['id']+'.json'),os.O_CREAT|os.O_WRONLY|os.O_EXCL|os.O_NOFOLLOW,0o600)
        with os.fdopen(fd,'w') as output:json.dump(proof,output);output.flush();os.fsync(output.fileno())
        return proof

    def revalidate(self,proof_id,timeout=30,expected_group_id=None):
        if not isinstance(timeout,(int,float)) or not 0<timeout<=60:fail('invalid_timeout','Verification timeout must be at most 60 seconds.')
        self._deadline=self.clock()+timeout
        if not isinstance(proof_id,str) or len(proof_id)!=32 or any(c not in '0123456789abcdef' for c in proof_id):fail('invalid_proof','Invalid server proof identifier.')
        try:
            fd=os.open(self.directory/(proof_id+'.json'),os.O_RDONLY|os.O_NOFOLLOW)
            with os.fdopen(fd) as source:proof=json.load(source)
        except (OSError,ValueError):fail('missing_proof','Server fence proof is unavailable.')
        if proof.get('endpoint_sha256')!=self.identity:fail('wrong_endpoint','Proof belongs to another NiFi connection.')
        if expected_group_id is not None and self._group(expected_group_id)!=proof['group_id']:fail('wrong_group','Proof belongs to another process group.')
        graph=self._discover(proof['group_id'])
        if graph!=proof['graph']:fail('graph_changed','NiFi graph changed after fencing.')
        quiet,states=self._quiet(proof['group_id'],graph['processors'])
        if not quiet or any(state not in ('STOPPED','DISABLED') for state in states.values()):fail('source_restarted','Source is no longer stopped and drained.')
        return {'proof_id':proof_id,'group_id':proof['group_id'],'source_stopped_and_drained':True,'observed_at':self.clock(),'cutover_ready':False,'limitation':'An external operator can restart NiFi after this observation. Target reconciliation and ownership checks remain required.'}
