"""Discover independently executable S3 lanes in a native NiFi hierarchy.

Unmapped components remain visible and block executable export. Shadow remapping
is output isolation only; it does not constitute a blue/green promotion.
"""
import copy
import json
import uuid
from pathlib import Path
from .graph import analyze_graph
from .nifi_s3 import analyze_nifi_s3, validate_profile, FLEET_SCHEMA, MigrationError, need


def discover_fleet(document):
    assessment=analyze_graph(document)
    result={'report':{'ok':False,'errors':[],'warnings':[]},'inventory':[],'assessment':assessment,'profile':None,'mapped':0,'unmapped':0}
    errors=result['report']['errors']
    def error(code):
        if not any(e['code']==code for e in errors):errors.append({'code':code,'message':'Fleet export is blocked by unsupported or unsafe native components; inspect pipeline inventory and graph assessment.'})
    if not isinstance(document,dict) or not isinstance(document.get('flowContents'),dict):
        error('native_flow_contents_required');return result
    if any(d['severity']=='error' for d in assessment.get('diagnostics',[])):
        error('invalid_native_graph')
    root=document['flowContents'];services=[];candidates=[]
    # The graph analyzer bounds recursion and input sizes before traversal.
    if errors:return result
    def visit(group,path):
        ident=group.get('identifier',group.get('id'));label=group.get('name') or ident or '(unnamed)';path=path+[str(label)]
        for field in ('inputPorts','outputPorts','funnels','remoteProcessGroups','parameterProviders'):
            if group.get(field):error('unsupported_'+field)
        if group.get('parameterContextName') or group.get('variables'):error('unsupported_group_parameters')
        if group.get('flowFileConcurrency','UNBOUNDED')!='UNBOUNDED' or group.get('flowFileOutboundPolicy','STREAM_WHEN_AVAILABLE')!='STREAM_WHEN_AVAILABLE' or group.get('executionEngine','INHERITED') not in ('INHERITED','STANDARD'):
            error('unsupported_group_policy')
        services.extend(copy.deepcopy(group.get('controllerServices') or []))
        processors=group.get('processors') or []
        if processors:
            item={'id':ident,'path':path,'processor_ids':[p.get('identifier',p.get('id')) for p in processors],'mapped':False,'reason':None}
            result['inventory'].append(item)
            lane=copy.deepcopy(group);lane['processGroups']=[];lane['controllerServices']=[]
            candidates.append((lane,item))
        elif group.get('connections'):
            error('container_connections_unsupported')
        for child in group.get('processGroups') or []:visit(child,path)
    visit(root,[])
    if not 1<=len(candidates)<=256:error('unsupported_lane_count')
    envelope={k:copy.deepcopy(v) for k,v in document.items() if k!='flowContents'}
    flat_root={k:copy.deepcopy(v) for k,v in root.items() if k not in ('processors','connections','processGroups','controllerServices')}
    # Hoisting is used only after original graph scope validation. IDs and all
    # settings survive; no controller implementation is silently substituted.
    flat_root['identifier']=str(uuid.uuid5(uuid.NAMESPACE_URL,'flowbridge-fleet-container/'+str(root.get('identifier',root.get('id','root')))))
    flat_root.pop('id',None)
    for service in services:service['groupIdentifier']=flat_root['identifier']
    flat_root.update(processors=[],connections=[],controllerServices=services,processGroups=[])
    envelope['flowContents']=flat_root
    profiles=[]
    for lane,item in candidates:
        lane['groupIdentifier']=flat_root['identifier']
        sample=copy.deepcopy(envelope);sample['flowContents']['processGroups']=[lane]
        checked=analyze_nifi_s3(sample,_fleet=True)
        if checked['report']['ok']:
            item['mapped']=True;profiles.extend(checked['profile']['lanes'])
        else:
            item['reason']=checked['report']['errors'][0]['code'];error(item['reason'])
    result['mapped']=sum(item['mapped'] for item in result['inventory']);result['unmapped']=len(result['inventory'])-result['mapped']
    if not errors:
        profile={'schema':FLEET_SCHEMA,'lanes':profiles,'delivery':'at_least_once','maximum_object_bytes':64*1024*1024}
        try:validate_profile(profile)
        except (MigrationError,KeyError,TypeError,ValueError):error('fleet_location_overlap_or_invalid_profile')
        else:result['profile']=profile;result['report']['ok']=True
    result['report']['warnings']=[{'code':'bounded_fleet','message':'Only independent NiFi 2.12 ListS3→FetchS3Object→literal UpdateAttribute→PutS3Object lanes are executable. Discovery covers the complete graph; unsupported components block export.'},{'code':'state_and_cutover','message':'Native NiFi state is not translated. Production cutover requires stopping and draining NiFi plus verified baseline or explicit backfill approval. Isolated shadow copying does not authorize production promotion.'}]
    return result


def remap_fleet(profile,destination_remap):
    validate_profile(profile)
    need(profile['schema']==FLEET_SCHEMA,'fleet_profile_required')
    need(isinstance(destination_remap,dict) and set(destination_remap)=={lane['id'] for lane in profile['lanes']},'complete_shadow_remap_required')
    protected={lane[side]['bucket'] for lane in profile['lanes'] for side in ('source','destination')}
    remapped=copy.deepcopy(profile)
    for lane in remapped['lanes']:
        destination=destination_remap[lane['id']]
        need(isinstance(destination,dict) and set(destination)=={'endpoint','region','path_style_access','bucket'},'invalid_shadow_destination')
        need(isinstance(destination['bucket'],str),'invalid_shadow_destination')
        need(destination['bucket'] not in protected,'shadow_destination_overlaps_production')
        lane['destination']=copy.deepcopy(destination)
    validate_profile(remapped)
    return remapped


def export_fleet(document,destination_remap=None):
    result=discover_fleet(document);result['files']={}
    if not result['report']['ok']:return result
    if destination_remap is not None:
        try:result['profile']=remap_fleet(result['profile'],destination_remap)
        except (MigrationError,KeyError,TypeError,ValueError):
            result['report']['ok']=False;result['report']['errors'].append({'code':'invalid_shadow_remap','message':'Every lane needs a unique isolated shadow destination, disjoint from production and source buckets.'});result['profile']=None;return result
    root=Path(__file__).parent.parent
    result['files']={'s3-fleet-profile.json':json.dumps(result['profile'],indent=2)+'\n','pipeline-inventory.json':json.dumps(result['inventory'],indent=2)+'\n','flowbridge/nifi_s3.py':root.joinpath('flowbridge/nifi_s3.py').read_text(),'flowbridge/__init__.py':'','requirements.txt':root.joinpath('requirements-media.txt').read_text(),'LICENSE':root.joinpath('LICENSE').read_text(),'README.md':'# Bounded native S3 fleet\n\nEach mapped lane is an independent latest-object S3 workflow. Run the packaged `flowbridge.nifi_s3` module with --profile s3-fleet-profile.json --state /persistent/fleet.db --watch. Production first requires --source-stopped after actual NiFi stop/drain and baseline reconciliation; --accept-backfill explicitly permits copying missing/different objects. Shadow outputs must be fully remapped away from production and use a separate ledger initialized by establish_shadow(production_profile). No automatic promotion, rollback, state transfer or support for arbitrary NiFi processors is claimed.\n'}
    return result
