"""Version-aware NiFi JSON inventory without pretending every node can migrate.

NiFi 1.x/2.x flow definitions and JSON root groups are inventoried. Unknown
fields are retained in a redacted original when requested. No code, expression,
parameter provider, remote endpoint or controller service is executed.
"""
from __future__ import annotations

import copy
import json
import math
import re

MAX_COMPONENTS = 10000
MAX_DEPTH = 48
MAX_VALUES = 150000
MAX_TEXT = 5 * 1024 * 1024
_SECRET = re.compile(r'password|passwd|secret|token|credential|private.?key|jaas|authorization|api.?key|access.?key', re.I)
_PARAMETER = re.compile(r'(#+)\{([^{}]+)\}')
_URL_AUTH = re.compile(r'([A-Za-z][A-Za-z0-9+.-]*://)[^/@\s]+:[^/@\s]+@')
_REDACTED = '[REDACTED]'

# Classification is deliberately distinct from executable conversion support.
_PROCESSORS = {
    'InvokeHTTP': ('http_api', 'HTTP method, authentication, status relationships, request/response FlowFiles and scheduling need a reviewed HTTP task contract.'),
    'ListenHTTP': ('http_server', 'An inbound HTTP server is not equivalent to a scheduled API task.'),
    'HandleHttpRequest': ('http_server', 'Request/response correlation and the HTTP context service must be preserved.'),
    'HandleHttpResponse': ('http_server', 'Request/response correlation and response relationships need a runtime adapter.'),
    'GetHTTP': ('http_api', 'HTTP polling state, authentication and content semantics need a reviewed task contract.'),
    'UpdateAttribute': ('attribute_transform', 'FlowFile attributes and NiFi expression evaluation need explicit equivalent mappings.'),
    'EvaluateJsonPath': ('json_extract', 'JSONPath behavior, destinations and unmatched/failure relationships need explicit equivalent mappings.'),
    'RouteOnAttribute': ('conditional_route', 'Every named relationship, expression and default/unmatched path must be preserved.'),
    'ReplaceText': ('text_transform', 'Regex engine, replacement evaluation, character encoding and buffering semantics need explicit equivalent mappings.'),
    'JoltTransformJSON': ('json_transform', 'Jolt specification/version and failure handling require an equivalent transformation engine.'),
    'QueryRecord': ('record_query', 'Record schemas, readers/writers and each SQL relationship require explicit equivalent mappings.'),
    'ConvertRecord': ('record_transform', 'Record reader/writer services and schema behavior must be migrated together.'),
    'SplitJson': ('json_split', 'Fan-out, fragment attributes, limits and failure relationships require a reviewed mapping.'),
    'MergeContent': ('merge', 'Stateful binning, ordering and completion rules do not map to plain DAG dependencies.'),
    'ConsumeKafka': ('kafka_source', 'Modern Kafka connection service and record/FlowFile processing strategy require service-aware mapping.'),
    'PublishKafka': ('kafka_sink', 'Modern Kafka connection service and message/record publication settings require service-aware mapping.'),
    'ConsumeKafka_2_6': ('kafka_source', 'Candidate only for the separately validated direct byte-flow Kafka subset.'),
    'PublishKafka_2_6': ('kafka_sink', 'Candidate only for the separately validated direct byte-flow Kafka subset.'),
    'ExecuteScript': ('custom_code', 'User code is inventoried but never executed or automatically translated.'),
    'ExecuteStreamCommand': ('external_command', 'External commands are inventoried but never executed or automatically translated.'),
}


class _Invalid(ValueError):
    pass


def _validate_json(value):
    count, text_count = 0, 0
    pending = [(value, 0)]
    while pending:
        current, depth = pending.pop()
        count += 1
        if count > MAX_VALUES or depth > MAX_DEPTH:
            raise _Invalid('The document exceeds the inventory size or nesting limit.')
        if isinstance(current, dict):
            for key, item in current.items():
                if not isinstance(key, str):
                    raise _Invalid('JSON object keys must be strings.')
                text_count += len(key)
                pending.append((item, depth + 1))
        elif isinstance(current, list):
            pending.extend((item, depth+1) for item in current)
        elif isinstance(current, str):
            text_count += len(current)
        elif isinstance(current, float):
            if not math.isfinite(current):
                raise _Invalid('Non-finite numbers are not valid NiFi JSON.')
        elif current is not None and not isinstance(current, (int, bool)):
            raise _Invalid('The input must contain only JSON values.')
        if text_count > MAX_TEXT:
            raise _Invalid('The document exceeds the inventory text limit.')


def _public_credential_setting(key, value):
    if key in ('Use Default Credentials', 'Use Anonymous Credentials'):
        return type(value) is bool or isinstance(value, str) and value in ('true', 'false')
    if key == 'AWS Credentials Provider Service':
        return isinstance(value, str) and re.fullmatch(r'[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}', value) is not None
    return False


def _sanitize(value, redactions, path='$'):
    if isinstance(value, dict):
        descriptors=value.get('propertyDescriptors', value.get('descriptors', {}))
        sensitive_properties={name for name, descriptor in descriptors.items()
                              if isinstance(descriptors,dict) and isinstance(descriptor,dict) and descriptor.get('sensitive') is True} if isinstance(descriptors,dict) else set()
        sensitive_record=value.get('sensitive') is True or (isinstance(value.get('name'),str) and bool(_SECRET.search(value['name'])))
        result={}
        for key, item in value.items():
            child=path+'.'+key
            if key=='original_tree' and '_flowbridge_xml' in path:
                result[key]=_REDACTED;redactions.append('_opaque:'+child)
            elif item not in (None,'',False) and ((bool(_SECRET.search(key)) and not _public_credential_setting(key,item) and not path.endswith(('.propertyDescriptors','.descriptors'))) or (key=='value' and sensitive_record)):
                result[key]=_REDACTED;redactions.append(child)
            elif key=='properties' and isinstance(item,dict):
                result[key]={}
                for name,prop in item.items():
                    if prop not in (None,'') and (name in sensitive_properties or _SECRET.search(name) and not _public_credential_setting(name,prop)):
                        result[key][name]=_REDACTED;redactions.append(child+'.'+name)
                    else:result[key][name]=_sanitize(prop,redactions,child+'.'+name)
            else:result[key]=_sanitize(item,redactions,child)
        return result
    if isinstance(value,list):
        return [_sanitize(item,redactions,path+'['+str(i)+']') for i,item in enumerate(value)]
    if isinstance(value,str) and (_URL_AUTH.search(value) or re.search(r'[?&](?:access[_-]?token|api[_-]?key|token|password|secret|signature|key)=[^&#\s]+',value,re.I) or value.startswith('enc{') or '-----BEGIN PRIVATE KEY-----' in value):
        redactions.append(path)
        return _REDACTED
    return value


def _capability(type_name):
    short=type_name.rsplit('.',1)[-1]
    category,reason=_PROCESSORS.get(short,('unknown_processor','No semantic mapping is implemented for this processor; inventory is preserved for review.'))
    candidate=short in ('ConsumeKafka_2_6','PublishKafka_2_6')
    return {'status':'translation_candidate' if candidate else 'assessment_only',
            'category':category,'targets':['nifi','seatunnel','camel-k','kafka'] if candidate else [],
            'reason':reason,'automatic_conversion':False}


def analyze_graph(document, nifi_version='auto', include_original=False):
    """Return redacted structural inventory and per-component diagnostics.

    `ok` means structurally valid inventory, NOT executable migration. Unsupported
    semantic mappings remain warnings and per-component assessment-only status.
    Missing endpoints, cycles, ambiguous identities, unresolved explicit service
    references and secret material are errors. `original`, when requested, is a
    deep redacted copy: unknown fields survive; credentials never appear there.
    """
    graph={name:[] for name in ('groups','processors','connections','controller_services','parameter_contexts','parameters','ports','funnels','remote_process_groups','parameter_providers')}
    diagnostics=[]
    version={'requested':nifi_version if isinstance(nifi_version,str) and re.fullmatch(r'auto|[0-9]+(?:\.[0-9A-Za-z-]+)*',nifi_version) else 'invalid',
             'detected_bundle_majors':[],'declared_product_version':None,'status':'unverified','supported_inventory_families':['1.x','2.x']}
    result={'ok':False,'source_version':version,'graph':graph,'diagnostics':diagnostics,
            'capabilities':{'inventory':False,'automatic_conversion':False,'runtime_validated':False}}
    def diag(severity,code,message,component=None,path=None):
        entry={'severity':severity,'code':code,'message':message}
        if component is not None:entry['component_id']=component
        if path is not None:entry['path']=path
        diagnostics.append(entry)
    try:
        if not isinstance(document,dict):raise _Invalid('NiFi graph inventory requires a JSON object.')
        _validate_json(document)
        redactions=[]
        safe=_sanitize(document,redactions)
        if include_original:
            result['original']=copy.deepcopy(safe)
            result['original_redacted']=bool(redactions)
        if any(not item.startswith('_opaque:') for item in redactions):
            diag('error','secret_material','Credential or sensitive parameter values were redacted. Supply credentials separately during an authorized deployment.')
        xml_metadata=safe.get('_flowbridge_xml')
        if isinstance(xml_metadata,dict):
            if xml_metadata.get('export_blocked'):
                diag('error','xml_export_blocked','The XML importer found unmapped structures; inventory is available but automatic export is blocked.')
            elif xml_metadata.get('review_required'):
                diag('warning','xml_review_required','The normalized legacy XML flow requires explicit migration review.')
        envelope=safe
        if isinstance(envelope.get('flowSnapshot'),dict):envelope=envelope['flowSnapshot']
        if isinstance(envelope.get('versionedFlowSnapshot'),dict):envelope=envelope['versionedFlowSnapshot']
        if 'flowContents' in envelope:root=envelope['flowContents'];root_path='$.flowContents'
        elif 'rootGroup' in envelope:root=envelope['rootGroup'];root_path='$.rootGroup'
        elif 'processors' in envelope or 'processGroups' in envelope:root=envelope;root_path='$'
        else:raise _Invalid('No NiFi flowContents, rootGroup or process-group JSON structure was found.')
        if not isinstance(root,dict):raise _Invalid('The NiFi root process group must be an object.')
        declared=safe.get('nifiVersion',envelope.get('nifiVersion'))
        if declared is not None:
            if isinstance(declared,str) and re.fullmatch(r'[0-9]+(?:\.[0-9A-Za-z-]+)*',declared):version['declared_product_version']=declared
            else:diag('error','invalid_version','The declared NiFi product version is malformed.')
        explicit=nifi_version if nifi_version!='auto' else version['declared_product_version']
        if explicit is not None:
            major=explicit.split('.',1)[0] if isinstance(explicit,str) else None
            if major not in ('1','2'):
                version['status']='unsupported'
                diag('error','unsupported_version','Only NiFi 1.x and 2.x JSON inventory families are recognized; future-version compatibility is not assumed.')
            else:
                version['status']='declared_family'
                if nifi_version!='auto' and declared and declared.split('.',1)[0]!=major:
                    diag('error','version_conflict','The selected NiFi family conflicts with the document product version.')
        else:diag('warning','version_unverified','Flow encoding versions and individual component bundle versions do not establish the NiFi runtime version.')
        identities={};node_ids=set();group_by_id={};raw_components=[];observed_majors=set()
        def identify(raw,kind,path,group_id=None):
            ident=raw.get('identifier',raw.get('id'))
            synthetic=not isinstance(ident,str) or not ident
            if synthetic:
                ident='@'+path
                diag('warning' if kind=='group' else 'error','missing_identifier','A component has no identifier; a local inventory identifier was assigned.',ident,path)
            if ident in identities:
                diag('error','duplicate_identifier','Multiple components share an identifier; connections are ambiguous.',ident,path)
            else:identities[ident]=kind
            name=raw.get('name')
            result={'id':ident,'name':name if isinstance(name,str) else kind,'kind':kind,'group_id':group_id,'path':path}
            if synthetic:result['synthetic_id']=True
            if len(identities)>MAX_COMPONENTS:raise _Invalid('The document exceeds the component inventory limit.')
            return result
        def collection(raw,key,path):
            value=raw.get(key,[])
            if value is None:return []
            if not isinstance(value,list):
                diag('error','invalid_collection','A component collection must be a JSON array.',path=path+'.'+key);return []
            output=[]
            for i,item in enumerate(value):
                location=path+'.'+key+'['+str(i)+']'
                if not isinstance(item,dict):diag('error','invalid_component','A component entry must be an object.',path=location)
                else:output.append((item,location))
            return output
        def properties(raw,component):
            config=raw.get('config',{})
            if not isinstance(config,dict):
                diag('error','invalid_configuration','Processor configuration must be an object.',component['id']);config={}
            values=raw.get('properties',config.get('properties',{}))
            if not isinstance(values,dict):
                diag('error','invalid_properties','Component properties must be an object.',component['id']);values={}
            if 'properties' in raw and 'properties' in config and raw['properties']!=config['properties']:
                diag('error','ambiguous_properties','Two property representations conflict; both are retained in the original document.',component['id'])
            component['properties']=copy.deepcopy(values)
            component['configuration']=copy.deepcopy(config)
            descriptors=raw.get('propertyDescriptors',config.get('descriptors',{}))
            component['property_descriptors']=copy.deepcopy(descriptors) if isinstance(descriptors,dict) else {}
            raw_components.append((component,raw))
        def service(raw,path,group_id,external=False):
            item=identify(raw,'controller_service',path,group_id)
            item.update({'type':raw.get('type',''),'external':external,'bundle':copy.deepcopy(raw.get('bundle',{}))})
            properties(raw,item);graph['controller_services'].append(item)
            if external:diag('warning','external_service','External controller service requires an explicitly supplied implementation and configuration.',item['id'])
            return item
        pending=[(root,None,root_path)]
        while pending:
            raw,parent,path=pending.pop()
            group=identify(raw,'group',path,parent);gid=group['id']
            group['parent_id']=parent
            group['parameter_context']=raw.get('parameterContextName',raw.get('parameterContextIdentifier',raw.get('parameterContextId')))
            group['variables']=copy.deepcopy(raw.get('variables',{}))
            group['flowFileConcurrency']=copy.deepcopy(raw.get('flowFileConcurrency'))
            group['flowFileOutboundPolicy']=copy.deepcopy(raw.get('flowFileOutboundPolicy'))
            group['raw']=copy.deepcopy({key:value for key,value in raw.items() if key not in ('processors','processGroups','connections','controllerServices','inputPorts','outputPorts','funnels','remoteProcessGroups')})
            if group['flowFileConcurrency'] not in (None,'UNBOUNDED') or group['flowFileOutboundPolicy'] not in (None,'STREAM_WHEN_AVAILABLE'):
                diag('warning','group_execution_mapping_required','Process-group concurrency and batch-output policies require explicit target semantics.',gid)
            graph['groups'].append(group);group_by_id[gid]=group
            for node,node_path in collection(raw,'processors',path):
                item=identify(node,'processor',node_path,gid)
                type_name=node.get('type','')
                if not isinstance(type_name,str):type_name='';diag('error','invalid_processor_type','Processor type must be a string.',item['id'])
                item.update({'type':type_name,'bundle':copy.deepcopy(node.get('bundle',{})),'capability':_capability(type_name),
                             'scheduling':{key:copy.deepcopy(node.get(key,node.get('config',{}).get(key) if isinstance(node.get('config'),dict) else None)) for key in ('schedulingStrategy','schedulingPeriod','executionNode','concurrentlySchedulableTaskCount')},
                             'raw':copy.deepcopy(node),
                             'auto_terminated_relationships':copy.deepcopy(node.get('autoTerminatedRelationships',node.get('config',{}).get('autoTerminatedRelationships',[]) if isinstance(node.get('config'),dict) else []))})
                item['annotation_data']=copy.deepcopy(node.get('annotationData',node.get('config',{}).get('annotationData') if isinstance(node.get('config'),dict) else None))
                item['declared_relationships']=copy.deepcopy(node.get('relationships',[]))
                properties(node,item);graph['processors'].append(item);node_ids.add(item['id'])
                if item['annotation_data']:diag('warning','annotation_mapping_required','Advanced processor rules or annotations require explicit translation.',item['id'])
                if item['scheduling'].get('schedulingStrategy') not in (None,'TIMER_DRIVEN') or item['scheduling'].get('executionNode') not in (None,'ALL'):
                    diag('warning','scheduling_mapping_required','NiFi scheduling or cluster execution settings require explicit target scheduling semantics.',item['id'])
                diag('warning','component_mapping_required',item['capability']['reason'],item['id'])
                bundle=item['bundle']
                if isinstance(bundle,dict) and bundle.get('group')=='org.apache.nifi' and isinstance(bundle.get('version'),str):
                    major=bundle['version'].split('.',1)[0]
                    if major.isdigit():observed_majors.add(major)
            for node,node_path in collection(raw,'controllerServices',path):service(node,node_path,gid)
            for key,kind in (('inputPorts','input_port'),('outputPorts','output_port'),('funnels','funnel')):
                for node,node_path in collection(raw,key,path):
                    item=identify(node,kind,node_path,gid);item['configuration']=copy.deepcopy(node)
                    graph['funnels' if key=='funnels' else 'ports'].append(item);node_ids.add(item['id'])
                    diag('warning','boundary_mapping_required','Ports and funnels retain routing semantics and require a target-specific mapping.',item['id'])
            for remote,remote_path in collection(raw,'remoteProcessGroups',path):
                item=identify(remote,'remote_process_group',remote_path,gid);item['configuration']=copy.deepcopy(remote);graph['remote_process_groups'].append(item)
                diag('warning','remote_group_unsupported','Remote process groups require Site-to-Site protocol and endpoint mapping; no automatic translation is implemented.',item['id'])
                contents=remote.get('contents',remote)
                if isinstance(contents,dict):
                    for key,kind in (('inputPorts','remote_input_port'),('outputPorts','remote_output_port')):
                        for port,port_path in collection(contents,key,remote_path):
                            node=identify(port,kind,port_path,item['id']);node['configuration']=copy.deepcopy(port);graph['ports'].append(node);node_ids.add(node['id'])
            for edge,edge_path in collection(raw,'connections',path):
                item=identify(edge,'connection',edge_path,gid)
                def endpoint(value):return value.get('id',value.get('identifier')) if isinstance(value,dict) else value
                relationships=edge.get('selectedRelationships',edge.get('relationships',[]))
                if not isinstance(relationships,list) or any(not isinstance(v,str) for v in relationships):
                    diag('error','invalid_relationships','Connection relationships must be a list of names.',item['id']);relationships=[]
                item.update({'source_id':endpoint(edge.get('source',edge.get('sourceId'))),'target_id':endpoint(edge.get('destination',edge.get('destinationId'))),'relationships':list(relationships),'configuration':copy.deepcopy(edge)})
                graph['connections'].append(item)
                if any(v!='success' for v in relationships):diag('warning','relationship_mapping_required','Named, failure and retry relationships must be preserved explicitly.',item['id'])
            for child,child_path in reversed(collection(raw,'processGroups',path)):pending.append((child,gid,child_path))
        # Full-instance exports may also carry controller-level services.
        if envelope is not root:
            for node,node_path in collection(envelope,'controllerServices','$'):
                service(node,node_path,None,True)
        # Versioned-flow snapshots may carry external services as ID-keyed maps.
        externals=envelope.get('externalControllerServices',{})
        if isinstance(externals,dict):
            for key,value in externals.items():
                if not isinstance(value,dict):diag('error','invalid_external_service','External service metadata must be an object.');continue
                raw=dict(value);raw.setdefault('identifier',key);service(raw,'$.externalControllerServices.'+key,None,True)
        elif externals:diag('error','invalid_external_service','External services must be an ID-keyed object.')
        contexts=envelope.get('parameterContexts',safe.get('parameterContexts',{}))
        if isinstance(contexts,dict):context_items=list(contexts.items())
        elif isinstance(contexts,list):context_items=[(str(i),v) for i,v in enumerate(contexts)]
        else:context_items=[];diag('error','invalid_parameter_contexts','Parameter contexts must be an object or array.')
        contexts_by_key={}
        for key,context in context_items:
            if not isinstance(context,dict):diag('error','invalid_parameter_context','Parameter context must be an object.');continue
            item={'id':context.get('identifier',context.get('id',key)),'name':context.get('name',key),'inherited_contexts':copy.deepcopy(context.get('inheritedParameterContexts',[])),'configuration':copy.deepcopy(context)}
            graph['parameter_contexts'].append(item)
            contexts_by_key[item['id']]=item;contexts_by_key[item['name']]=item
            item['parameter_names']=[]
            for param,param_path in collection(context,'parameters','$.parameterContexts.'+key):
                value=param.get('parameter',param)
                if not isinstance(value,dict):diag('error','invalid_parameter','Parameter must be an object.',path=param_path);continue
                name=value.get('name')
                if not isinstance(name,str) or not name:diag('error','invalid_parameter','Parameter name is required.',path=param_path);continue
                if name in item['parameter_names']:diag('error','duplicate_parameter','A parameter name occurs more than once in its context.',path=param_path)
                item['parameter_names'].append(name)
                graph['parameters'].append({'context_id':item['id'],'name':name,'sensitive':value.get('sensitive') is True,'value':copy.deepcopy(value.get('value')),'path':param_path})
        providers=envelope.get('parameterProviders',{})
        if isinstance(providers,dict):graph['parameter_providers']=[{'id':key,'configuration':copy.deepcopy(value)} for key,value in providers.items()]
        elif isinstance(providers,list):graph['parameter_providers']=copy.deepcopy(providers)
        elif providers:diag('error','invalid_parameter_providers','Parameter providers must be an object or array.')
        if graph['parameter_providers']:diag('warning','parameter_provider_unsupported','Parameter providers are inventoried but are not contacted or executed.')
        # NiFi explicitly does not inherit an unset context from the parent
        # process group. Parameter-context composition is a separate feature.
        def context_for(group_id):
            group=group_by_id.get(group_id)
            return contexts_by_key.get(group['parameter_context']) if group and isinstance(group['parameter_context'],str) else None
        for group in graph['groups']:
            if group['parameter_context'] is not None and group['parameter_context'] not in contexts_by_key:
                diag('error','unresolved_parameter_context','A process group references a missing parameter context.',group['id'])
        services={item['id']:item for item in graph['controller_services']}
        for item,raw in raw_components:
            item['service_references']=[];item['parameter_references']=[];item['expression_properties']=[]
            descriptors=item['property_descriptors']
            context=context_for(item['group_id'])
            for key,value in item['properties'].items():
                descriptor=descriptors.get(key,{})
                service_hint=isinstance(descriptor,dict) and any(descriptor.get(k) for k in ('identifiesControllerService','controllerServiceDefinition','controllerServiceType'))
                service_hint=service_hint or key in ('Kafka Connection Service','Record Reader','Record Writer','SSL Context Service','HTTP Context Map')
                if isinstance(value,str) and value and value!=_REDACTED and (service_hint or value in services):
                    item['service_references'].append({'property':key,'service_id':value})
                    if value not in services:diag('error','unresolved_controller_service','A controller-service property references a service absent from this export.',item['id'])
                    else:
                        visible={None};cursor=item['group_id']
                        while cursor in group_by_id and cursor not in visible:
                            visible.add(cursor);cursor=group_by_id[cursor]['parent_id']
                        if services[value]['group_id'] not in visible:
                            diag('error','controller_service_scope','A referenced controller service is outside this component process-group scope.',item['id'])
                if isinstance(value,str):
                    if '${' in value:
                        item['expression_properties'].append(key)
                        diag('warning','expression_mapping_required','NiFi Expression Language is preserved but is not evaluated or assumed portable.',item['id'])
                    for hashes,parameter in _PARAMETER.findall(value):
                        if len(hashes)%2==0:continue
                        item['parameter_references'].append(parameter)
                        if context is None or parameter not in context['parameter_names']:
                            severity='warning' if context and context['inherited_contexts'] else 'error'
                            diag(severity,'unresolved_parameter','A parameter reference is unresolved in the supplied direct context; inherited contexts require explicit resolution.',item['id'])
        adjacency={ident:set() for ident in node_ids};reverse={ident:set() for ident in node_ids}
        for edge in graph['connections']:
            left,right=edge['source_id'],edge['target_id']
            if not isinstance(left,str) or not isinstance(right,str) or left not in node_ids or right not in node_ids:
                diag('error','dangling_connection','A connection endpoint does not identify an inventoried processor, port or funnel.',edge['id']);continue
            adjacency[left].add(right);reverse[right].add(left)
        # Iterative Kosaraju avoids recursion failures on long valid pipelines.
        visited=set();order=[]
        for start in sorted(node_ids):
            if start in visited:continue
            stack=[(start,False)]
            while stack:
                node,expanded=stack.pop()
                if expanded:order.append(node);continue
                if node in visited:continue
                visited.add(node);stack.append((node,True))
                stack.extend((child,False) for child in sorted(adjacency[node],reverse=True) if child not in visited)
        assigned=set();cycles=[]
        for start in reversed(order):
            if start in assigned:continue
            component=[];stack=[start];assigned.add(start)
            while stack:
                node=stack.pop();component.append(node)
                for parent in reverse[node]:
                    if parent not in assigned:assigned.add(parent);stack.append(parent)
            if len(component)>1 or start in adjacency[start]:
                cycles.append(sorted(component));diag('error','cycle_requires_semantics','A flow cycle requires explicit retry/state semantics and cannot become a plain acyclic task graph.',start)
        graph['cycles']=cycles
        graph['counts']={key:len(value) for key,value in graph.items() if isinstance(value,list)}
        version['detected_bundle_majors']=sorted(observed_majors)
        if explicit is not None and isinstance(explicit,str) and observed_majors and observed_majors-{explicit.split('.',1)[0]}:
            diag('warning','bundle_family_differs','Component bundle families differ from the declared runtime family; no component upgrade or compatibility is implied.')
        if len(observed_majors)>1:diag('warning','mixed_bundle_families','Multiple Apache NiFi bundle major versions are present; runtime compatibility requires verification.')
        if observed_majors-{'1','2'}:diag('error','unsupported_bundle_family','An Apache NiFi bundle belongs to an unrecognized future version family.')
        result['capabilities'].update({'inventory':True,'classified_processors':sum(p['capability']['category']!='unknown_processor' for p in graph['processors']),
                                      'assessment_only_processors':sum(p['capability']['status']=='assessment_only' for p in graph['processors']),
                                      'translation_candidates':sum(p['capability']['status']=='translation_candidate' for p in graph['processors'])})
        result['ok']=not any(d['severity']=='error' for d in diagnostics)
    except _Invalid as exc:
        diag('error','invalid_document',str(exc))
    except (KeyError,TypeError,ValueError,AttributeError,RecursionError):
        diag('error','invalid_document','The document contains malformed NiFi structures; partial inventory is retained for inspection.')
    return result
