"""Strict NiFi graph to finite Airflow 3 TaskFlow mapping.

Airflow authoring contract: https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html
Processor contracts: https://nifi.apache.org/components/org.apache.nifi.processors.standard.InvokeHTTP/
https://nifi.apache.org/components/org.apache.nifi.processors.standard.ReplaceText/
https://nifi.apache.org/components/org.apache.nifi.processors.attributes.UpdateAttribute/
"""
import hashlib
import json
import re
from pathlib import Path
from urllib.parse import urlsplit

from .airflow_runtime import MAX_CONTENT, MAX_ENVELOPE


class Unsupported(ValueError):
    pass


def _literal(value):
    if not isinstance(value, str) or any(marker in value for marker in ('${', '#{', '{{', '{%', '{#')) or len(value.encode()) > MAX_CONTENT:
        raise Unsupported('Only bounded literal strings are supported; expressions and parameters require a mapping.')
    return value


def _mapping(processor):
    fqcn=processor.get('type','')
    official={'org.apache.nifi.processors.standard.InvokeHTTP':'InvokeHTTP','org.apache.nifi.processors.standard.ReplaceText':'ReplaceText','org.apache.nifi.processors.attributes.UpdateAttribute':'UpdateAttribute'}
    if fqcn not in official: raise Unsupported('Only exact official NiFi processor types have executable mappings.')
    kind=official[fqcn]
    props={k:v for k,v in processor.get('properties',{}).items() if v is not None and v != ''}
    if any(processor.get(key) or processor.get('configuration',{}).get(key) for key in ('annotationData','annotation_data')):
        raise Unsupported('Advanced processor rules are not mapped.')
    for key,value in props.items():
        _literal(key); _literal(value)
        if re.search(r'password|secret|credential|authorization|token',key,re.I):
            raise Unsupported('Credentials must not be embedded in generated DAGs.')
    if kind=='InvokeHTTP':
        aliases={'Remote URL':'HTTP URL','Follow Redirects':'HTTP Redirects Enabled'}
        normalized={}
        for key,value in props.items():
            key=aliases.get(key,key)
            if key in normalized: raise Unsupported('Duplicate HTTP configuration aliases are ambiguous.')
            normalized[key]=value
        props=normalized
        allowed={'HTTP URL','HTTP Method','HTTP Redirects Enabled','Accept','Response Body Ignored','Response Generation Required','HTTP/2 Disabled','Request Body Enabled','Request Content-Encoding'}
        if set(props)-allowed: raise Unsupported('HTTP headers, timeouts, authentication, proxy or other unmapped settings require an explicit mapping.')
        if props.get('Request Body Enabled','false').lower()!='false': raise Unsupported('GET request bodies are not mapped; disable Request Body Enabled explicitly if present.')
        if props.get('HTTP Method','GET')!='GET': raise Unsupported('Only bounded HTTPS GET ingestion is supported.')
        if props.get('HTTP Redirects Enabled','true').lower()!='false': raise Unsupported('HTTP redirects must be explicitly disabled.')
        if props.get('Response Body Ignored','false').lower()!='false' or props.get('Response Generation Required','false').lower()!='false': raise Unsupported('Only the normal successful HTTP response body is mapped.')
        if props.get('HTTP/2 Disabled','true').lower()!='true' or props.get('Request Content-Encoding','DISABLED')!='DISABLED': raise Unsupported('Only HTTP/1.1 without request compression is mapped.')
        url=props.get('HTTP URL',''); parsed=urlsplit(url)
        if parsed.scheme!='https' or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise Unsupported('Use a literal HTTPS URL without embedded credentials, query or fragment.')
        return {'operation':'http_get','url':url,'accept':props.get('Accept','*/*')}
    if kind=='UpdateAttribute':
        fixed={'Store State','Delete Attributes Expression','Stateful Variables Initial Value','canonical-value-lookup-cache-size'}
        if props.get('Store State','Do not store state')!='Do not store state': raise Unsupported('Stateful attributes are not mapped.')
        if props.get('Delete Attributes Expression') or props.get('Stateful Variables Initial Value'): raise Unsupported('Attribute deletion and state initialization are not mapped.')
        attrs={k:v for k,v in props.items() if k not in fixed}
        if 'uuid' in attrs: raise Unsupported('FlowFile identity changes are not mapped.')
        if len(json.dumps(attrs).encode())>8192: raise Unsupported('Literal attributes exceed the 8 KiB limit.')
        return {'operation':'update_attributes','attributes':attrs}
    if kind=='ReplaceText':
        allowed={'Replacement Strategy','Replacement Value','Search Value','Character Set','Evaluation Mode','Maximum Buffer Size','Line-by-Line Evaluation Mode'}
        if set(props)-allowed: raise Unsupported('ReplaceText contains unmapped settings.')
        if props.get('Character Set','UTF-8').upper()!='UTF-8' or props.get('Evaluation Mode','Line-by-Line').lower()!='entire text': raise Unsupported('ReplaceText requires UTF-8 and Entire text mode.')
        strategy=props.get('Replacement Strategy','Regex Replace')
        if strategy not in ('Literal Replace','Append','Prepend','Always Replace'): raise Unsupported('Regex and variable substitution are not mapped.')
        if 'Replacement Value' not in props: raise Unsupported('An explicit literal replacement value is required.')
        if strategy=='Literal Replace' and not props.get('Search Value'): raise Unsupported('Literal replacement requires a nonempty search value.')
        if props.get('Maximum Buffer Size','1 MB') not in ('1 MB','1 mb','1048576 B'): raise Unsupported('Custom buffer limits require an explicit mapping.')
        return {'operation':'replace_text','strategy':strategy,'replacement':props['Replacement Value'],'search':props.get('Search Value','')}
    raise Unsupported('No executable Airflow mapping exists for this processor; streaming and unmapped processors are blocked.')


def export_airflow(analysis, batch_contract=None):
    errors=[]; warnings=[{'code':'airflow.batch_semantics','message':'One manual bounded batch per DAG run replaces NiFi scheduling. Airflow retries are disabled; failure routes, provenance, queues, metadata parity and streaming behavior are not preserved.'}, {'code':'airflow.xcom','message':'HTTP response content and attributes enter the Airflow XCom backend (16 KiB content / 32 KiB envelope). Review data sensitivity, access and retention before deployment.'}]
    def error(code,message,ident=None):
        item={'code':code,'message':message}
        if ident is not None:item['component_id']=ident
        errors.append(item)
    if not isinstance(analysis,dict) or not isinstance(analysis.get('graph'),dict):
        return {'report':{'ok':False,'source':'nifi','target':'airflow','errors':[{'code':'airflow.graph','message':'A validated NiFi graph analysis is required.'}],'warnings':[]},'files':{}}
    graph=analysis['graph']
    if batch_contract != {'mode':'one_shot','acknowledge_scheduling_change':True}: error('airflow.batch_contract','Explicitly acknowledge one-shot batch scheduling before exporting an Airflow DAG.')
    for finding in analysis.get('diagnostics',[]):
        if finding.get('severity')=='error': error('airflow.graph_validation','NiFi graph has blocking validation diagnostics.',finding.get('component_id'))
    for key in ('controller_services','parameters','parameter_contexts','parameter_providers','ports','funnels','remote_process_groups'):
        if graph.get(key): error('airflow.unsupported_graph',f'{key} need an explicit Airflow mapping.')
    for group in graph.get('groups',[]):
        if group.get('variables') or group.get('parameter_context'):
            error('airflow.group_context','Group variables and parameter contexts need an explicit mapping.',group.get('id'))
        if group.get('flowFileConcurrency') not in (None,'','UNBOUNDED') or group.get('flowFileOutboundPolicy') not in (None,'','STREAM_WHEN_AVAILABLE'):
            error('airflow.group_policy','Group concurrency and outbound policies need an explicit mapping.',group.get('id'))
    processors=graph.get('processors',[]); connections=graph.get('connections',[])
    if not processors or len(processors)>200: error('airflow.node_limit','Airflow export requires between one and 200 processors.')
    nodes={}; mappings={}; parents={}; children={}
    for processor in processors:
        ident=processor.get('id')
        if not isinstance(ident,str) or not ident or ident in nodes:
            error('airflow.processor_id','Processor identifiers must be unique nonempty strings.');continue
        nodes[ident]=processor; parents[ident]=[]; children[ident]=[]
        try: mappings[ident]=_mapping(processor)
        except (Unsupported,ValueError,TypeError) as exc:error('airflow.processor_mapping',str(exc) if isinstance(exc,Unsupported) else 'Processor configuration is malformed.',ident)
    for connection in connections:
        left=connection.get('source_id');right=connection.get('target_id');relations=connection.get('relationships',[])
        if left not in nodes or right not in nodes: error('airflow.connection','Connection does not link mapped processors.');continue
        parents[right].append(left);children[left].append(right)
        expected='response' if nodes[left].get('type','').endswith('.InvokeHTTP') or nodes[left].get('type')=='InvokeHTTP' else 'success'
        if not isinstance(relations,list) or len(relations)!=1 or str(relations[0]).lower()!=expected:error('airflow.relationship','Only HTTP Response and transformation success relationships are mapped.',connection.get('id'))
        config=connection.get('configuration') or {}
        if any(config.get(k) not in (None,'',[],0,'0','0 sec','0 secs','DO_NOT_LOAD_BALANCE') for k in ('prioritizers','flowFileExpiration','loadBalanceStrategy')):error('airflow.queue_semantics','Queue expiration, prioritization and load balancing are not mapped.',connection.get('id'))
    for ident in nodes:
        if len(parents[ident])>1:error('airflow.join','Multiple incoming connections require FlowFile merge/queue semantics and are blocked.',ident)
        if mappings.get(ident,{}).get('operation')=='http_get':
            if parents[ident]:error('airflow.http_input','InvokeHTTP is supported only as a source with no incoming FlowFile.',ident)
        elif not parents[ident]:error('airflow.source','Every source must be a mapped HTTPS GET processor.',ident)
    indegree={ident:len(upstream) for ident,upstream in parents.items()};order=[];ready=sorted(k for k,v in indegree.items() if v==0)
    while ready:
        ident=ready.pop(0);order.append(ident)
        for child in children[ident]:
            indegree[child]-=1
            if indegree[child]==0:ready.append(child);ready.sort()
    if len(order)!=len(nodes):error('airflow.cycle','Airflow requires an acyclic graph; NiFi feedback loops need redesign.')
    report={'ok':not errors,'source':'nifi','target':'airflow','errors':errors,'warnings':warnings,'mapped_processors':len(mappings),'processor_count':len(processors),'mode':'bounded_one_shot','runtime_verified':False}
    result={'report':report,'files':{}}
    if errors:return result
    ids={ident:'processor_'+hashlib.sha256(ident.encode()).hexdigest()[:16] for ident in order}
    plan={'schema':'flowbridge/airflow-batch/v1','processors':[{'id':ident,'task_id':ids[ident],'type':nodes[ident]['type'],'mapping':mappings[ident]} for ident in order], 'connections':connections,'contract':batch_contract}
    dag_id='flowbridge_http_'+hashlib.sha256(json.dumps({'groups':graph.get('groups',[]),'plan':plan},sort_keys=True).encode()).hexdigest()[:16]
    lines=['"""Generated finite batch mapping. Review AIRFLOW-MAPPING.md before enabling."""','import json','from datetime import datetime, timedelta, timezone','from airflow.sdk import DAG, task','from flowbridge_airflow_runtime import execute','', '@task(retries=0, execution_timeout=timedelta(seconds=60), show_return_value_in_logs=False)', 'def mapped_step(config, incoming=None):', '    return execute(config, incoming)', '', 'with DAG(dag_id='+repr(dag_id)+', schedule=None, start_date=datetime(2025, 1, 1, tzinfo=timezone.utc), catchup=False, is_paused_upon_creation=True, max_active_runs=1, tags=["flowbridge", "review-required"]) as dag:']
    for ident in order:
        cfg='json.loads('+repr(json.dumps(mappings[ident]))+')'; upstream=parents[ident]
        lines.append('    '+ids[ident]+' = mapped_step.override(task_id='+repr(ids[ident])+')('+cfg+(', '+ids[upstream[0]] if upstream else '')+')')
    result['files']={'dags/flowbridge_http_batch.py':'\n'.join(lines)+'\n','dags/flowbridge_airflow_runtime.py':Path(__file__).with_name('airflow_runtime.py').read_text(),'airflow-mapping.json':json.dumps(plan,indent=2)+'\n','compatibility-report.json':json.dumps(report,indent=2)+'\n','AIRFLOW-MAPPING.md':_GUIDANCE,'LICENSE':Path(__file__).parent.parent.joinpath('LICENSE').read_text()}
    return result


_GUIDANCE='''# Bounded HTTP ingestion and transformations for Airflow 3

This project contains executable TaskFlow mappings, not placeholder success tasks. It uses the public `airflow.sdk` API. Copy both Python files from `dags/` into an isolated Airflow 3 DAG bundle; inspect import errors and run a test DAG before deployment. No Airflow dependency is installed by Flowbridge and this export does not deploy or enable a DAG.

The DAG is paused on creation, has no schedule and disables task retries. Each manual run makes one HTTPS GET per source and passes bounded response envelopes to downstream tasks. Review airflow-mapping.json against the original graph. There is no automatic NiFi stop, state migration or cutover. Re-running a DAG repeats GET requests and does not preserve NiFi provenance or exactly-once behavior.

Supported operations are unauthenticated HTTPS GET with redirects disabled, literal stateless UpdateAttribute, and UTF-8 Entire text ReplaceText with literal/append/prepend/always-replace semantics. Success-edge fanout copies an envelope for independent downstream tasks. Joins, failures, streaming, expression language, parameters, custom services, advanced rules and every unmapped processor block export. HTTP non-2xx responses, compression, invalid UTF-8 and payloads above 16 KiB fail the task. Generated execution has fixed time bounds rather than NiFi timeout/retry parity.

Output is the leaf task's bounded XCom envelope, not a deployed external sink. Content is base64 encoded and attributes are strings; base64 is not encryption. The XCom backend stores response data (up to 32 KiB envelope). Review endpoint authorization, sensitivity, retention and worker egress. HTTP status code, URL and MIME type are represented; other NiFi-generated attributes and metadata are not claimed equivalent. A safe next sink requires its own tested mapping.

References: https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html and https://github.com/apache/airflow/blob/main/README.md. Airflow is a finite workflow orchestrator, not a direct replacement for streaming FlowFile queues. Processor references are linked in the exporter source. The generated DAG has not been validated by an installed Airflow scheduler unless separate runtime evidence is supplied.
'''
