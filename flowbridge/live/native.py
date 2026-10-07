"""Authenticated source assessment and target inspection; no implied promotion."""
import os
from pathlib import Path
from .platforms import JsonClient, NiFiClient, CamelKClient, PlatformError, _identifier, fail


def connection(value):
    if not isinstance(value, dict):
        fail('missing_connection', 'A platform connection is required.')
    return JsonClient(value.get('url'), bearer_token=value.get('bearer_token') or None,
                      allow_http_loopback=value.get('allow_http_loopback') is True)


def dispatch(operation, payload):
    target = payload.get('target')
    if target not in ('seatunnel', 'camel-k', 'airflow'):
        fail('unsupported_target', 'Select SeaTunnel, Camel K or Airflow for native migration.')
    source = payload.get('source')
    if not isinstance(source, dict) or source.get('platform') != 'nifi':
        fail('source_required', 'A connected NiFi source is required.')
    source_client = connection(source)
    nifi = NiFiClient(source_client)
    group = source.get('group_id') or 'root'
    source_status = nifi.inspect(group)
    if operation in ('fence', 'revalidate'):
        if operation == 'fence' and payload.get('acknowledge') is not True:
            fail('acknowledgment_required', 'Explicit authorization is required to stop NiFi ingestion and drain this scope.')
        from .nifi_fence import NiFiFence
        controller = NiFiFence(source_client, Path(os.environ.get('FLOWBRIDGE_DATA_DIR', '/data')) / 'source-fences')
        if operation == 'revalidate':
            return controller.revalidate(payload.get('proof_id'), expected_group_id=group)
        def validate_scope(actual_group):
            checked_payload = dict(payload, source=dict(source, group_id=actual_group))
            assessment = dispatch('prepare', checked_payload)
            if assessment['summary']['prepared'] is not True:
                fail('mapping_blocked', 'Resolve target mapping blockers before stopping this source.')
            return True
        try:
            proof = controller.stop_and_drain(group, timeout=30, validate_scope=validate_scope)
        except PlatformError as error:
            fail(error.code, str(error) + ' Source processors may be partially stopped; inspect NiFi before retrying. No destination was started.')
        return {'proof_id':proof['id'], 'group_id':proof['group_id'],
                'processor_count':len(proof['graph']['processors']),
                'queued_flowfiles':proof['queued_flowfiles'], 'active_threads':proof['active_threads'],
                'source_stopped_and_drained':True, 'cutover_ready':False, 'requires_revalidation':True}
    if operation == 'prepare':
        document = nifi.export_flow(group)
        if target == 'seatunnel':
            # File conversion is an assessment only. Native jobs additionally need
            # live partition boundaries; never manufacture offsets from a document.
            export_target = 'seatunnel'
        else:
            export_target = {'camel-k':'camel-k-s3', 'airflow':'airflow-s3'}[target]
        from ..service import convert
        result = convert(document, source='nifi', target=export_target,
                         batch_contract=payload.get('batch_contract'), nifi_version='2')
        return {'document': document, 'export_target': export_target, 'cutover_ready': False,
                'summary': {'source':source_status, 'report':result['report'],
                            'prepared': result['report']['ok'], 'deployed':False,
                            'cutover_ready':False,
                            'required_checks':['source stop and drain', 'target execution', 'data reconciliation', 'production ownership']}}
    if operation != 'inspect':
        fail('unsupported_operation', 'Automated native promotion is unavailable; no workload was changed.')
    destination = payload.get('destination')
    client = connection(destination)
    resource = destination.get('resource')
    if target == 'camel-k':
        status = CamelKClient(client).inspect(destination.get('namespace'), resource)
    elif target == 'seatunnel':
        from ..targets.seatunnel import SeaTunnelNativeClient
        status = SeaTunnelNativeClient(client).inspect(resource or None)
    else:
        # Airflow 3 REST API: base URL must include /api/v2.
        resource = _identifier(resource)
        data = client.request('GET', '/dags/' + resource)
        if data.get('dag_id') != resource or type(data.get('is_paused')) is not bool:
            fail('invalid_response', 'Airflow did not return the requested DAG state.')
        status = {'platform':'airflow', 'dag_id':resource, 'paused':data['is_paused'],
                  'cutover_ready':False, 'limitation':'DAG existence does not verify task execution, checkpoints or cutover.'}
    return {'source':source_status, 'destination':status, 'cutover_ready':False,
            'limitation':'Inspection is a status sample. Source fencing and destination reconciliation remain required.'}
