"""Bounded live control-plane adapters. No implicit writes or credential persistence."""
import copy
import ipaddress
import json
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request

MAX_RESPONSE = 2 * 1024 * 1024
_ID = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]{0,252}$')
_DNS = re.compile(r'^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$')
_SECRET = re.compile(r'password|secret|token|credential|jaas|private.?key', re.I)


class PlatformError(ValueError):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


def fail(code, message):
    raise PlatformError(code, message) from None


def _identifier(value, dns=False):
    if not isinstance(value, str) or not (_DNS if dns else _ID).fullmatch(value) or value in ('.', '..'):
        fail('invalid_identifier', 'Invalid platform resource identifier.')
    return value


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class JsonClient:
    """Owner-selected API base URL; HTTPS verification always enabled.

    Private endpoints are intentional. The caller must authenticate the owner and
    authorize endpoint registration; never expose this as an anonymous URL proxy.
    No environment proxy, redirects, cookies or persisted credentials are used.
    """
    def __init__(self, base_url, bearer_token=None, ca_file=None, timeout=10,
                 allow_http_loopback=False):
        try:
            if not isinstance(base_url, str) or any(ord(c) < 33 for c in base_url) or '\\' in base_url:
                raise ValueError()
            parsed = urllib.parse.urlsplit(base_url)
            port = parsed.port
            if parsed.scheme not in ('https', 'http') or not parsed.hostname or parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment:
                raise ValueError()
            if any(p in ('.', '..') for p in urllib.parse.unquote(parsed.path).split('/')):
                raise ValueError()
            if parsed.scheme == 'http':
                # Literal loopback only, avoiding DNS answers changing after validation.
                if not allow_http_loopback or not ipaddress.ip_address(parsed.hostname).is_loopback:
                    raise ValueError()
                if bearer_token:
                    fail('insecure_credentials', 'Bearer credentials require HTTPS.')
            if port is not None and not 1 <= port <= 65535:
                raise ValueError()
        except (ValueError, TypeError):
            fail('invalid_endpoint', 'Use an HTTPS API base URL without embedded credentials, query or fragment. Explicit local HTTP is limited to literal loopback addresses.')
        if bearer_token is not None and (not isinstance(bearer_token, str) or not re.fullmatch(r'[!-~]{1,16384}', bearer_token)):
            fail('invalid_credential', 'Invalid bearer credential format.')
        if not isinstance(timeout, (int, float)) or not 0 < timeout <= 30:
            fail('invalid_timeout', 'Timeout must be greater than zero and at most 30 seconds.')
        self._base = base_url.rstrip('/')
        self._token = bearer_token
        self._timeout = timeout
        try:
            context = ssl.create_default_context(cafile=ca_file)
        except (OSError, ssl.SSLError):
            fail('invalid_ca', 'The configured CA trust file could not be loaded.')
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}),
                                                   urllib.request.HTTPSHandler(context=context), _NoRedirect())

    def request(self, method, path, document=None, query=None):
        if method not in ('GET', 'POST', 'PUT') or not isinstance(path, str) or not path.startswith('/') or path.startswith('//') or '?' in path or '#' in path or '\\' in path or any(p in ('.', '..') for p in urllib.parse.unquote(path).split('/')):
            fail('invalid_request', 'Unsupported platform request.')
        url = self._base + path
        if query:
            url += '?' + urllib.parse.urlencode(query)
        headers = {'Accept': 'application/json', 'Accept-Encoding': 'identity'}
        if self._token:
            headers['Authorization'] = 'Bearer ' + self._token
        payload = None
        if document is not None:
            try:
                payload = json.dumps(document, allow_nan=False).encode()
            except (ValueError, TypeError, RecursionError):
                fail('invalid_document', 'Invalid request document.')
            if len(payload) > MAX_RESPONSE:
                fail('request_too_large', 'Request exceeds the 2 MiB limit.')
            headers['Content-Type'] = 'application/json'
        req = urllib.request.Request(url, data=payload, headers=headers, method=method)
        try:
            with self._opener.open(req, timeout=self._timeout) as response:
                if response.headers.get('Content-Encoding', 'identity') not in ('identity', ''):
                    fail('unsupported_encoding', 'Compressed platform responses are not accepted.')
                raw = response.read(MAX_RESPONSE + 1)
                if len(raw) > MAX_RESPONSE:
                    fail('response_too_large', 'Platform response exceeds the 2 MiB limit.')
                result = json.loads(raw, parse_constant=lambda _: fail('invalid_response', 'Platform returned invalid JSON.'))
                if not isinstance(result, dict):
                    fail('invalid_response', 'Platform response must be a JSON object.')
                return result
        except PlatformError:
            raise
        except urllib.error.HTTPError as error:
            # Never include upstream body, URL, request headers or exception text.
            code = error.code
            error.close()
            fail('http_error', f'Platform returned HTTP {code}; check access and endpoint configuration.')
        except (OSError, urllib.error.URLError, ValueError, RecursionError):
            fail('connection_failed', 'Platform connection or JSON response failed; verify endpoint, TLS trust and access.')


def _export_check(document):
    """Reject recognized literal credentials rather than silently rewriting a flow."""
    def walk(value, depth=0):
        if depth > 64:
            fail('invalid_response', 'Platform document exceeds nesting limits.')
        if isinstance(value, dict):
            descriptors = value.get('propertyDescriptors', {})
            props = value.get('properties', {})
            if isinstance(descriptors, dict) and isinstance(props, dict):
                for key, descriptor in descriptors.items():
                    if isinstance(descriptor, dict) and descriptor.get('sensitive') and props.get(key) not in (None, ''):
                        fail('sensitive_export', 'Export includes sensitive properties; prepare a credential-free definition at the source.')
            for key, item in value.items():
                from ..graph import _public_credential_setting
                if _SECRET.search(key) and item not in (None, '', False, [], {}) and not _public_credential_setting(key,item):
                    fail('sensitive_export', 'Export includes credential-like settings; prepare a credential-free definition at the source.')
                walk(item, depth + 1)
        elif isinstance(value, list):
            for item in value:
                walk(item, depth + 1)
    walk(document)
    return document


class NiFiClient:
    def __init__(self, client):
        self.client = client

    def export_flow(self, group_id):
        group_id = _identifier(group_id)
        document = self.client.request('GET', f'/process-groups/{group_id}/download',
                                       query={'includeReferencedServices': 'true'})
        if not isinstance(document.get('flowContents'), dict):
            fail('invalid_response', 'NiFi did not return a flow definition.')
        return _export_check(document)

    def discover(self, group_id='root'):
        """Read the hierarchy in bounded calls; denied or truncated scope stays blocked."""
        from ..fleet import discover_fleet
        pending=[(_identifier(group_id),[])];seen=set();groups=[];errors=[];deadline=time.monotonic()+60
        while pending and len(seen)<256 and time.monotonic()<deadline:
            gid,parent=pending.pop(0)
            if gid in seen:
                errors.append({'group_id':gid,'code':'duplicate_group_reference'});continue
            seen.add(gid)
            try:
                data=self.client.request('GET',f'/flow/process-groups/{gid}')
                entity=data.get('processGroupFlow',{});flow=entity.get('flow')
                if not isinstance(flow,dict):raise ValueError()
                actual=_identifier(entity.get('id',gid))
                breadcrumb=(entity.get('breadcrumb') or {}).get('breadcrumb') or {}
                path=parent+[str(breadcrumb.get('name') or actual)]
                processors=flow.get('processors') or [];children=flow.get('processGroups') or []
                connections=flow.get('connections') or []
                inbound={((c.get('component') or {}).get('destination') or {}).get('id') for c in connections}
                entry={'id':actual,'path':path,'processors':len(processors),'connections':len(connections),'ingest_candidates':sum((p.get('component') or {}).get('id') not in inbound for p in processors),'assessment':None}
                if any(flow.get(k) for k in ('inputPorts','outputPorts','remoteProcessGroups','funnels')) or (connections and not processors):
                    errors.append({'group_id':actual,'code':'intergroup_or_port_dependencies_require_mapping'})
                if processors:
                    assessment=discover_fleet(self.export_flow(actual))
                    entry['assessment']={k:assessment[k] for k in ('report','inventory','mapped','unmapped')}
                    if not assessment['report']['ok']:errors.append({'group_id':actual,'code':'unmapped_workflow'})
                    if children:errors.append({'group_id':actual,'code':'mixed_parent_workflow_requires_scope_review'})
                groups.append(entry)
                for child in children:
                    component=child.get('component') or {}
                    pending.append((_identifier(component.get('id') or child.get('id')),path))
            except Exception:
                errors.append({'group_id':gid,'code':'group_unreadable_or_unassessable'})
        if pending:errors.append({'code':'discovery_limit_reached','remaining_groups':len(pending)})
        return {'platform':'nifi','groups':groups,'group_count':len(groups),'workflow_count':sum(bool(g['processors']) for g in groups),'ingest_candidates':sum(g['ingest_candidates'] for g in groups),'complete':not pending and not any(e['code'] in ('group_unreadable_or_unassessable','duplicate_group_reference') for e in errors),'report':{'ok':not errors,'errors':errors,'warnings':[{'code':'snapshot_only','message':'Read-only discovery is a point-in-time inventory. It does not deploy green or authorize cutover.'}]},'cutover_ready':False}

    def inspect(self, group_id):
        group_id = _identifier(group_id)
        data = self.client.request('GET', f'/flow/process-groups/{group_id}/status', query={'recursive': 'true'})
        status = data.get('processGroupStatus', {})
        snapshot = status.get('aggregateSnapshot', {}) if isinstance(status, dict) else {}
        if not isinstance(snapshot, dict):
            fail('invalid_response', 'NiFi did not return process-group status.')
        def count(key):
            value = snapshot.get(key)
            return value if type(value) is int and value >= 0 else None
        return {'platform': 'nifi', 'resource': group_id,
                'active_threads': count('activeThreadCount'), 'queued_count': count('flowFilesQueued'),
                'queued_bytes': count('bytesQueued'), 'snapshot_only': True,
                'capabilities': {'inspect': True, 'export': True, 'import': False, 'cutover': False},
                'limitation': 'A status sample does not prove sources are stopped or queues remain drained. Automated NiFi cutover is blocked.'}


class CamelKClient:
    def __init__(self, client):
        self.client = client

    def _path(self, namespace, name=None):
        path = f'/apis/camel.apache.org/v1/namespaces/{_identifier(namespace, True)}/integrations'
        return path + '/' + _identifier(name, True) if name is not None else path

    def export_flow(self, namespace, name):
        data = self.client.request('GET', self._path(namespace, name))
        if data.get('apiVersion') != 'camel.apache.org/v1' or data.get('kind') != 'Integration' or not isinstance(data.get('spec'), dict):
            fail('invalid_response', 'API did not return a Camel K Integration.')
        # Remove server-managed metadata/status, retain desired specification.
        metadata = data.get('metadata', {})
        return _export_check({'apiVersion': data['apiVersion'], 'kind': data['kind'],
                              'metadata': {'name': metadata.get('name', name), 'namespace': namespace,
                                           **{k: copy.deepcopy(metadata[k]) for k in ('labels', 'annotations') if k in metadata}},
                              'spec': copy.deepcopy(data['spec'])})

    def inspect(self, namespace, name):
        data = self.client.request('GET', self._path(namespace, name))
        if data.get('kind') != 'Integration':
            fail('invalid_response', 'API did not return a Camel K Integration.')
        status = data.get('status', {})
        phase = status.get('phase') if isinstance(status, dict) else None
        # Do not expose arbitrary upstream messages that could contain credentials.
        if not isinstance(phase, str) or not re.fullmatch(r'[A-Za-z]{1,64}', phase):
            phase = 'Unknown'
        return {'platform': 'camel-k', 'resource': f'{namespace}/{name}', 'phase': phase,
                'capabilities': {'inspect': True, 'export': True, 'create_dry_run': True,
                                 'create_with_explicit_apply': True, 'cutover': False}}

    def create(self, namespace, document, allow_apply=False):
        path = self._path(namespace)
        if type(allow_apply) is not bool:
            fail('invalid_apply', 'Apply must be an explicit boolean.')
        if not isinstance(document, dict) or document.get('apiVersion') != 'camel.apache.org/v1' or document.get('kind') != 'Integration' or not isinstance(document.get('spec'), dict):
            fail('invalid_document', 'Expected a Camel K Integration document.')
        metadata = document.get('metadata', {})
        if not isinstance(metadata, dict) or metadata.get('namespace', namespace) != namespace:
            fail('invalid_namespace', 'Document namespace does not match the selected namespace.')
        _identifier(metadata.get('name'), True)
        if 'status' in document or any(k in metadata for k in ('uid', 'resourceVersion', 'managedFields', 'ownerReferences', 'deletionTimestamp', 'finalizers')):
            fail('invalid_document', 'Create requires a desired-state document without managed metadata.')
        data = _export_check(copy.deepcopy(document))
        data['metadata']['namespace'] = namespace
        result = self.client.request('POST', path, document=data,
                                     query={} if allow_apply else {'dryRun': 'All'})
        return {'applied': allow_apply, 'dry_run': not allow_apply,
                'resource': f"{namespace}/{metadata['name']}",
                'uid': result.get('metadata', {}).get('uid'),
                'limitation': 'API acceptance does not prove the operator built or started the integration. An applied Integration may execute immediately.'}


class SeaTunnelClient:
    def __init__(self, client, api_version='v2'):
        if api_version not in ('v1', 'v2'):
            fail('invalid_version', 'Select SeaTunnel REST v1 or v2 explicitly.')
        self.client = client
        self.prefix = '/hazelcast/rest/maps' if api_version == 'v1' else ''

    def inspect(self, job_id):
        if not isinstance(job_id, str) or not re.fullmatch(r'[0-9]{1,20}', job_id):
            fail('invalid_identifier', 'SeaTunnel job ID must be decimal digits.')
        data = self.client.request('GET', f'{self.prefix}/job-info/{job_id}')
        state = data.get('jobStatus')
        if not isinstance(state, str) or not re.fullmatch(r'[A-Z_]{1,64}', state):
            fail('job_unavailable', 'SeaTunnel did not return a known job status.')
        return {'platform': 'seatunnel', 'resource': job_id, 'state': state,
                'capabilities': {'inspect': True, 'export': False, 'import': False, 'cutover': False},
                'limitation': 'Job status and DAG metadata are not a complete reusable job configuration. Upload the original job configuration for conversion.'}

    def export_flow(self, job_id):
        fail('unsupported_export', 'A complete SeaTunnel job-configuration export is not implemented; provide the original configuration file.')
