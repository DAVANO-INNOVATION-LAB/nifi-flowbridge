import io
import unittest
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.error
from unittest.mock import patch
from flowbridge.live.platforms import CamelKClient, JsonClient, MAX_RESPONSE, NiFiClient, PlatformError, SeaTunnelClient, _NoRedirect

class FakeClient:
    def __init__(self, result): self.result, self.calls = result, []
    def request(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.result

class Response(io.BytesIO):
    headers = {}

class PlatformTests(unittest.TestCase):
    def test_invalid_urls(self):
        for url in ['file:///tmp/data','https://user:pass@example.com','https://example.com?token=x','https://example.com/#x','http://example.com','https://example.com/../x','https://example.com/%2e%2e/x','https://example.com\\@evil.com','https://example.com\n']:
            with self.subTest(url=url), self.assertRaises(PlatformError): JsonClient(url)

    def test_loopback_http_requires_explicit_flag_and_no_token(self):
        JsonClient('http://127.0.0.1:9090', allow_http_loopback=True)
        for url in ['http://localhost:9090','http://10.0.0.1']:
            with self.assertRaises(PlatformError): JsonClient(url, allow_http_loopback=True)
        with self.assertRaises(PlatformError): JsonClient('http://127.0.0.1', bearer_token='secret', allow_http_loopback=True)

    def test_token_injection(self):
        with self.assertRaises(PlatformError): JsonClient('https://example.com', bearer_token='x\r\nOther: bad')

    def test_tls_default_context(self):
        with patch('flowbridge.live.platforms.ssl.create_default_context') as context, patch('flowbridge.live.platforms.urllib.request.build_opener'):
            JsonClient('https://example.com', ca_file='/trust/ca.pem')
            context.assert_called_once_with(cafile='/trust/ca.pem')

    def test_bounded_response(self):
        c=JsonClient('https://example.com')
        with patch.object(c._opener,'open',return_value=Response(b' '*(MAX_RESPONSE+1))), self.assertRaises(PlatformError) as error:
            c.request('GET','/x')
        self.assertEqual(error.exception.code,'response_too_large')

    def test_upstream_error_redacted(self):
        c=JsonClient('https://example.com',bearer_token='SENSITIVE')
        error=urllib.error.HTTPError('https://example.com/SENSITIVE',403,'SENSITIVE',{},io.BytesIO(b'SENSITIVE'))
        with patch.object(c._opener,'open',side_effect=error), self.assertRaises(PlatformError) as raised: c.request('GET','/x')
        self.assertNotIn('SENSITIVE',str(raised.exception))

    def test_no_redirect(self): self.assertIsNone(_NoRedirect().redirect_request(None,None,302,'',{},'https://other/'))

    def test_nifi_counts_unknown_not_zero(self):
        f=FakeClient({'processGroupStatus':{'aggregateSnapshot':{'activeThreadCount':2,'flowFilesQueued':7}}})
        result=NiFiClient(f).inspect('abc-123')
        self.assertEqual(result['active_threads'],2)
        self.assertEqual(result['queued_count'],7)
        self.assertIsNone(result['queued_bytes'])
        self.assertFalse(result['capabilities']['cutover'])
        self.assertEqual(f.calls[0][0],('GET','/flow/process-groups/abc-123/status'))

    def test_nifi_export_secret_rejection(self):
        f=FakeClient({'flowContents':{'processors':[]}})
        NiFiClient(f).export_flow('abc')
        self.assertEqual(f.calls[0][0],('GET','/process-groups/abc/download'))
        self.assertEqual(f.calls[0][1]['query'],{'includeReferencedServices':'true'})
        f.result={'flowContents':{'properties':{'custom':'secret'},'propertyDescriptors':{'custom':{'sensitive':True}}}}
        with self.assertRaises(PlatformError): NiFiClient(f).export_flow('abc')

    def test_path_traversal(self):
        f=FakeClient({})
        for resource in ['../x','a/b','a?token=x','%2f']:
            with self.assertRaises(PlatformError): NiFiClient(f).export_flow(resource)
        self.assertEqual(f.calls,[])

    def test_camel_export(self):
        f=FakeClient({'apiVersion':'camel.apache.org/v1','kind':'Integration','metadata':{'name':'orders','uid':'id','managedFields':[]},'spec':{'flows':[]},'status':{'phase':'Running'}})
        result=CamelKClient(f).export_flow('test','orders')
        self.assertNotIn('status',result)
        self.assertEqual(result['metadata'],{'name':'orders','namespace':'test'})

    def test_camel_dry_run_apply_explicit(self):
        f=FakeClient({'metadata':{'uid':'id'}})
        document={'apiVersion':'camel.apache.org/v1','kind':'Integration','metadata':{'name':'orders'},'spec':{'flows':[]}}
        c=CamelKClient(f)
        self.assertFalse(c.create('test',document)['applied'])
        self.assertEqual(f.calls[-1][1]['query'],{'dryRun':'All'})
        self.assertTrue(c.create('test',document,allow_apply=True)['applied'])
        self.assertEqual(f.calls[-1][1]['query'],{})
        self.assertNotIn('namespace',document['metadata'])
        with self.assertRaises(PlatformError): c.create('test',document,allow_apply='yes')

    def test_namespace_mismatch(self):
        f=FakeClient({})
        doc={'apiVersion':'camel.apache.org/v1','kind':'Integration','metadata':{'name':'orders','namespace':'other'},'spec':{}}
        with self.assertRaises(PlatformError): CamelKClient(f).create('test',doc,allow_apply=True)
        self.assertEqual(f.calls,[])

    def test_seatunnel_does_not_fake_export(self):
        f=FakeClient({'jobStatus':'RUNNING','errorMsg':'sensitive'})
        c=SeaTunnelClient(f)
        result=c.inspect('1234')
        self.assertEqual(f.calls[0][0],('GET','/job-info/1234'))
        self.assertNotIn('errorMsg',result)
        self.assertFalse(result['capabilities']['export'])
        with self.assertRaises(PlatformError): c.export_flow('1234')
        SeaTunnelClient(f,'v1').inspect('1234')
        self.assertEqual(f.calls[-1][0],('GET','/hazelcast/rest/maps/job-info/1234'))


class LoopbackHTTPTests(unittest.TestCase):
    def test_real_http_export_path_and_redirect_refusal(self):
        class Handler(BaseHTTPRequestHandler):
            paths = []
            def log_message(self, *args): pass
            def do_GET(self):
                self.paths.append(self.path)
                if self.path == '/redirect':
                    self.send_response(302)
                    self.send_header('Location', '/unexpected')
                    self.end_headers()
                    return
                body = b'{"flowContents":{"processors":[]}}'
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(body)
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            c = JsonClient(f'http://127.0.0.1:{server.server_port}', allow_http_loopback=True)
            self.assertIn('flowContents', NiFiClient(c).export_flow('group-1'))
            self.assertEqual(Handler.paths[0], '/process-groups/group-1/download?includeReferencedServices=true')
            with self.assertRaises(PlatformError): c.request('GET', '/redirect')
            self.assertNotIn('/unexpected', Handler.paths)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
