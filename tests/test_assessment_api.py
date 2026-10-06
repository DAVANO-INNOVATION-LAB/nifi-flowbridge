"""Real HTTP assessment/export tests. Offline routes use host/origin validation;
Bearer authentication protects separate /api/live routes, not these offline APIs.
No uploaded script, HTTP ingestion target, or generated DAG is executed here.
"""
import copy
import http.client
import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from flowbridge.server import Handler

ROOT = Path(__file__).resolve().parent.parent
CONTRACT = {'mode': 'one_shot', 'acknowledge_scheduling_change': True}


def example():
    return json.loads((ROOT / 'examples/nifi-http-airflow.json').read_text())


class AssessmentHTTPTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.key = 'synthetic-assessment-test-token-' + '0' * 32
        token_file = Path(cls.directory.name) / 'owner-token'
        token_file.write_text(cls.key)
        token_file.chmod(0o600)
        cls.environment = patch.dict('os.environ', {'FLOWBRIDGE_LIVE_ENABLED': 'true',
                                                   'FLOWBRIDGE_LIVE_TOKEN_FILE': str(token_file)})
        cls.environment.start()
        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()
        cls.environment.stop()
        cls.directory.cleanup()

    def request(self, path, document, *, version='2', contract=None, origin=None):
        payload = {'document': document, 'source': 'nifi', 'target': 'airflow', 'nifi_version': version}
        if contract is not None:
            payload['batch_contract'] = contract
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + self.key,
                   'Origin': origin or f'http://127.0.0.1:{self.server.server_port}'}
        connection = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=10)
        try:
            connection.request('POST', path, json.dumps(payload), headers)
            response = connection.getresponse()
            data = json.loads(response.read())
            self.assertNotIn(self.key, json.dumps(data))
            return response.status, data
        finally:
            connection.close()

    def test_xml_inventory_preserves_script_as_data_and_blocks_export(self):
        marker = Path(self.directory.name) / 'must-not-execute'
        xml = '''<flowController><rootGroup><id>root</id><name>Legacy</name>
        <processor><id>script</id><name>Script</name><class>org.apache.nifi.processors.script.ExecuteScript</class>
        <property><name>Script Body</name><value>open(%r, 'w').write('executed')</value></property>
        </processor></rootGroup></flowController>''' % str(marker)
        status, result = self.request('/api/assess', xml, version='1')
        self.assertEqual(status, 200)
        self.assertEqual(result['assessment']['graph']['counts']['processors'], 1)
        self.assertFalse(result['assessment']['capabilities']['automatic_conversion'])
        self.assertFalse(marker.exists())
        status, result = self.request('/api/convert', xml, version='1', contract=CONTRACT)
        self.assertEqual(status, 422)
        self.assertEqual(result['files'], {})
        self.assertIn('xml_review_required', [e['code'] for e in result['report']['errors']])
        self.assertFalse(marker.exists())

    def test_future_nifi3_is_blocked_with_inventory(self):
        document = example()
        for node in document['flowContents']['processors']:
            node['bundle']['version'] = '3.0.0'
        status, result = self.request('/api/assess', document, version='3')
        self.assertEqual(status, 200)
        self.assertFalse(result['report']['ok'])
        self.assertEqual(result['assessment']['source_version']['status'], 'unsupported')
        self.assertIn('unsupported_version', [e['code'] for e in result['report']['errors']])
        self.assertEqual(result['assessment']['graph']['counts']['processors'], len(document['flowContents']['processors']))
        status, result = self.request('/api/convert', document, version='3', contract=CONTRACT)
        self.assertEqual(status, 422)
        self.assertEqual(result['files'], {})

    def test_nested_inventory_includes_services_ports_and_connections(self):
        document = example()
        root = document['flowContents']
        root['processGroups'] = [{'identifier': 'nested', 'name': 'Nested',
                                 'processors': [{'identifier': 'child', 'type': 'org.example.CustomProcessor', 'properties': {}}],
                                 'inputPorts': [{'identifier': 'port-in', 'name': 'In'}],
                                 'controllerServices': [{'identifier': 'service', 'type': 'org.example.Service', 'properties': {}}],
                                 'connections': [{'identifier': 'nested-edge', 'source': {'id': 'port-in', 'type': 'INPUT_PORT'},
                                                  'destination': {'id': 'child', 'type': 'PROCESSOR'}, 'selectedRelationships': []}]}]
        status, result = self.request('/api/assess', document)
        self.assertEqual(status, 200)
        counts = result['assessment']['graph']['counts']
        self.assertEqual(counts['groups'], 2)
        self.assertEqual(counts['processors'], len(root['processors']) + 1)
        self.assertEqual(counts['connections'], len(root['connections']) + 1)
        self.assertEqual(counts['ports'], 1)
        self.assertEqual(len(result['assessment']['graph']['controller_services']), 1)
        self.assertIn('child', [p['id'] for p in result['assessment']['graph']['processors']])

    def test_secrets_redacted_from_entire_http_assessment(self):
        document = example()
        node = document['flowContents']['processors'][0]
        node['properties']['password'] = 'DO-NOT-RETURN-PASSWORD'
        node['properties']['custom-field'] = 'DO-NOT-RETURN-SENSITIVE'
        node['propertyDescriptors'] = {'custom-field': {'sensitive': True}}
        status, result = self.request('/api/assess', document)
        self.assertEqual(status, 200)
        encoded = json.dumps(result)
        self.assertNotIn('DO-NOT-RETURN-PASSWORD', encoded)
        self.assertNotIn('DO-NOT-RETURN-SENSITIVE', encoded)
        status, result = self.request('/api/convert', document, contract=CONTRACT)
        self.assertEqual(status, 422)
        self.assertEqual(result['files'], {})
        self.assertNotIn('DO-NOT-RETURN', json.dumps(result))

    def test_airflow_requires_exact_acknowledgement(self):
        for contract in (None, {'mode': 'one_shot'}, {'mode': 'one_shot', 'acknowledge_scheduling_change': False}):
            with self.subTest(contract=contract):
                status, result = self.request('/api/convert', example(), contract=contract)
                self.assertEqual(status, 422)
                self.assertIn('airflow.batch_contract', [e['code'] for e in result['report']['errors']])
                self.assertEqual(result['files'], {})

    def test_supported_airflow_mapping_is_paused_finite_and_not_executed(self):
        with patch('urllib.request.OpenerDirector.open', side_effect=AssertionError('Conversion must not fetch source URLs')):
            status, result = self.request('/api/convert', example(), contract=CONTRACT)
        self.assertEqual(status, 200, result)
        self.assertTrue(result['report']['ok'])
        self.assertEqual(result['report']['mode'], 'bounded_one_shot')
        self.assertFalse(result['report']['runtime_verified'])
        dag = result['files']['dags/flowbridge_http_batch.py']
        self.assertIn('schedule=None', dag)
        self.assertIn('is_paused_upon_creation=True', dag)
        self.assertIn('retries=0', dag)
        plan = json.loads(result['files']['airflow-mapping.json'])
        self.assertEqual({p['mapping']['operation'] for p in plan['processors']},
                         {'http_get', 'update_attributes', 'replace_text'})
        self.assertEqual(len(plan['processors']), len(example()['flowContents']['processors']))

    def test_unknown_processor_blocks_all_output(self):
        document = example()
        document['flowContents']['processors'][-1]['type'] = 'org.example.DoesNotExist'
        status, assessment = self.request('/api/assess', document)
        self.assertEqual(status, 200)
        nodes = assessment['assessment']['graph']['processors']
        self.assertEqual(nodes[-1]['type'], 'org.example.DoesNotExist')
        status, result = self.request('/api/convert', document, contract=CONTRACT)
        self.assertEqual(status, 422)
        self.assertEqual(result['files'], {})
        self.assertIn('airflow.processor_mapping', [e['code'] for e in result['report']['errors']])

    def test_untrusted_browser_origin_rejected_even_with_token(self):
        status, result = self.request('/api/assess', example(), origin='https://untrusted.example')
        self.assertEqual(status, 403)
        self.assertNotIn('assessment', result)
