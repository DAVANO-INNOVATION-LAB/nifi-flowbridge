import ast
import copy
import http.server
import json
import shutil
import ssl
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from flowbridge.airflow import export_airflow
from flowbridge.airflow_runtime import execute, envelope, content, MappingError

CONTRACT={'mode':'one_shot','acknowledge_scheduling_change':True}

def graph():
    return {'ok':True,'diagnostics':[],'graph':{'processors':[
        {'id':'fetch','type':'org.apache.nifi.processors.standard.InvokeHTTP','properties':{'HTTP URL':'https://example.test/data','HTTP Method':'GET','HTTP Redirects Enabled':'false'}},
        {'id':'tag','type':'org.apache.nifi.processors.attributes.UpdateAttribute','properties':{'Store State':'Do not store state','source':'api'}},
        {'id':'replace','type':'org.apache.nifi.processors.standard.ReplaceText','properties':{'Evaluation Mode':'Entire text','Replacement Strategy':'Literal Replace','Search Value':'old','Replacement Value':'new'}},
    ],'connections':[
        {'id':'one','source_id':'fetch','target_id':'tag','relationships':['Response']},
        {'id':'two','source_id':'tag','target_id':'replace','relationships':['success']},
    ]}}


class AirflowExportTests(unittest.TestCase):
    def test_executable_project_contains_all_mapped_tasks_and_parses(self):
        result=export_airflow(graph(),CONTRACT)
        self.assertTrue(result['report']['ok'], result)
        code=result['files']['dags/flowbridge_http_batch.py'];ast.parse(code)
        self.assertIn('from airflow.sdk import DAG, task',code)
        self.assertIn('is_paused_upon_creation=True',code)
        self.assertIn('schedule=None',code)
        self.assertIn('show_return_value_in_logs=False',code)
        self.assertEqual(code.count(' = mapped_step.override('),3)
        self.assertNotIn('EmptyOperator',code)
        plan=json.loads(result['files']['airflow-mapping.json'])
        self.assertEqual([p['mapping']['operation'] for p in plan['processors']],['http_get','update_attributes','replace_text'])
        self.assertFalse(result['report']['runtime_verified'])

    def test_explicit_batch_contract_is_required(self):
        result=export_airflow(graph())
        self.assertFalse(result['report']['ok']);self.assertEqual(result['files'],{})

    def test_streaming_node_blocks_all_executable_output(self):
        data=graph();data['graph']['processors'][1]['type']='org.apache.nifi.ConsumeKafka'
        result=export_airflow(data,CONTRACT)
        self.assertFalse(result['report']['ok']);self.assertEqual(result['files'],{})

    def test_failure_edges_cycles_and_joins_block_export(self):
        for mutation in ('failure','cycle','join'):
            with self.subTest(mutation=mutation):
                data=graph();edges=data['graph']['connections']
                if mutation=='failure':edges[0]['relationships']=['Failure']
                elif mutation=='cycle':edges.append({'source_id':'replace','target_id':'fetch','relationships':['success']})
                else:edges.append({'source_id':'fetch','target_id':'replace','relationships':['Response']})
                self.assertFalse(export_airflow(data,CONTRACT)['report']['ok'])

    def test_graph_secret_diagnostics_cannot_be_overridden(self):
        data=graph();data['diagnostics']=[{'severity':'error','code':'secret','component_id':'fetch','message':'blocked'}]
        self.assertEqual(export_airflow(data,CONTRACT)['files'],{})

    def test_expression_advanced_state_and_unknown_http_properties_block(self):
        cases=[(1,'tag','${hostname()}'),(1,'Store State','Store state locally'),(0,'proxy-host','proxy.test'),(0,'HTTP URL','https://example.test/?token=secret'),(0,'HTTP Redirects Enabled','true'),(2,'Replacement Strategy','Regex Replace')]
        for index,key,value in cases:
            with self.subTest(key=key,value=value):
                data=graph();data['graph']['processors'][index]['properties'][key]=value
                self.assertEqual(export_airflow(data,CONTRACT)['files'],{})
        data=graph();data['graph']['processors'][1]['configuration']={'annotationData':'advanced rules'}
        self.assertEqual(export_airflow(data,CONTRACT)['files'],{})

    def test_fanout_keeps_independent_envelopes(self):
        original=envelope(b'old',{'a':'b'})
        tagged=execute({'operation':'update_attributes','attributes':{'a':'changed'}},original)
        self.assertEqual(original['attributes']['a'],'b');self.assertEqual(tagged['attributes']['a'],'changed')
        replaced=execute({'operation':'replace_text','strategy':'Literal Replace','search':'old','replacement':'new'},original)
        self.assertEqual(content(replaced),b'new');self.assertEqual(content(original),b'old')

    def test_runtime_payload_and_utf8_bounds(self):
        with self.assertRaises(MappingError):envelope(b'x'*16385)
        with self.assertRaises(MappingError):execute({'operation':'replace_text','strategy':'Append','replacement':'x'},envelope(b'\xff'))
        with self.assertRaises(MappingError):execute({'operation':'replace_text','strategy':'Append','replacement':'x'},envelope(b'x'*16384))

    def test_literal_data_cannot_become_python_code(self):
        data=graph();data['graph']['processors'][1]['properties']['label']='\"\n__import__("os").system("false")\n'
        result=export_airflow(data,CONTRACT);self.assertTrue(result['report']['ok'])
        tree=ast.parse(result['files']['dags/flowbridge_http_batch.py'])
        names=[node.func.id for node in ast.walk(tree) if isinstance(node,ast.Call) and isinstance(node.func,ast.Name)]
        self.assertNotIn('__import__',names)

    def test_custom_names_and_jinja_templates_are_rejected(self):
        for kind in ('custom.InvokeHTTP', 'InvokeHTTP'):
            data=graph();data['graph']['processors'][0]['type']=kind
            self.assertEqual(export_airflow(data,CONTRACT)['files'],{})
        data=graph();data['graph']['processors'][1]['properties']['label']='{{ dangerous }}'
        self.assertEqual(export_airflow(data,CONTRACT)['files'],{})

    def test_group_policies_and_parameter_providers_require_mapping(self):
        for group in ({'variables':{'a':'b'}},{'flowFileConcurrency':'SINGLE_BATCH'},{'flowFileOutboundPolicy':'BATCH_OUTPUT'}):
            data=graph();data['graph']['groups']=[group]
            self.assertEqual(export_airflow(data,CONTRACT)['files'],{})
        data=graph();data['graph']['parameter_providers']=[{'id':'p'}]
        self.assertEqual(export_airflow(data,CONTRACT)['files'],{})

    def test_dag_identity_is_stable_and_configuration_specific(self):
        first=export_airflow(graph(),CONTRACT)['files']
        self.assertEqual(first,export_airflow(graph(),CONTRACT)['files'])
        data=graph();data['graph']['processors'][1]['properties']['source']='other'
        second=export_airflow(data,CONTRACT)['files']
        def identity(files):
            tree=ast.parse(files['dags/flowbridge_http_batch.py'])
            return next(k.value.value for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='DAG' for k in n.keywords if k.arg=='dag_id')
        self.assertNotEqual(identity(first),identity(second));self.assertIn('LICENSE',first)

    def test_http_requires_verified_https(self):
        for url in ('http://127.0.0.1/', 'https://user:pass@example.test/', 'https://example.test/?secret=x'):
            with self.assertRaises(MappingError):execute({'operation':'http_get','url':url})


@unittest.skipUnless(shutil.which('openssl'),'OpenSSL required for a trusted local TLS fixture')
class AirflowHttpsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory=tempfile.TemporaryDirectory();root=Path(cls.directory.name)
        cls.cert=root/'cert.pem';key=root/'key.pem'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1','-keyout',str(key),'-out',str(cls.cert),'-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost'],check=True,capture_output=True)
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path=='/redirect':self.send_response(302);self.send_header('Location','/data');self.end_headers();return
                self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers()
                self.wfile.write(b'x'*16385 if self.path=='/large' else b'{"status":"old"}')
            def log_message(self,*args):pass
        cls.server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(str(cls.cert),str(key))
        cls.server.socket=context.wrap_socket(cls.server.socket,server_side=True)
        cls.thread=threading.Thread(target=cls.server.serve_forever,daemon=True);cls.thread.start()
        cls.trusted=ssl.create_default_context(cafile=str(cls.cert))
    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown();cls.server.server_close();cls.thread.join();cls.directory.cleanup()

    def test_real_verified_https_get_and_transform(self):
        with patch('flowbridge.airflow_runtime.ssl.create_default_context',return_value=self.trusted):
            result=execute({'operation':'http_get','url':f'https://localhost:{self.server.server_port}/data'})
        self.assertEqual(content(result),b'{"status":"old"}')
        result=execute({'operation':'replace_text','strategy':'Literal Replace','search':'old','replacement':'new'},result)
        self.assertEqual(content(result),b'{"status":"new"}')
        self.assertEqual(result['attributes']['invokehttp.status.code'],'200')

    def test_redirect_and_oversize_fail_in_actual_http_client(self):
        for path in ('redirect','large'):
            with self.subTest(path=path),patch('flowbridge.airflow_runtime.ssl.create_default_context',return_value=self.trusted):
                with self.assertRaises(MappingError):execute({'operation':'http_get','url':f'https://localhost:{self.server.server_port}/{path}'})

    def test_untrusted_certificate_is_rejected(self):
        with self.assertRaises(MappingError):execute({'operation':'http_get','url':f'https://localhost:{self.server.server_port}/data'})
