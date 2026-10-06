import http.client
import json
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from flowbridge.server import Handler

class MediaAPITests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.server=ThreadingHTTPServer(('127.0.0.1',0),Handler);cls.thread=threading.Thread(target=cls.server.serve_forever,daemon=True);cls.thread.start()
 @classmethod
 def tearDownClass(cls):cls.server.shutdown();cls.server.server_close();cls.thread.join()
 def request(self,path,body=None,mime='application/json'):
  connection=http.client.HTTPConnection('127.0.0.1',self.server.server_port)
  if body is not None and not isinstance(body,bytes):body=json.dumps(body).encode()
  connection.request('GET' if body is None else 'POST',path,body,{'Content-Type':mime});response=connection.getresponse();result=response.status,response.getheader('Content-Type'),response.read();connection.close();return result
 def blueprint(self):return json.loads(self.request('/api/example/media')[2])
 def test_reference_target_exports_downloads_and_reimports_without_execution(self):
  for target in ('airflow','kafka'):
   with self.subTest(target=target):
    status,mime,raw=self.request('/api/download',{'document':self.blueprint(),'target':target})
    self.assertEqual(status,200);self.assertEqual(mime,'application/zip')
    code,_,recovered=self.request('/api/import-package',raw,'application/zip')
    self.assertEqual(code,200);self.assertEqual(json.loads(recovered)['blueprint'],self.blueprint())
 def test_partial_native_drafts_require_explicit_review_download(self):
  for target in ('seatunnel','camel-k'):
   payload={'document':self.blueprint(),'target':target}
   self.assertEqual(self.request('/api/download',payload)[0],422)
   payload['allow_partial']=True
   code,mime,raw=self.request('/api/download',payload)
   self.assertEqual((code,mime),(200,'application/zip'))
   self.assertEqual(self.request('/api/import-package',raw,'application/zip')[0],200)
 def test_nifi_generated_template_reimports_canonical_intent(self):
  code,mime,raw=self.request('/api/download',{'document':self.blueprint(),'target':'nifi','allow_partial':True})
  self.assertEqual((code,mime),(200,'application/zip'))
  result=json.loads(self.request('/api/import-package',raw,'application/zip')[2])
  self.assertEqual(result['blueprint'],self.blueprint())
 def test_corrupt_package_is_rejected(self):self.assertEqual(self.request('/api/import-package',b'not a zip','application/zip')[0],400)
 def test_brand_and_recorded_demo_evidence_are_served(self):
  self.assertEqual(self.request('/brand.png')[0],200)
  html=self.request('/')[2].decode();self.assertIn('Davano Innovation Labs',html);self.assertNotIn('Built to make migrations understandable.',html)
  evidence=json.loads(self.request('/api/demo/media/evidence')[2]);self.assertFalse(evidence['current_production_status']);self.assertEqual(evidence['proof']['result'],'passed')
