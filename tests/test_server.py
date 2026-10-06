import http.client
import json
import threading
import unittest
from unittest.mock import patch
from http.server import ThreadingHTTPServer
from flowbridge.server import Handler


class ServerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def request(self, method, path, data=None, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port)
        connection.request(method, path, json.dumps(data) if data else None, headers or {})
        response = connection.getresponse()
        result = response.status, response.read(), dict(response.getheaders())
        connection.close()
        return result

    def test_rebinding_and_cross_origin_blocked(self):
        self.assertEqual(self.request("GET", "/api/health", headers={"Host": "attacker.invalid"})[0], 403)
        self.assertEqual(self.request("GET", "/api/health", headers={"Origin": "https://attacker.invalid"})[0], 403)

    def test_explicit_cluster_host_and_tls_origin(self):
        with patch.dict("os.environ", {"FLOWBRIDGE_ALLOWED_HOSTS": "flowbridge.example.test,flowbridge:8790", "FLOWBRIDGE_ALLOWED_ORIGINS": "https://flowbridge.example.test"}):
            self.assertEqual(self.request("GET", "/api/health", headers={"Host": "flowbridge.example.test", "Origin": "https://flowbridge.example.test"})[0], 200)
            self.assertEqual(self.request("GET", "/api/health", headers={"Host": "flowbridge:8790"})[0], 200)
            self.assertEqual(self.request("GET", "/api/health", headers={"Host": "flowbridge.example.test", "Origin": "https://attacker.invalid"})[0], 403)
            self.assertEqual(self.request("GET", "/api/health", headers={"Host": "unconfigured.example.test"})[0], 403)

    def test_example_converts_to_all_targets(self):
        status, body, _ = self.request("GET", "/api/example")
        self.assertEqual(status, 200)
        for target in ("flowbridge", "nifi", "seatunnel", "camel-k", "kafka"):
            status, output, headers = self.request("POST", "/api/convert", {"source": "auto", "target": target, "document": json.loads(body)}, {"Content-Type": "application/json"})
            self.assertEqual(status, 200, output)
            self.assertTrue(json.loads(output)["files"])
            self.assertIn("frame-ancestors 'none'", headers["Content-Security-Policy"])

    def test_secret_payload_does_not_echo(self):
        status, output, _ = self.request("POST", "/api/convert", {"document": {"password": "secret-sentinel"}}, {"Content-Type": "application/json"})
        self.assertEqual(status, 422)
        self.assertNotIn(b"secret-sentinel", output)

    def test_unsupported_source_not_echoed(self):
        status, output, _ = self.request("POST", "/api/convert", {"source": {"secret": "sentinel"}, "document": {}}, {"Content-Type": "application/json"})
        self.assertEqual(status, 400)
        self.assertNotIn(b"sentinel", output)

    def test_path_traversal_and_wrong_content_type(self):
        self.assertEqual(self.request("GET", "/../flowbridge/server.py")[0], 404)
        self.assertEqual(self.request("POST", "/api/convert", {"document": {}}, {"Content-Type": "text/plain"})[0], 415)

    def test_demo_video_supports_browser_seeking(self):
        status, body, headers = self.request("GET", "/demo.mp4", headers={"Range": "bytes=0-31"})
        self.assertEqual(status, 206)
        self.assertEqual(len(body), 32)
        self.assertIn(b"ftyp", body)
        self.assertEqual(headers["Content-Type"], "video/mp4")
        self.assertTrue(headers["Content-Range"].startswith("bytes 0-31/"))
        status, suffix, _ = self.request("GET", "/demo.mp4", headers={"Range": "bytes=-16"})
        self.assertEqual((status, len(suffix)), (206, 16))
        self.assertEqual(self.request("GET", "/demo.mp4", headers={"Range": "bytes=999999999-"})[0], 416)
        self.assertEqual(self.request("GET", "/demo.mp4", headers={"Range": "bytes=-0"})[0], 416)
        status, body, headers = self.request("HEAD", "/demo.mp4")
        self.assertEqual((status, body), (200, b""))
        self.assertGreater(int(headers["Content-Length"]), 1000)
