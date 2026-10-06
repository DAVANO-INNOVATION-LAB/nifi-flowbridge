import http.client
import json
import tempfile
import threading
import unittest
from pathlib import Path
from http.server import ThreadingHTTPServer
from unittest.mock import patch
from flowbridge.server import Handler


class LiveAPITests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.key = "synthetic-test-token-not-a-real-secret-1234"
        path = Path(self.directory.name) / "token"
        path.write_text(self.key)
        self.environment = patch.dict("os.environ", {"FLOWBRIDGE_LIVE_ENABLED": "true", "FLOWBRIDGE_LIVE_TOKEN_FILE": str(path)})
        self.environment.start()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.environment.stop()
        self.directory.cleanup()

    def request(self, path, data=None, auth=None):
        headers = {"Content-Type": "application/json"}
        if auth is not None:
            headers["Authorization"] = "Bearer " + auth
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port)
        connection.request("POST" if data is not None else "GET", path, json.dumps(data) if data is not None else None, headers)
        response = connection.getresponse()
        result = response.status, json.loads(response.read())
        connection.close()
        return result

    def test_live_auth_required_before_network_or_manager_access(self):
        with patch("flowbridge.live.api.manager") as manager:
            for auth in (None, "wrong"):
                for path, data in (("/api/live/assess", {}), ("/api/live/platform/export", {}), ("/api/live/jobs/example", None)):
                    self.assertEqual(self.request(path, data, auth)[0], 401)
            manager.assert_not_called()

    def test_availability_does_not_expose_token(self):
        status, result = self.request("/api/live/status")
        self.assertEqual(status, 200)
        self.assertTrue(result["enabled"])
        self.assertNotIn(self.key, json.dumps(result))

    def test_live_disabled_even_with_valid_token(self):
        with patch.dict("os.environ", {"FLOWBRIDGE_LIVE_ENABLED": "false"}):
            self.assertFalse(self.request("/api/live/status")[1]["enabled"])
            self.assertEqual(self.request("/api/live/start", {}, self.key)[0], 401)

    def test_internal_exception_does_not_leak_credentials(self):
        with patch("flowbridge.live.api.dispatch", side_effect=RuntimeError("password=DO-NOT-ECHO")):
            status, result = self.request("/api/live/assess", {}, self.key)
            self.assertEqual(status, 422)
            self.assertNotIn("DO-NOT-ECHO", json.dumps(result))

    def test_authenticated_request_passes_exact_payload(self):
        with patch("flowbridge.live.api.dispatch", return_value={"job_id": "job-1", "state": "copying"}) as dispatch:
            self.assertEqual(self.request("/api/live/start", {"plan_id": "plan-1", "acknowledge": True}, self.key)[0], 200)
            dispatch.assert_called_once_with("/api/live/start", {"plan_id": "plan-1", "acknowledge": True})
