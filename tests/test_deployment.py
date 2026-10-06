"""Offline release checks using Helm's renderer; no cluster or Python extras needed."""
import re
import json
import shutil
import subprocess
import sys
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from flowbridge.server import Handler


CHART = Path(__file__).resolve().parents[1] / "charts" / "flowbridge"
HELM = shutil.which("helm")


@unittest.skipUnless(HELM, "Helm is required for offline chart validation")
class DeploymentTests(unittest.TestCase):
    def helm(self, *args, succeeds=True):
        result = subprocess.run(
            [HELM, *map(str, args)], capture_output=True, text=True, timeout=30
        )
        if succeeds:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout + result.stderr

    def render(self, *args, succeeds=True):
        return self.helm("template", "migration", CHART, *args, succeeds=succeeds)

    def resource(self, rendered, kind):
        matches = [
            document for document in re.split(r"(?m)^---\s*$", rendered)
            if re.search(rf"(?m)^kind:\s*{re.escape(kind)}\s*$", document)
        ]
        self.assertEqual(len(matches), 1, f"Expected one {kind}, got {len(matches)}")
        return matches[0]

    def test_chart_lints(self):
        self.helm("lint", CHART, "--strict")

    def test_default_pod_can_use_platform_assigned_uid(self):
        manifest = self.render()
        deployment = self.resource(manifest, "Deployment")
        for control in (
            r"runAsNonRoot:\s*true",
            r"allowPrivilegeEscalation:\s*false",
            r"readOnlyRootFilesystem:\s*true",
            r"type:\s*RuntimeDefault",
            r"automountServiceAccountToken:\s*false",
        ):
            self.assertRegex(deployment, control)
        self.assertRegex(deployment, r"drop:\s*(?:\[\s*[\"']?ALL[\"']?\s*\]|\n\s*-\s*[\"']?ALL[\"']?)")
        self.assertNotRegex(deployment, r"\b(?:runAsUser|runAsGroup|fsGroup):\s*\d+")
        self.assertNotRegex(deployment, r"\b(?:privileged|hostNetwork|hostPID|hostIPC):\s*true")
        self.assertNotIn("hostPath:", deployment)
        self.assertIn("resources:", deployment)
        self.assertIn("limits:", deployment)
        self.assertIn("requests:", deployment)

    def test_default_does_not_expose_an_external_endpoint(self):
        manifest = self.render()
        service = self.resource(manifest, "Service")
        self.assertRegex(service, r"type:\s*ClusterIP")
        self.assertNotRegex(manifest, r"(?m)^kind:\s*(?:Ingress|Route)\s*$")
        self.assertNotIn("nodePort:", service)

    def test_openshift_route_uses_tls_and_explicit_origin(self):
        manifest = self.render(
            "--api-versions", "route.openshift.io/v1",
            "--set", "route.enabled=true", "--set", "route.host=flows.example.test",
        )
        route = self.resource(manifest, "Route")
        deployment = self.resource(manifest, "Deployment")
        self.assertRegex(route, r'termination:\s*edge')
        self.assertRegex(route, r'insecureEdgeTerminationPolicy:\s*Redirect')
        self.assertRegex(route, r'host:\s*"?flows\.example\.test"?')
        self.assertIn("https://flows.example.test", deployment)
        self.assertNotRegex(deployment, r"\b(?:runAsUser|runAsGroup|fsGroup):\s*\d+")
        self.assertNotRegex(manifest, r"(?m)^kind:\s*Ingress\s*$")

    def test_kubernetes_ingress_has_tls_and_matching_backend(self):
        manifest = self.render(
            "--set", "ingress.enabled=true", "--set", "ingress.host=flows.example.test",
            "--set", "ingress.tlsSecretName=flows-tls",
        )
        ingress = self.resource(manifest, "Ingress")
        self.assertRegex(ingress, r'secretName:\s*"?flows-tls"?')
        self.assertIn("migration-flowbridge", ingress)
        self.assertIn("https://flows.example.test", self.resource(manifest, "Deployment"))
        self.assertNotRegex(manifest, r"(?m)^kind:\s*Route\s*$")

    def test_conflicting_external_endpoints_are_rejected(self):
        self.render(
            "--api-versions", "route.openshift.io/v1",
            "--set", "route.enabled=true", "--set", "route.host=route.example.test",
            "--set", "ingress.enabled=true", "--set", "ingress.host=ingress.example.test",
            "--set", "ingress.tlsSecretName=flows-tls", succeeds=False,
        )

    def test_external_endpoints_require_explicit_host_and_tls(self):
        for args in (
            ("--set", "route.enabled=true"),
            ("--set", "ingress.enabled=true", "--set", "ingress.tlsSecretName=flows-tls"),
            ("--set", "ingress.enabled=true", "--set", "ingress.host=flows.example.test"),
        ):
            with self.subTest(args=args):
                self.render(*args, succeeds=False)

    def test_helm_health_hook_matches_application_response(self):
        # Execute the actual rendered test command against the application's
        # HTTP handler, substituting only the service address with a local port.
        pod = self.resource(self.render(), "Pod")
        command_line = re.search(r'(?m)^\s*- ("import json, urllib\.request;.*")\s*$', pod)
        self.assertIsNotNone(command_line, "Expected the Python service health command")
        command = json.loads(command_line.group(1))
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            command = command.replace(
                "http://migration-flowbridge:8790/", f"http://127.0.0.1:{server.server_port}/"
            )
            result = subprocess.run([sys.executable, "-c", command], capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
