import copy
import json
import unittest
from pathlib import Path
from flowbridge.service import convert


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.flow = json.loads((Path(__file__).parent.parent / "examples/flowbridge.json").read_text())

    def test_kafka_export_is_executable_project(self):
        result = convert(self.flow, "auto", "kafka")
        self.assertTrue(result["report"]["ok"])
        self.assertIn("pom.xml", result["files"])
        java = result["files"]["src/main/java/io/davano/flowbridge/FlowApplication.java"]
        self.assertNotIn("@@", java)
        self.assertIn("at_least_once", java)

    def test_cross_cluster_kafka_cannot_silently_misroute(self):
        self.flow["sink"]["brokers"] = "different:9092"
        result = convert(self.flow, "auto", "kafka")
        self.assertFalse(result["report"]["ok"])
        self.assertEqual(result["files"], {})
