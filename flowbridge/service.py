"""Conversion service and executable Kafka project packaging."""
import json
from pathlib import Path

from . import core


def analyze(data, source="auto"):
    return core.analyze(data, source)


def convert(data, source="auto", target="flowbridge"):
    result = core.convert(data, source, target)
    if result["report"]["ok"] and target == "kafka":
        flow = result["flow"]
        if flow["source"]["brokers"] != flow["sink"]["brokers"]:
            result["report"]["ok"] = False
            result["report"]["errors"].append({"code": "kafka.cross_cluster", "message": "Kafka Streams export requires source and destination in the same cluster."})
            result["files"] = {}
            return result
        templates = Path(__file__).parent.parent / "templates" / "kafka"
        for path in templates.rglob("*"):
            if path.is_file():
                content = path.read_text()
                for key, value in {
                    "BROKERS": flow["source"]["brokers"], "INPUT": flow["source"]["topic"],
                    "OUTPUT": flow["sink"]["topic"], "GROUP": flow["source"]["group"],
                    "OFFSET": flow["source"]["offset"],
                }.items():
                    content = content.replace("@@" + key + "@@", json.dumps(value, ensure_ascii=True))
                result["files"][str(path.relative_to(templates))] = content
    if result["report"]["ok"]:
        result["files"]["compatibility-report.json"] = json.dumps(result["report"], indent=2) + "\n"
        guidance = {
            "nifi": "Import nifi-flow.json as a flow definition into a compatible NiFi 1.28 installation. Processors are stopped. Configure publisher success/failure relationships and validate every property before enabling. NiFi 2 is not supported.",
            "seatunnel": "Review seatunnel.json against SeaTunnel 2.3.12 with the Kafka connector installed. This JSON job configuration uses NATIVE records. Validate it in an isolated target environment before running it.",
            "camel-k": "Review integration.json against Camel K 2.8 and your operator/runtime. It is a Kubernetes Integration resource in JSON form. Validate the resource and route in a development namespace before enabling it.",
            "kafka": "Read RUN-KAFKA.md. Java 17 and Maven are required. Run mvn test before connecting to a development Kafka cluster. Kafka manifest import does not reflect edits to generated Java.",
            "flowbridge": "Import flowbridge.json to restore the supported canonical flow. This is an interchange document, not an executable deployment.",
        }
        result["files"]["README.md"] = "# Flowbridge migration draft\n\n" + guidance[target] + "\n\nRead compatibility-report.json. Generation does not verify target runtime compatibility or preserve live offsets, authentication, provenance, retries or delivery guarantees. Test representative records and failure cases before cutover. No deployment was performed.\n"
        result["files"]["LICENSE"] = (Path(__file__).parent.parent / "LICENSE").read_text()
    return result
