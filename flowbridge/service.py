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
    return result
