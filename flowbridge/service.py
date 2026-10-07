"""Conversion service and executable Kafka project packaging."""
import json
from pathlib import Path

from . import core


def _media_document(data):
    if data.get("schema") == "flowbridge/media-etl/v1":
        return data
    group = data.get("flowContents")
    if isinstance(group, dict) and str(group.get("comments", "")).startswith("FLOWBRIDGE_MEDIA_BLUEPRINT_V1\n"):
        from .media import import_nifi_media
        recovered = import_nifi_media(data)
        if recovered["report"]["ok"]:
            return recovered["blueprint"]
    return None


def analyze(data, source="auto"):
    media = _media_document(data)
    if media is not None:
        from .media import validate_media
        return validate_media(media)
    return core.analyze(data, source)


def assess(data, source="auto", nifi_version="auto"):
    if _media_document(data) is not None:
        return analyze(data, source)
    if source == "nifi" or (source == "auto" and any(k in data for k in ("flowContents", "rootGroup", "processors", "flowSnapshot", "versionedFlowSnapshot"))):
        from .graph import analyze_graph
        from .nifi_upgrade import plan_upgrade
        analysis = analyze_graph(data, nifi_version=nifi_version)
        diagnostics = analysis.get("diagnostics", [])
        errors = [d for d in diagnostics if d.get("severity") == "error"]
        warnings = [d for d in diagnostics if d.get("severity") != "error"]
        return {"report": {"ok": analysis["ok"], "source": "nifi", "errors": errors, "warnings": warnings}, "assessment": analysis, "upgrade_plan": plan_upgrade(data)}
    return analyze(data, source)


def convert(data, source="auto", target="flowbridge", batch_contract=None, nifi_version="auto"):
    declared = nifi_version if nifi_version != "auto" else data.get("nifiVersion")
    if declared is not None and (not isinstance(declared, str) or declared.split(".", 1)[0] not in ("1", "2")):
        return {"report": {"ok": False, "errors": [{"code":"unsupported_version", "message":"NiFi version compatibility is verified structurally for 1.x and 2.x only. Future versions cannot be exported."}], "warnings": []}, "files": {}}
    if isinstance(data.get("_flowbridge_xml"), dict) and data["_flowbridge_xml"].get("export_blocked") and target != "nifi-upgrade":
        return {"report": {"ok": False, "errors": [{"code":"xml_review_required", "message":"Legacy XML is available for assessment only. Export a reviewed JSON definition from NiFi before executable conversion."}], "warnings": []}, "files": {}}
    if target in ("airflow-s3", "camel-k-s3"):
        if source not in ("auto", "nifi") or nifi_version not in ("auto", "2", "2.12.0"):
            return {"report":{"ok":False,"errors":[{"code":"unsupported_version","message":"Native S3 target mappings require compatible NiFi 2.12 flows."}],"warnings":[]},"files":{}}
        if target == "airflow-s3":
            from .targets.airflow import export_airflow_fleet
            return export_airflow_fleet(data, contract=batch_contract)
        from .targets.camel_k import export_camel_k
        return export_camel_k(data)
    if target == "s3-fleet":
        if source not in ("auto", "nifi") or nifi_version not in ("auto", "2", "2.12.0"):
            return {"report":{"ok":False,"errors":[{"code":"unsupported_version","message":"Fleet execution requires verified NiFi 2.12 mappings."}],"warnings":[]},"files":{}}
        from .fleet import export_fleet
        return export_fleet(data)
    if target == "continuous-worker":
        declared = nifi_version if nifi_version != "auto" else data.get("nifiVersion")
        if source not in ("auto", "nifi") or (declared is not None and (not isinstance(declared,str) or declared.split(".",1)[0] != "2")):
            return {"report":{"ok":False,"errors":[{"code":"unsupported_version","message":"Continuous native S3 migration requires an explicitly compatible NiFi 2 flow."}],"warnings":[]},"files":{}}
        from .nifi_s3 import export_nifi_s3
        return export_nifi_s3(data)
    if target == "seatunnel" and isinstance(data.get("flowContents"), dict) and isinstance(data["flowContents"].get("processors"), list) and any(p.get("type") == "org.apache.nifi.kafka.processors.ConsumeKafka" for p in data.get("flowContents", {}).get("processors", []) if isinstance(p, dict)):
        try:
            if source not in ("nifi", "auto") or nifi_version not in ("auto", "2", "2.12.0"):
                raise core.Invalid("unsupported_version", "Modern native Kafka mapping requires NiFi 2.12.")
            from .targets.seatunnel import flow_from_nifi
            flow = flow_from_nifi(data)
            result = core.convert(flow, "flowbridge", "seatunnel")
            result["report"]["source"] = "nifi"
            result["report"]["warnings"].append({"code":"live_boundary_required", "message":"Native SeaTunnel handover is verified only for one partition with non-null values, null keys and no headers. This draft has no live boundary: read committed offsets after NiFi stop/drain before using the native job exporter. Timestamps are not equivalent."})
            result["files"]["migration-report.json"] = json.dumps(result["report"], indent=2) + "\n"
            result["files"]["README.md"] = "Review-only SeaTunnel draft. Use flowbridge.targets.seatunnel.export_from_nifi with verified live offsets after source stop/drain for a handover job. This package does not deploy or cut over.\n"
            return result
        except core.Invalid as error:
            return {"report":{"ok":False,"errors":[{"code":error.code,"message":error.message}],"warnings":[]},"files":{}}
    media = _media_document(data)
    if media is not None:
        if target == "nifi":
            from .media import export_nifi_media
            return export_nifi_media(media)
        if target == "flowbridge":
            result = analyze(media)
            result["files"] = {"media-etl.json": json.dumps(result["blueprint"], indent=2)+"\n"} if result["report"]["ok"] else {}
            return result
        from .media_targets import export_media
        return export_media(media, target)
    if target in ("airflow", "nifi-upgrade"):
        assessment = assess(data, source, nifi_version)
        if "assessment" not in assessment:
            return {"report": {"ok": False, "errors": [{"code": "nifi_required", "message": "This target requires a NiFi flow definition."}], "warnings": []}, "files": {}}
        if target == "airflow":
            from .airflow import export_airflow
            return export_airflow(assessment["assessment"], batch_contract=batch_contract)
        plan = assessment["upgrade_plan"]
        errors = plan["errors"] + [d for d in assessment["report"]["errors"] if d.get("code") != "xml_export_blocked"]
        report = {"ok": not errors, "source": "nifi", "errors": errors, "warnings": [{"code": "review_only", "message": "Review-only upgrade plan. Unmapped components and target bundle versions still require validation; no deployable flow is produced."}]}
        return {"report": report, "target": target, "files": {} if errors else {"nifi-upgrade-plan.json": json.dumps(plan, indent=2)+"\n", "README.md": "# NiFi upgrade review plan\n\nThis is a JSON Patch planning artifact, not a deployable flow. Review required_reviews and every unmapped component before applying operations to a copy.\n"}}
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
