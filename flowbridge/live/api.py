"""Authenticated API dispatch. Tokens are provisioned outside the web application."""
import hmac
import os
import threading
from pathlib import Path

_manager = None
_lock = threading.Lock()


def token():
    if os.environ.get("FLOWBRIDGE_LIVE_ENABLED", "false").lower() != "true":
        return None
    filename = os.environ.get("FLOWBRIDGE_LIVE_TOKEN_FILE", "")
    try:
        value = Path(filename).read_text().strip()
        return value if len(value) >= 32 else None
    except OSError:
        return None


def authorized(header):
    expected = token()
    return bool(expected and isinstance(header, str) and hmac.compare_digest(header, "Bearer " + expected))


def manager():
    global _manager
    with _lock:
        if _manager is None:
            from .jobs import JobManager
            _manager = JobManager(os.environ.get("FLOWBRIDGE_DATA_DIR", "/data"))
        return _manager


def dispatch(path, payload):
    from .jobs import MigrationError
    if path == "/api/live/assess":
        return manager().assess(payload)
    if path == "/api/live/start":
        return manager().start(payload.get("plan_id"), payload.get("acknowledge"))
    if path == "/api/live/cancel":
        return manager().cancel(payload.get("job_id"))
    if path == "/api/live/cutover":
        return manager().cutover(payload)
    if path.startswith("/api/live/platform/"):
        from .platforms import JsonClient, NiFiClient, CamelKClient, SeaTunnelClient
        client = JsonClient(payload.get("url"), bearer_token=payload.get("bearer_token"), allow_http_loopback=payload.get("allow_http_loopback") is True)
        kind = payload.get("platform")
        operation = path.rsplit("/", 1)[-1]
        if kind == "nifi":
            platform = NiFiClient(client)
            if operation == "discover":
                return platform.discover(payload.get("group_id") or "root")
            if operation == "inspect":
                return platform.inspect(payload.get("group_id"))
            if operation == "export":
                return {"document": platform.export_flow(payload.get("group_id")), "source": "nifi"}
        elif kind == "camel-k":
            platform = CamelKClient(client)
            args = (payload.get("namespace"), payload.get("name"))
            if operation == "inspect":
                return platform.inspect(*args)
            if operation == "export":
                return {"document": platform.export_flow(*args), "source": "camel-k"}
            if operation == "validate":
                from ..core import analyze
                document = payload.get("document")
                report = analyze(document, "camel-k")
                if not report["report"]["ok"]:
                    return report
                return {"validation": platform.create(payload.get("namespace"), document), "applied": False}
        elif kind == "seatunnel":
            platform = SeaTunnelClient(client)
            if operation == "inspect":
                return platform.inspect(payload.get("job_id"))
            if operation == "export":
                return platform.export_flow(payload.get("job_id"))
        raise MigrationError("This platform operation is not supported. No deployment was performed.")
    raise MigrationError("Unknown connected-migration operation.")
