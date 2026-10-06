"""Loopback-only by default; no input execution or external service calls."""
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

from .io import MAX_BYTES, archive, read_document, read_archive

ROOT = Path(__file__).resolve().parent.parent


class Handler(BaseHTTPRequestHandler):
    server_version = "Flowbridge"

    def log_message(self, *args):
        pass  # Inputs, headers, and uploaded secrets must never reach access logs.

    def send(self, status, body, content_type="application/json", attachment=False):
        if isinstance(body, dict):
            body = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
        if attachment:
            self.send_header("Content-Disposition", 'attachment; filename="flowbridge-export.zip"')
        self.end_headers()
        self.wfile.write(body)

    def valid_origin(self):
        # Host validation defeats DNS rebinding; browser origins must match exactly.
        host = self.headers.get("Host", "")
        allowed = {f"localhost:{self.server.server_port}", f"127.0.0.1:{self.server.server_port}"}
        allowed.update(value.strip() for value in os.environ.get("FLOWBRIDGE_ALLOWED_HOSTS", "").split(",") if value.strip())
        origins = {f"http://{host}", f"https://{host}"}
        origins.update(value.strip() for value in os.environ.get("FLOWBRIDGE_ALLOWED_ORIGINS", "").split(",") if value.strip())
        return host in allowed and self.headers.get("Origin", f"http://{host}") in origins

    def do_GET(self):
        if not self.valid_origin():
            return self.send(403, {"error": "Local origin required"})
        path = urlsplit(self.path).path
        if path.startswith("/api/live/"):
            from .live import api
            if path == "/api/live/status":
                return self.send(200, {"enabled": bool(api.token()), "authRequired": True})
            if not api.authorized(self.headers.get("Authorization")):
                return self.send(401, {"error": "Live mode requires its owner access token and enabled server configuration."})
            if path == "/api/live/jobs":
                try:
                    return self.send(200, api.manager().list_jobs())
                except OSError:
                    return self.send(503, {"error": "Migration storage is unavailable."})
            if path.startswith("/api/live/jobs/"):
                try:
                    return self.send(200, api.manager().status(path.rsplit("/", 1)[-1]))
                except (ValueError, OSError):
                    return self.send(404, {"error": "Migration unavailable."})
            return self.send(404, {"error": "Not found"})
        if path == "/api/health":
            return self.send(200, {"status": "ok", "version": "0.3.0", "mode": "local"})
        if path == "/api/example/media":
            return self.send(200, json.loads((ROOT / "examples" / "media-etl.json").read_text()))
        if path == "/api/demo/media/target-imports":
            return self.send(200, json.loads((ROOT / "docs" / "media-target-import-evidence.json").read_text()))
        if path == "/api/demo/media/native-import":
            return self.send(200, json.loads((ROOT / "docs" / "nifi-native-import-evidence.json").read_text()))
        if path == "/api/demo/media/evidence":
            return self.send(200, json.loads((ROOT / "docs" / "media-demo-evidence.json").read_text()))
        if path == "/api/example/http":
            return self.send(200, json.loads((ROOT / "examples" / "nifi-http-airflow.json").read_text()))
        if path == "/api/example":
            return self.send(200, {"schema": "flowbridge/v1", "name": "order-events", "source": {"type": "kafka", "brokers": "localhost:9092", "topic": "orders-in", "group": "flowbridge-orders", "offset": "earliest"}, "sink": {"type": "kafka", "brokers": "localhost:9092", "topic": "orders-out"}})
        files = {"/brand.png": ("brand.png", "image/png"), "/": ("index.html", "text/html; charset=utf-8"), "/index.html": ("index.html", "text/html; charset=utf-8"), "/style.css": ("style.css", "text/css"), "/app.js": ("app.js", "text/javascript"), "/ui.js": ("ui.js", "text/javascript")}
        if path not in files:
            return self.send(404, {"error": "Not found"})
        name, mime = files[path]
        try:
            return self.send(200, (ROOT / "web" / name).read_bytes(), mime)
        except OSError:
            return self.send(404, {"error": "Not found"})

    def do_POST(self):
        if not self.valid_origin():
            return self.send(403, {"error": "Local origin required"})
        live = self.path.startswith("/api/live/")
        if self.path not in ("/api/analyze", "/api/assess", "/api/convert", "/api/download", "/api/import-package") and not live:
            return self.send(404, {"error": "Not found"})
        if live:
            from .live import api
            if not api.authorized(self.headers.get("Authorization")):
                return self.send(401, {"error": "Live mode requires its owner access token and enabled server configuration."})
        if self.path == "/api/import-package":
            try:
                length = int(self.headers.get("Content-Length", "-1"))
                if not 0 < length <= MAX_BYTES: return self.send(413, {"error":"Package exceeds size limit"})
                self.connection.settimeout(10)
                files = read_archive(self.rfile.read(length))
                if "nifi-media-flow.json" in files:
                    from .media import import_nifi_media
                    result = import_nifi_media(read_document(files["nifi-media-flow.json"].encode()))
                else:
                    from .media_targets import import_media_package
                    result = import_media_package(files)
                return self.send(200 if result["report"]["ok"] else 422, result)
            except Exception:
                return self.send(400, {"error":"Supply a bounded unchanged Flowbridge media ZIP package."})
        if self.headers.get("Content-Type", "").split(";")[0] != "application/json":
            return self.send(415, {"error": "Send application/json"})
        try:
            length = int(self.headers.get("Content-Length", "-1"))
            if not 0 < length <= MAX_BYTES:
                return self.send(413, {"error": "Request must be under 2 MiB"})
            self.connection.settimeout(10)
            payload = read_document(self.rfile.read(length))
            if live:
                try:
                    return self.send(200, api.dispatch(self.path, payload))
                except Exception as exc:
                    from .live.jobs import MigrationError
                    from .live.kafka import KafkaBridgeError
                    from .live.platforms import PlatformError
                    safe = isinstance(exc, (MigrationError, KafkaBridgeError, PlatformError))
                    message = str(exc) if safe else "Connected operation failed. Check configuration and endpoint access; no completion is claimed."
                    return self.send(422, {"error": message, "report": {"ok": False, "errors": [{"code": getattr(exc, "code", "migration_error") if safe else "migration_error", "message": message}], "warnings": []}})
            document = payload.get("document")
            if isinstance(document, str):
                document = read_document(document.encode())
            if not isinstance(document, dict):
                raise ValueError("Document must be an object")
            source = payload.get("source", "auto")
            target = payload.get("target", "flowbridge")
            formats = ("nifi", "seatunnel", "camel-k", "kafka", "flowbridge")
            if not isinstance(source, str) or source not in ("auto",) + formats or not isinstance(target, str) or target not in formats + ("airflow", "nifi-upgrade"):
                raise ValueError("Unknown format")
            from .service import analyze, assess, convert
            if self.path == "/api/assess":
                result = assess(document, source, payload.get("nifi_version", "auto"))
                return self.send(200, result)
            result = analyze(document, source) if self.path == "/api/analyze" else convert(document, source, target, batch_contract=payload.get("batch_contract"), nifi_version=payload.get("nifi_version", "auto"))
            if self.path == "/api/download" and (result["report"]["ok"] or (payload.get("allow_partial") is True and result.get("files"))):
                return self.send(200, archive(result["files"]), "application/zip", True)
            return self.send(200 if result["report"]["ok"] else 422, result)
        except (ValueError, OSError, RecursionError, TypeError):
            return self.send(400, {"error": "Input could not be processed safely. Check the JSON and size limits."})


def run(host="127.0.0.1", port=8790):
    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    print(f"Flowbridge: http://127.0.0.1:{port}", flush=True)
    server.serve_forever()
