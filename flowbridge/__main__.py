import argparse
import json
import sys
from pathlib import Path

from .io import read_document, write_artifacts


def main():
    parser = argparse.ArgumentParser(description="Inspect and migrate a supported flow locally.")
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("analyze", "convert"):
        p = sub.add_parser(command)
        p.add_argument("input", type=Path)
        p.add_argument("--source", default="auto", choices=["auto", "nifi", "seatunnel", "camel-k", "kafka", "flowbridge"])
        if command == "convert":
            p.add_argument("--target", required=True, choices=["nifi", "seatunnel", "camel-k", "kafka", "flowbridge"])
            p.add_argument("--output", type=Path, required=True)
            p.add_argument("--accept-warnings", action="store_true", help="Export review artifacts despite compatibility warnings")
    serve = sub.add_parser("serve")
    serve.add_argument("--host", default="127.0.0.1", choices=["127.0.0.1", "0.0.0.0"])
    serve.add_argument("--port", type=int, default=8790)
    args = parser.parse_args()
    if args.command == "serve":
        from .server import run
        run(args.host, args.port)
        return 0
    try:
        from .service import analyze, convert
        with args.input.open("rb") as stream:
            data = read_document(stream.read(2 * 1024 * 1024 + 1))
        result = analyze(data, args.source) if args.command == "analyze" else convert(data, args.source, args.target)
        print(json.dumps(result["report"], indent=2))
        if not result["report"]["ok"]:
            return 2
        if args.command == "convert":
            if result["report"].get("warnings") and not args.accept_warnings:
                print("Review warnings, then use --accept-warnings to write draft artifacts.", file=sys.stderr)
                return 3
            write_artifacts(result["files"], args.output)
        return 0
    except (ValueError, OSError, RecursionError):
        print("Input or output could not be processed safely. Check JSON, size limits, and destination.", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
