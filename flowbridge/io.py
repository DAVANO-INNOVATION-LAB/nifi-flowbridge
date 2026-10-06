"""Bounded input and safe artifact packaging."""
import gzip
import io
import json
import zipfile
from pathlib import Path, PurePosixPath

MAX_BYTES = 2 * 1024 * 1024


def read_document(raw):
    if len(raw) > MAX_BYTES:
        raise ValueError("Input exceeds the 2 MiB limit")
    if raw.startswith(b"\x1f\x8b"):
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as stream:
            raw = stream.read(MAX_BYTES + 1)
        if len(raw) > MAX_BYTES:
            raise ValueError("Expanded input exceeds the 2 MiB limit")
    if raw.lstrip().startswith(b"<"):
        from .nifi_xml import parse_nifi_xml
        return parse_nifi_xml(raw)
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate JSON property")
            result[key] = value
        return result
    value = json.loads(raw, object_pairs_hook=unique,
                       parse_constant=lambda _: (_ for _ in ()).throw(ValueError("Non-finite JSON number")))
    if not isinstance(value, dict):
        raise ValueError("Expected a JSON object")
    return value


def safe_name(name):
    path = PurePosixPath(name)
    if not name or "\\" in name or path.is_absolute() or any(p in ("..", ".") for p in name.split("/")):
        raise ValueError("Unsafe artifact path")
    return path


def archive(files):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as output:
        for name, content in sorted(files.items()):
            safe_name(name)
            info = zipfile.ZipInfo(name, (2024, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            output.writestr(info, content)
    return buffer.getvalue()


def write_artifacts(files, directory):
    directory = Path(directory)
    if directory.exists():
        raise ValueError("Output directory already exists; choose a new directory")
    # Validate all paths before writing anything, and never overwrite existing work.
    for name in files:
        safe_name(name)
    directory.mkdir(parents=True)
    for name, content in files.items():
        target = directory / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")


def read_archive(raw):
    """Read bounded UTF-8 review artifacts without extracting files to disk."""
    if len(raw)>MAX_BYTES: raise ValueError("Archive exceeds size limit")
    files={};total=0
    with zipfile.ZipFile(io.BytesIO(raw)) as source:
        entries=source.infolist()
        if len(entries)>100:raise ValueError("Too many archive entries")
        for entry in entries:
            safe_name(entry.filename)
            if entry.is_dir() or entry.filename in files or entry.file_size>MAX_BYTES:
                raise ValueError("Invalid archive entry")
            with source.open(entry) as stream:content=stream.read(MAX_BYTES-total+1)
            total+=len(content)
            if total>MAX_BYTES:raise ValueError("Expanded archive exceeds size limit")
            files[entry.filename]=content.decode("utf-8")
    return files
