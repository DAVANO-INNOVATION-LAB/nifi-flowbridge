"""Provision a local owner token without printing it or overwriting credentials."""
import argparse
import os
import secrets
import stat
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--path", type=Path, default=Path(".secrets/live-token"))
args = parser.parse_args()
args.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
if stat.S_IMODE(args.path.parent.stat().st_mode) & 0o077:
    raise SystemExit("Choose a private directory accessible only to your user.")
if args.path.exists():
    raise SystemExit("Token already exists; it was not changed.")
fd = os.open(args.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, "w") as stream:
    stream.write(secrets.token_urlsafe(48) + "\n")
# Compose mounts only this file. The private parent protects host access; file
# readability allows an arbitrary non-root container UID to read the mount.
args.path.chmod(0o444)
print("Owner token saved in " + str(args.path.resolve()) + ". It was not printed.")
