# NiFi migration rules provenance

Upstream: https://github.com/stackabletech/nifi-migrate
Revision: `0de82df8ee73a9b2c52bd6c28850cab37fb93eff`
Copyright: 2025 Stackable GmbH. Apache-2.0 license (included).

The three Rust source files are preserved unmodified for provenance. Flowbridge's
`flowbridge/nifi_upgrade.py` adapts their six component renames and Jolt property
renames into review-only JSON Patch plans. It adds conflict detection, guarded
class/bundle replacements and explicit target bundle/runtime validation requirements.
It does not execute the upstream binary or claim complete NiFi 1→2 compatibility.
