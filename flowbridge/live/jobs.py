"""Durable transfer ledger. Connection credentials exist only in process memory."""
import json
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path


class MigrationError(ValueError):
    pass


class JobManager:
    def __init__(self, directory, adapter_factory=None):
        from . import kafka
        self.kafka = kafka
        self.factory = adapter_factory or kafka.KafkaAdapter
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path = directory / "migrations.db"
        self.lock = threading.RLock()
        self.plans = {}
        self.running = {}
        with self.db() as db:
            db.executescript("""CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY, plan_id TEXT UNIQUE, state TEXT, plan TEXT, positions TEXT, groups_json TEXT, copied INTEGER DEFAULT 0, lag INTEGER DEFAULT 0, error TEXT, result TEXT, created REAL, updated REAL);
            CREATE TABLE IF NOT EXISTS mappings(job TEXT, partition_id INTEGER, source_offset INTEGER, mapping TEXT, PRIMARY KEY(job,partition_id,source_offset));""")
            db.execute("UPDATE jobs SET state='interrupted',error='Process restarted. Transfer requires inspection and fresh credentials; no automatic restart.',updated=? WHERE state IN ('copying','mirroring','cutting_over')", (time.time(),))
        os.chmod(self.path, 0o600)

    @contextmanager
    def db(self):
        db = sqlite3.connect(self.path, timeout=15)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        try:
            with db:
                yield db
        finally:
            db.close()

    def assess(self, request):
        from ..core import text
        source, target = request.get("source"), request.get("target")
        if not isinstance(source, dict) or not isinstance(target, dict):
            raise MigrationError("Both Kafka endpoints are required.")
        for endpoint in (source, target):
            text(endpoint.get("topic"))
            text(endpoint.get("brokers"), "brokers")
        groups = request.get("groups", [])
        if not isinstance(groups, list) or len(groups) > 20 or len(set(groups)) != len(groups):
            raise MigrationError("Supply at most twenty distinct consumer groups.")
        for group in groups:
            text(group)
        clients = []
        try:
            for endpoint in (source, target):
                clients.append(self.factory({key: value for key, value in endpoint.items() if key != "topic"}))
            plan = self.kafka.preflight(clients[0], clients[1], source["topic"], target["topic"])
        finally:
            for client in clients:
                client.close()
        count = sum(p["source_high"] - p["source_low"] for p in plan["partitions"])
        if count > 1_000_000:
            raise MigrationError("This release limits a migration to one million source offset positions.")
        ident = uuid.uuid4().hex
        with self.lock:
            self.plans = {key: value for key, value in self.plans.items() if time.time() - value["created"] < 900}
            if len(self.plans) >= 10:
                raise MigrationError("Too many pending plans; wait for a plan to expire.")
            self.plans[ident] = {"source": dict(source), "target": dict(target), "groups": groups, "plan": plan, "created": time.time()}
        return {"plan_id": ident, "report": {"ok": True, "errors": [], "warnings": [{"code": "delivery", "message": "At-least-once transfer: crashes can produce duplicates. Cutover blocks on incomplete or ambiguous offset mappings."}, {"code": "quiescence", "message": "You must stop external producers and consumers before cutover. This tool does not automatically stop NiFi or other applications."}]}, "summary": {"source_topic": source["topic"], "target_topic": target["topic"], "partitions": len(plan["partitions"]), "initial_offset_positions": count, "consumer_groups": groups}}

    def start(self, plan_id, acknowledge):
        if acknowledge is not True:
            raise MigrationError("Explicit transfer acknowledgement is required.")
        with self.lock, self.db() as db:
            previous = db.execute("SELECT id FROM jobs WHERE plan_id=?", (plan_id,)).fetchone()
            if previous:
                return self.status(previous["id"])
            if self.running:
                raise MigrationError("Only one migration may run at a time.")
            draft = self.plans.get(plan_id)
            if not draft or time.time() - draft["created"] > 900:
                raise MigrationError("Assessment expired. Assess the endpoints again.")
            ident = uuid.uuid4().hex
            positions = {p["source_partition"]: p["source_low"] for p in draft["plan"]["partitions"]}
            db.execute("INSERT INTO jobs(id,plan_id,state,plan,positions,groups_json,created,updated) VALUES(?,?,'copying',?,?,?,?,?)", (ident, plan_id, json.dumps(draft["plan"]), json.dumps(positions), json.dumps(draft["groups"]), time.time(), time.time()))
            control = {"cancel": threading.Event(), "cutover": threading.Event()}
            self.running[ident] = control
            db.commit()
            threading.Thread(target=self._run, args=(ident, draft, control), daemon=True).start()
            self.plans.pop(plan_id, None)
        return {"job_id": ident, "state": "copying"}

    def status(self, ident):
        with self.db() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (ident,)).fetchone()
        if row is None:
            raise MigrationError("Migration not found.")
        return {"job_id": row["id"], "state": row["state"], "copied": row["copied"], "lag": row["lag"], "error": row["error"], "cutover_ready": row["state"] == "mirroring" and row["lag"] == 0, "events": [], "result": json.loads(row["result"] or "null")}

    def list_jobs(self):
        with self.db() as db:
            ids = [row["id"] for row in db.execute("SELECT id FROM jobs ORDER BY created DESC LIMIT 25")]
        return {"jobs": [self.status(ident) for ident in ids]}

    def cancel(self, ident):
        with self.lock:
            control = self.running.get(ident)
            if not control:
                return self.status(ident)
            if control["cutover"].is_set():
                raise MigrationError("Cutover is already applying offsets; inspect its result before another action.")
            control["cancel"].set()
        return self.status(ident)

    def cutover(self, request):
        ident = request.get("job_id")
        if any(request.get(key) is not True for key in ("acknowledge", "producers_stopped", "consumers_stopped")):
            raise MigrationError("Confirm that producers and consumers are stopped and acknowledge cutover.")
        with self.lock:
            state = self.status(ident)
            if state["state"] == "completed":
                return state
            if not state["cutover_ready"] or ident not in self.running:
                raise MigrationError("The migration must be caught up before cutover.")
            self.running[ident]["cutover"].set()
        return self.status(ident)

    def _update(self, ident, state=None, lag=None, error=None, result=None):
        with self.db() as db:
            db.execute("UPDATE jobs SET state=COALESCE(?,state),lag=COALESCE(?,lag),error=?,result=COALESCE(?,result),updated=? WHERE id=?", (state, lag, error, json.dumps(result) if result is not None else None, time.time(), ident))

    def _run(self, ident, draft, control):
        clients = []
        try:
            for endpoint in (draft["source"], draft["target"]):
                clients.append(self.factory({key: value for key, value in endpoint.items() if key != "topic"}))
            source, target = clients
            original = draft["plan"]
            positions = {p["source_partition"]: p["source_low"] for p in original["partitions"]}

            def record(mapping):
                partition, offset = mapping["source_partition"], mapping["source_offset"]
                with self.db() as db:
                    db.execute("INSERT INTO mappings VALUES(?,?,?,?)", (ident, partition, offset, json.dumps(mapping)))
                    positions[partition] = offset + 1
                    db.execute("UPDATE jobs SET positions=?,copied=copied+1,updated=? WHERE id=?", (json.dumps(positions), time.time(), ident))

            def lookup(topic, partition, offset):
                if topic != original["source_topic"]:
                    return None
                with self.db() as db:
                    row = db.execute("SELECT mapping FROM mappings WHERE job=? AND partition_id=? AND source_offset=?", (ident, partition, offset)).fetchone()
                return json.loads(row["mapping"]) if row else None

            def current_plan():
                fresh = self.kafka.refresh_plan(source, target, original)
                if any(fresh[key] != original[key] for key in ("source_cluster_id", "target_cluster_id")) or len(fresh["partitions"]) != len(original["partitions"]):
                    raise MigrationError("Endpoint identity or partition count changed.")
                by_partition = {p["source_partition"]: p for p in original["partitions"]}
                for p in fresh["partitions"]:
                    old = by_partition[p["source_partition"]]
                    if p["source_low"] > positions[p["source_partition"]]:
                        raise MigrationError("Source retention passed the transfer checkpoint.")
                    p["source_low"] = old["source_low"]
                    p["target_low"] = old["target_low"]
                    p["target_high"] = old["target_high"]
                if sum(p["source_high"] - p["source_low"] for p in fresh["partitions"]) > 1_000_000:
                    raise MigrationError("Migration offset limit reached.")
                return fresh

            while not control["cancel"].is_set():
                plan = current_plan()
                result = self.kafka.copy_snapshot(source, target, plan, positions=positions, on_delivery=record, cancelled=control["cancel"].is_set, max_records=1000, max_bytes=16 * 1024 * 1024, timeout=10)
                lag = sum(max(0, p["source_high"] - positions[p["source_partition"]]) for p in plan["partitions"])
                self._update(ident, "mirroring" if lag == 0 else "copying", lag)
                if control["cutover"].is_set():
                    self._update(ident, "cutting_over")
                    before = [(p["source_partition"], p["source_high"]) for p in plan["partitions"]]
                    if control["cancel"].wait(2):
                        break
                    final_plan = current_plan()
                    after = [(p["source_partition"], p["source_high"]) for p in final_plan["partitions"]]
                    if before != after or lag:
                        raise MigrationError("Source is still changing. Cutover was refused.")
                    preview = self.kafka.translate_group_offsets(source, target, final_plan, draft["groups"], lookup)
                    # Save the intended changes before any non-transactional group update.
                    self._update(ident, "cutting_over", result={"offset_preview": preview})
                    applied = self.kafka.apply_group_offsets(source, target, preview)
                    self._update(ident, "completed", 0, result={"offset_preview": preview, "offsets": applied, "delivery": "at_least_once", "source_offsets_modified": False, "note": "Target offsets applied. External applications must be pointed at the target separately."})
                    return
                if lag == 0:
                    control["cancel"].wait(1)
            self._update(ident, "cancelled", error="Copy stopped. Already delivered target records remain; source offsets were not changed.")
        except Exception as exc:
            code = exc.code if isinstance(exc, self.kafka.KafkaBridgeError) else "migration_check_failed"
            self._update(ident, "failed", error=code + ": Migration stopped. Target records or offsets may have changed; inspect both clusters before retrying. Raw broker errors are not logged.")
        finally:
            for client in clients:
                try:
                    client.close()
                except Exception:
                    pass
            with self.lock:
                self.running.pop(ident, None)
