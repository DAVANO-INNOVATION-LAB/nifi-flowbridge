"""Server-owned, durable shadow validation for bounded S3 fleets.

Readiness is evidence about one observed watermark, never permission to change
production ownership. Native fencing requires a trusted server-owned adapter.
"""
import copy
from contextlib import contextmanager
import fcntl
from functools import wraps
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time

from .nifi_s3 import ContinuousS3Runner, MigrationError, validate_profile


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def exclusive(operation):
    @wraps(operation)
    def wrapped(self, *args, **kwargs):
        descriptor = os.open(str(self.path) + '.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            try: fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError: raise MigrationError('bluegreen_operation_in_progress') from None
            return operation(self, *args, **kwargs)
        finally: os.close(descriptor)
    return wrapped


class BlueGreenPlan:
    def __init__(self, profile, state_path, client_factory, fence_adapter=None):
        validate_profile(profile)
        self.profile = copy.deepcopy(profile)
        self.factory = client_factory
        self.fence_adapter = fence_adapter
        self.path = Path(state_path)
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.path.is_symlink():
            raise MigrationError('symlink_state_refused')
        descriptor = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        os.close(descriptor)
        self.fingerprint = digest(profile)
        with self._db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS plan (id INTEGER PRIMARY KEY CHECK(id=1), fingerprint TEXT, document TEXT)')
            initial = {'phase': 'discovered', 'pipeline_ids': [lane['id'] for lane in profile['lanes']], 'shadow_profile': None, 'validation': None, 'cutover_supported': fence_adapter is not None, 'production_changed': False}
            db.execute('INSERT OR IGNORE INTO plan VALUES (1,?,?)', (self.fingerprint, json.dumps(initial)))
            if db.execute('SELECT fingerprint FROM plan WHERE id=1').fetchone()[0] != self.fingerprint:
                raise MigrationError('state_profile_mismatch')
        self.path.chmod(0o600)

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=15)
        try:
            with db: yield db
        finally: db.close()

    def status(self):
        with self._db() as db:
            current=json.loads(db.execute('SELECT document FROM plan WHERE id=1').fetchone()[0])
        current['cutover_supported']=self._has_adapter()
        return current

    def _has_adapter(self):
        return self.fence_adapter is not None and callable(getattr(self.fence_adapter,'fence',None)) and callable(getattr(self.fence_adapter,'verify',None))

    def _require_shadow_phase(self):
        state=self.status()
        if state.get('ownership')=='green' or state.get('production_changed') or state.get('cutover_attempted'):
            raise MigrationError('shadow_actions_refused_after_cutover_attempt')
        return state

    def _save(self, **updates):
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            current = json.loads(db.execute('SELECT document FROM plan WHERE id=1').fetchone()[0])
            timestamp = time.time()
            if 'phase' in updates:
                current['history'] = (current.get('history', []) + [{'phase': updates['phase'], 'at': timestamp}])[-100:]
            current.update(updates)
            current['cutover_supported']=self._has_adapter()
            current['updated_at'] = timestamp
            db.execute('UPDATE plan SET document=? WHERE id=1', (json.dumps(current, sort_keys=True),))
        return current

    def _runner(self, profile):
        return ContinuousS3Runner(profile, str(self.path) + '.shadow.sqlite', self.factory)

    @exclusive
    def shadow_once(self, destination_mapping):
        self._require_shadow_phase()
        # Engine validates exact mapping coverage and separation from production.
        from .fleet import remap_fleet
        shadow = remap_fleet(self.profile, destination_mapping)
        previous = self.status()['shadow_profile']
        if previous is not None and previous != shadow:
            raise MigrationError('shadow_mapping_changed_requires_new_plan')
        self._save(phase='shadow', shadow_profile=shadow, validation=None)
        try:
            runner = self._runner(shadow)
            runner.establish_shadow(self.profile)
            result = runner.run_once()
            return self._save(phase='blocked' if result['failures'] else 'shadow', shadow_result=result)
        except Exception:
            self._save(phase='blocked', failure='shadow_execution_failed')
            raise MigrationError('shadow_execution_failed') from None

    @exclusive
    def validate_shadow(self):
        shadow = self._require_shadow_phase()['shadow_profile']
        if shadow is None:
            raise MigrationError('shadow_not_started')
        self._save(phase='validate', validation=None)
        runner = self._runner(shadow)
        blue = {lane['id']: lane for lane in self.profile['lanes']}
        records = []
        failures = []
        clients = {}
        try:
            for lane in shadow['lanes']:
                original = blue[lane['id']]['destination']
                config = {key: original[key] for key in ('endpoint', 'region', 'path_style_access')}
                cache = json.dumps(config, sort_keys=True)
                if cache not in clients: clients[cache] = self.factory(config)
                native = clients[cache]
                for key, source_head in runner._objects(lane):
                    if len(records) >= 50000:
                        raise MigrationError('validation_object_bound_exceeded')
                    source_bytes, source_type = runner._read(runner.clients[(lane['id'], 'source')], lane['source']['bucket'], key, source_head)
                    entry = {'pipeline_id': lane['id'], 'key': key, 'source_version': source_head.get('VersionId'), 'source_etag': source_head['ETag'], 'source_sha256': hashlib.sha256(source_bytes).hexdigest()}
                    matches = True
                    for side, client, bucket in [('blue', native, original['bucket']), ('green', runner.clients[(lane['id'], 'destination')], lane['destination']['bucket'])]:
                        try:
                            head = client.head_object(Bucket=bucket, Key=key)
                            body, content_type = runner._read(client, bucket, key, head)
                            entry[side + '_sha256'] = hashlib.sha256(body).hexdigest()
                            entry[side + '_metadata_match'] = head.get('Metadata', {}).get('media_type') == lane['media_type']
                            entry[side + '_content_type_match'] = content_type == source_type
                            matches &= body == source_bytes and entry[side + '_metadata_match'] and entry[side + '_content_type_match']
                        except Exception:
                            matches = False
                    entry['matched'] = bool(matches)
                    records.append(entry)
                    if not matches: failures.append({'pipeline_id': lane['id'], 'code': 'output_missing_or_different', 'object_id': digest(key)[:16]})
            before = sorted((r['pipeline_id'], r['key'], r['source_version'], r['source_etag']) for r in records)
            after = sorted((lane['id'], key, head.get('VersionId'), head['ETag']) for lane in shadow['lanes'] for key, head in runner._objects(lane))
            after_lookup = {(row[0], row[1]): row[2:] for row in after}
            stable = all(after_lookup.get((row[0], row[1])) == row[2:] for row in before)
            post_boundary_arrivals = len(set((row[0], row[1]) for row in after) - set((row[0], row[1]) for row in before))
            if not stable: failures.append({'code': 'source_changed_during_validation'})
            populated = {r['pipeline_id'] for r in records}
            for lane in shadow['lanes']:
                if lane['id'] not in populated: failures.append({'pipeline_id': lane['id'], 'code': 'empty_pipeline_not_proven'})
            validation = {'observed_at': time.time(), 'snapshot_sha256': digest(before), 'stable_source_snapshot': stable, 'post_boundary_arrivals': post_boundary_arrivals, 'objects': records, 'pipeline_count': len(shadow['lanes']), 'object_count': len(records), 'failures': failures, 'matched': not failures, 'scope': 'observed latest-object snapshot; not a production fence'}
            return self._save(phase='ready' if not failures else 'blocked', validation=validation)
        except Exception:
            self._save(phase='blocked', failure='shadow_validation_failed', validation=None)
            raise MigrationError('shadow_validation_failed') from None

    def _verify_fence(self, proof):
        expected = sorted(lane['id'] for lane in self.profile['lanes'])
        valid = (isinstance(proof, dict) and sorted(proof.get('pipeline_ids', [])) == expected
                 and proof.get('source_stopped') is True and type(proof.get('queued_flowfiles')) is int and proof.get('queued_flowfiles') == 0
                 and type(proof.get('active_threads')) is int and proof.get('active_threads') == 0 and bool(proof.get('fence_id')))
        if not valid or not self.fence_adapter or not self.fence_adapter.verify(self.profile, proof):
            raise MigrationError('source_fence_not_verified')

    def _verify_validation_boundary(self, validation):
        age=time.time()-validation.get('observed_at',0)
        if not 0<=age<=300:
            raise MigrationError('shadow_validation_expired')
        runner=ContinuousS3Runner(self.profile,str(self.path)+'.production.sqlite',self.factory)
        lanes={lane['id']:lane for lane in self.profile['lanes']}
        try:
            for record in validation['objects']:
                lane=lanes[record['pipeline_id']];client=runner.clients[(lane['id'],'source')]
                head=client.head_object(Bucket=lane['source']['bucket'],Key=record['key'])
                if head.get('VersionId')!=record.get('source_version') or head.get('ETag')!=record['source_etag']:
                    raise MigrationError('validated_source_boundary_changed')
                body,_=runner._read(client,lane['source']['bucket'],record['key'],head)
                if hashlib.sha256(body).hexdigest()!=record['source_sha256']:
                    raise MigrationError('validated_source_boundary_changed')
        except MigrationError:raise
        except Exception:raise MigrationError('validated_source_boundary_unreadable') from None

    @exclusive
    def request_cutover(self):
        # Only a server-configured adapter can stop and verify native sources.
        # There is no client metrics, acknowledgement, or evidence argument.
        state = self.status()
        if state.get('ownership')=='green':
            return state  # Repeated requests never re-fence or clear ownership.
        if state.get('cutover_attempted'):
            raise MigrationError('cutover_recovery_requires_operator_review')
        if not self._has_adapter():
            return self._save(phase='blocked', cutover_blockers=['native_source_fencing_adapter_unavailable'])
        if not (state.get('validation') or {}).get('matched'):
            raise MigrationError('validated_shadow_required')
        try:self._verify_validation_boundary(state['validation'])
        except MigrationError as exc:
            self._save(phase='blocked',cutover_blockers=[str(exc)])
            raise
        self._save(phase='fencing', cutover_attempted=True)
        try:
            proof = self.fence_adapter.fence(self.profile)
            self._verify_fence(proof)
            self._verify_validation_boundary(state['validation'])
            self._save(phase='fenced', fence_proof=proof)
            runner = ContinuousS3Runner(self.profile, str(self.path) + '.production.sqlite', self.factory)
            baseline = runner.establish_cutover(confirmed_source_stopped=True, backfill_existing=True)
            self._verify_fence(proof)
            self._verify_validation_boundary(state['validation'])
            self._save(phase='cutover', production_changed=True, ownership='green', baseline=baseline,
                       promotion_mode='new_worker_writes_existing_production_destinations',
                       data_boundary={'validated_snapshot_sha256':state['validation']['snapshot_sha256'],'previously_observed_keys':'unchanged through post-fence reconciliation','new_keys':'explicitly backfilled then continuously polled','historical_versions':'not transferred'})
            result = runner.run_once()
            return self._save(phase='active' if not result['failures'] else 'blocked', production_result=result)
        except Exception:
            self._save(phase='blocked', failure='cutover_failed_source_may_be_stopped', automatic_rollback=False)
            raise MigrationError('cutover_failed_source_may_be_stopped') from None

    @exclusive
    def run_production_once(self):
        state = self.status()
        if state.get('ownership') != 'green':
            raise MigrationError('production_ownership_not_established')
        try:
            self._verify_fence(state.get('fence_proof'))
            runner = ContinuousS3Runner(self.profile, str(self.path) + '.production.sqlite', self.factory)
            result = runner.run_once()
            return self._save(phase='active' if not result['failures'] else 'blocked', production_result=result)
        except Exception:
            self._save(phase='blocked', failure='production_fence_or_poll_failed')
            raise MigrationError('production_fence_or_poll_failed') from None
