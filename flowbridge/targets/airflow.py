"""Strict NiFi S3 estate to Airflow 3 scheduled microbatches.

Native NiFi scheduling/state is not preserved. Cutover initialization is explicit
and must happen outside task execution after the source has stopped and drained.
"""
import hashlib
import os
import fcntl
from contextlib import contextmanager
import json
from pathlib import Path

CONTRACT = {'mode': 'scheduled_microbatch', 'acknowledge_scheduling_change': True}


def export_airflow_fleet(document, contract=None):
    from ..fleet import discover_fleet
    result = discover_fleet(document)
    result['files'] = {}
    result['report']['target'] = 'airflow-s3'
    if contract != CONTRACT:
        result['report']['errors'].append({'code': 'airflow.microbatch_contract', 'message': 'Explicitly acknowledge scheduled microbatch semantics before exporting to Airflow.'})
        result['report']['ok'] = False
    if not result['report']['ok']:
        return result
    root = Path(__file__).resolve().parents[2]
    profile = result['profile']
    ident = 'flowbridge_s3_' + hashlib.sha256(json.dumps(profile, sort_keys=True).encode()).hexdigest()[:16]
    dag = '''"""Paused one-minute S3 microbatches. Requires pre-established cutover ledgers."""
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from airflow.sdk import DAG, task
from flowbridge.targets.airflow import run_lane
PROFILE = json.loads(PROFILE_LITERAL)
@task(retries=0, execution_timeout=timedelta(minutes=10), show_return_value_in_logs=False)
def migrate_lane(lane_id):
    return run_lane(PROFILE, lane_id, os.environ['FLOWBRIDGE_AIRFLOW_STATE_DIR'])
with DAG(dag_id=DAG_LITERAL, schedule=timedelta(minutes=1), start_date=datetime(2025,1,1,tzinfo=timezone.utc), catchup=False, is_paused_upon_creation=True, max_active_runs=1, max_active_tasks=1, tags=['flowbridge','bounded-microbatch']) as dag:
'''.replace('PROFILE_LITERAL', repr(json.dumps(profile))).replace('DAG_LITERAL', repr(ident))
    for lane in profile['lanes']:
        key = hashlib.sha256(lane['id'].encode()).hexdigest()[:16]
        dag += f"    migrate_lane.override(task_id='lane_{key}')({lane['id']!r})\n"
    result['files'] = {
         'dags/flowbridge_s3.py': dag,
        'prepare_airflow_cutover.py': "import argparse, json\nfrom pathlib import Path\nfrom flowbridge.targets.airflow import prepare_cutover\np=argparse.ArgumentParser()\np.add_argument('--profile',required=True)\np.add_argument('--state-dir',required=True)\np.add_argument('--source-stopped',action='store_true',required=True)\np.add_argument('--accept-backfill',action='store_true')\na=p.parse_args()\nprint(json.dumps(prepare_cutover(json.loads(Path(a.profile).read_text()),a.state_dir,a.source_stopped,a.accept_backfill)))\n",
        's3-profile.json': json.dumps(profile, indent=2)+'\n',
        'flowbridge/__init__.py': '', 'flowbridge/targets/__init__.py': '',
        'flowbridge/targets/airflow.py': Path(__file__).read_text(),
        'flowbridge/nifi_s3.py': (root/'flowbridge/nifi_s3.py').read_text(),
        'requirements.txt': 'boto3==1.42.61\n',
        'LICENSE': (root/'LICENSE').read_text(),
        'README.md': 'Install this package and requirements on every Airflow 3 worker. Put the DAG in a configured DAG bundle. Set FLOWBRIDGE_AIRFLOW_STATE_DIR to persistent private storage consistently available to the task process. This paused DAG runs bounded one-minute microbatches after explicit unpausing. It is not native streaming. Stop/drain NiFi and verify it is stopped before running `python prepare_airflow_cutover.py --profile s3-profile.json --state-dir /persistent/airflow --source-stopped --accept-backfill`. This state directory must match FLOWBRIDGE_AIRFLOW_STATE_DIR. Do not run both source and target owners. No tasks create their own cutover authorization. Review backfill before approval. Credentials use the external AWS SDK chain. At-least-once effects, no NiFi queue/state migration or automatic rollback. Task failures remain failures; repeated batch runs use durable per-lane checkpoints.\n'
    }
    result['report']['warnings'].append({'code': 'airflow.microbatch', 'message': 'Paused one-minute batches replace NiFi scheduling. Explicit source stop/drain and baseline reconciliation are required; no native streaming, queue transfer or automatic promotion is claimed.'})
    result['dag_id'] = ident
    return result


def _runner(profile, lane_id, state_dir, client_factory=None):
    from ..nifi_s3 import ContinuousS3Runner, validate_profile, FLEET_SCHEMA
    validate_profile(profile)
    matches = [lane for lane in profile['lanes'] if lane['id'] == lane_id]
    if len(matches) != 1:
        raise ValueError('unknown_lane')
    if client_factory is None:
        import boto3
        from botocore.config import Config
        def client_factory(config):
            return boto3.client('s3', endpoint_url=config['endpoint'], region_name=config['region'], config=Config(s3={'addressing_style':'path' if config['path_style_access'] else 'virtual'}, connect_timeout=10, read_timeout=30, retries={'max_attempts':2}))
    one = dict(profile, schema=FLEET_SCHEMA, lanes=matches)
    key = hashlib.sha256(lane_id.encode()).hexdigest()
    return ContinuousS3Runner(one, str(Path(state_dir)/('lane-'+key+'.sqlite')), client_factory)


@contextmanager
def _fleet_lock(state_dir):
    directory = Path(state_dir)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if directory.is_symlink():
        raise ValueError('symlink_state_refused')
    fd = os.open(directory/'.airflow-fleet.lock', os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield directory
    finally:
        os.close(fd)


def _fingerprint(profile):
    return hashlib.sha256(json.dumps(profile, sort_keys=True).encode()).hexdigest()


def prepare_cutover(profile, state_dir, source_stopped=False, backfill=False, client_factory=None):
    """Operator-only fleet baseline: a partial lane success never authorizes tasks."""
    if source_stopped is not True:
        raise ValueError('source_stop_confirmation_required')
    with _fleet_lock(state_dir) as directory:
        approval = directory/'cutover-approved.json'
        approval.unlink(missing_ok=True)
        results = [_runner(profile, lane['id'], state_dir, client_factory).establish_cutover(True, backfill) for lane in profile['lanes']]
        fd = os.open(approval, os.O_CREAT|os.O_WRONLY|os.O_EXCL|os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'w') as output:
            json.dump({'profile_sha256': _fingerprint(profile)}, output)
            output.flush(); os.fsync(output.fileno())
        return results


def run_lane(profile, lane_id, state_dir, client_factory=None):
    with _fleet_lock(state_dir) as directory:
        try:
            fd = os.open(directory/'cutover-approved.json', os.O_RDONLY|os.O_NOFOLLOW)
            with os.fdopen(fd) as source: approved = json.load(source)
        except (OSError, ValueError):
            raise ValueError('cutover_not_established') from None
        if approved != {'profile_sha256': _fingerprint(profile)}:
            raise ValueError('cutover_profile_mismatch')
        result = _runner(profile, lane_id, state_dir, client_factory).run_once()
        if result['counts']['failures']:
            raise RuntimeError('airflow_batch_failed: '+str(result['counts']['failures']))
        # Never put object keys, credentials, or content into XCom.
        return {'processed': result['counts']['processed'], 'failures': 0, 'delivery': result['delivery']}
