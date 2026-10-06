"""Official Airflow 3 image smoke for the generated synthetic HTTP example.

Run with networking disabled. Generate examples/nifi-http-airflow.json into a
project first, mount its dags/ read-only, and pass this file to `python -`.
AIRFLOW_PROOF_DAGS optionally overrides /opt/airflow/dags.
This checks DagBag import and real decorated-operator invocation, NOT a full
scheduler/TaskRunner execution. HTTPS I/O is covered by test_airflow.py separately.
"""
import os
import sys
from pathlib import Path

folder = Path(os.environ.get("AIRFLOW_PROOF_DAGS", "/opt/airflow/dags"))
sys.path.insert(0, str(folder))

from airflow.models.dagbag import DagBag
from flowbridge_airflow_runtime import content, envelope

bag = DagBag(dag_folder=str(folder), include_examples=False)
assert not bag.import_errors, bag.import_errors
assert len(bag.dags) == 1, list(bag.dags)
dag = next(iter(bag.dags.values()))
assert len(dag.tasks) == 4, "Use the four-node synthetic HTTP example"
assert dag.catchup is False
assert dag.max_active_runs == 1
assert dag.is_paused_upon_creation is True

ordered = dag.topological_sort()
assert [task.op_args[0]["operation"] for task in ordered] == [
    "http_get", "update_attributes", "replace_text", "update_attributes"
]
assert not ordered[0].upstream_task_ids
for previous, following in zip(ordered, ordered[1:]):
    assert following.upstream_task_ids == {previous.task_id}

# No source request is performed: use the documented synthetic fixture instead.
value = envelope(b'{"status":"old"}', {"invokehttp.status.code": "200"})
for operator in ordered[1:]:
    assert operator.retries == 0
    assert operator.show_return_value_in_logs is False
    operator.op_args = (operator.op_args[0], value)
    operator.op_kwargs = {}
    value = operator.execute(context={})
assert content(value) == b'{"status":"new"}'
assert value["attributes"]["source"] == "api"
assert value["attributes"]["migration.mode"] == "one-shot-review"
print("AIRFLOW_PROOF: DagBag imported four tasks; dependency chain verified; "
      "real decorated operators executed attribute/text transformations. "
      "No HTTP source request or scheduler run was performed.")
