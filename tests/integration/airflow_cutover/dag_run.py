"""Execute real Airflow DagRun/task lifecycle locally, never scheduler proof."""
import json
import os
from airflow.models.dagbag import DagBag
bag=DagBag(dag_folder=os.environ['AIRFLOW__CORE__DAGS_FOLDER'],include_examples=False)
assert not bag.import_errors,bag.import_errors
assert len(bag.dags)==1
run=next(iter(bag.dags.values())).test()
instances=run.get_task_instances()
processed=0
for ti in instances:
    value=ti.xcom_pull(task_ids=ti.task_id,key='return_value')
    if isinstance(value,dict):processed+=value.get('processed',0)
print('AIRFLOW_RESULT:'+json.dumps({'state':str(run.state),'tasks':[{'id':ti.task_id,'state':str(ti.state)} for ti in instances],'processed':processed,'run_id':run.run_id}))
