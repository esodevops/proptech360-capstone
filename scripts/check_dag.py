"""Check that Airflow can read the DAG without running its tasks."""
from pathlib import Path
from airflow.dag_processing.dagbag import DagBag

project = Path(__file__).resolve().parents[1]
bag = DagBag(dag_folder=str(project / 'dags'))
if bag.import_errors:
    raise RuntimeError(bag.import_errors)

dag = bag.dags['proptech360_dag']
assert dag.get_task('extraction_layer').downstream_task_ids == {'transformation_layer'}
assert dag.get_task('transformation_layer').downstream_task_ids == {'loading_layer'}
assert dag.max_active_runs == 1
print('DAG imports successfully and runs the three tasks in order.')
