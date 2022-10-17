from airflow import DAG
import pytest
from airflow.models import DagBag
import datetime
from pathlib import Path
from airflow.hooks.postgres_hook import PostgresHook

@pytest.fixture
def test_dag():
    dag_id = "my_first_dag"
    dag = DagBag().get_dag(dag_id)
    return dag



def test_task_count(test_dag: DAG):
    print(test_dag)
    assert len(test_dag.tasks) == 5

def test_dag_run(test_dag: DAG):
    test_dag.run(start_date=datetime.datetime(2019,1,1).astimezone(),end_date= datetime.datetime(2019,1,2).astimezone())
    path = Path('/tmp/processed_user.csv')
    assert path.exists()
    hook = PostgresHook(
        postgres_conn_id="postgres",
    )
    rowcount = hook.get_pandas_df("SELECT * from users2")
    assert len(rowcount) > 0
    