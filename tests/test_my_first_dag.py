from airflow import DAG
import pytest
from airflow.models import DagBag
import datetime
from pathlib import Path
from airflow.hooks.postgres_hook import PostgresHook


pytest_plugins = ['helpers_namespace']
@pytest.fixture
def test_dag():
    dag_id = "my_first_dag"
    dag = DagBag().get_dag(dag_id)
    return dag



def test_task_count(test_dag: DAG):
    print(test_dag)
    assert len(test_dag.tasks) == 5

@pytest.helpers.register
def test_dag_run(test_dag: DAG):
    test_dag.clear()
    test_dag.run(start_date=datetime.datetime(2020,1,1).astimezone(),end_date= datetime.datetime(2020,1,1).astimezone(), donot_pickle=True)
    # path = Path('/tmp/processed_user.csv')
    # assert path.exists()
    hook = PostgresHook(
        postgres_conn_id="postgres",
    )
    rowcount = hook.get_pandas_df("SELECT * from users2")
    assert len(rowcount) > 0
    