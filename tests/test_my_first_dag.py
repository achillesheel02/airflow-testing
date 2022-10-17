import pytest
from airflow.models import DagBag

@pytest.fixture
def test_dag():
    dag_id = "my_first_dag"
    dag = DagBag().get_dag(dag_id)
    return dag

def test_task_count(test_dag):
    assert test_dag.tasks == 4
