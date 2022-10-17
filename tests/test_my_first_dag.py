import pytest
from pytest_docker_tools import build
from airflow.api.client.local_client import Client


def test_my_first_dag():
    c = Client(None, None)
    c.trigger_dag(dag_id='my_first_dag', run_id='t1', conf={})