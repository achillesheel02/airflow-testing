from airflow.models import DagBag
import glob
import os
import datetime
from pathlib import Path
from airflow.hooks.postgres_hook import PostgresHook

dag_id = "my_first_dag"
# dag = DagBag().get_dag(dag_id)
# # print(dag)
# for dag in DagBag().dags:
#     print(dag)


DAG_PATH = os.path.join("dags/**/*.py")
DAG_FILES = glob.glob(DAG_PATH, recursive=True)
print(DAG_PATH)


dag = DagBag().get_dag(dag_id)
dag.run(start_date=datetime.datetime(2019,1,1).astimezone(),end_date= datetime.datetime(2019,1,2).astimezone())
path = Path('/tmp/processed_user.csv')
print(path.exists())

hook = PostgresHook(
        postgres_conn_id="postgres",
    )
rowcount = hook.get_pandas_df("SELECT * from users2")
print(rowcount)