import datetime
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import json
from pandas import json_normalize


def _process_user(ti):
    user = ti.xcom_pull(task_ids="extract_user")
    user = user["results"][0]
    processed_user = json_normalize(
        {
            "fistname": user["name"]["first"],
            "lastname": user["name"]["last"],
            "username": user["login"]["username"],
            "password": user["login"]["password"],
            "email": user["email"],
        }
    )
    processed_user.to_csv("/tmp/processed_user.csv", index=None, header=False)


def _store_user():
    hook = PostgresHook(
        postgres_conn_id="postgres",
    )
    hook.copy_expert(
        sql="COPY users2 FROM stdin WITH DELIMITER AS ','",
        filename="/tmp/processed_user.csv",
    )


dag = DAG(
    dag_id="my_first_dag",
    schedule_interval="@daily",
    start_date=datetime.datetime(2022, 9, 9),
    catchup=False,
)
create_table = PostgresOperator(
    dag=dag,
    postgres_conn_id="postgres",
    task_id="create_table",
    sql="""
        CREATE TABLE IF NOT EXISTS users2 (
            firstname TEXT NOT NULL,
            lastname TEXT NOT NULL,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
            email TEXT NOT NULL
        );
        """,
)

is_api_available = HttpSensor(
    dag=dag, task_id="is_api_available", http_conn_id="user_api", endpoint="api/"
)

extract_user = SimpleHttpOperator(
    dag=dag,
    task_id="extract_user",
    http_conn_id="user_api",
    endpoint="api/",
    method="GET",
    response_filter=lambda response: json.loads(response.text),
    log_response=True,
)

process_user = PythonOperator(
    dag=dag, task_id="process_user", python_callable=_process_user
)

store_user = PythonOperator(dag=dag, task_id="store_user", python_callable=_store_user)

create_table >> is_api_available >> extract_user >> process_user >> store_user
