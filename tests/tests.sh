#!/bin/bash
pip install apache-airflow pytest
airflow connections add postgres \
          --conn-type postgres \
          --conn-host postgres \
          --conn-port 5432 \
          --conn-login airflow \
          --conn-password airflow 

airflow connections add user_data \
          --conn-type http \
          --conn-host  https://random-data-api.com/api/v2/ 

pytest -rs