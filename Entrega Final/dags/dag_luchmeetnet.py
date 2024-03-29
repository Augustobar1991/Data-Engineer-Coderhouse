from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import DAG, Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from functools import partial

from scripts.main import load_weather_data,enviar

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="dag_luchmeetnet",
    start_date=datetime(2023, 11, 28),
    catchup=False,
    schedule_interval="0 * * * *",
    default_args=default_args
) as dag:

    # task con dummy operator
    dummy_start_task = DummyOperator(
        task_id="start"
    )

    create_tables_task = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="coderhouse_redshift",
        sql="sql/creates.sql",
        hook_params={	
            "options": "-c search_path=augustobar1991_coderhouse"
        }
    )

    load_weather_data_task = PythonOperator(
        task_id="load_weather_data_v1",
        python_callable=load_weather_data,
        op_kwargs={
            "config_file": "/opt/airflow/config/config.ini",
            "start": "{{ data_interval_start }}",
            "end": "{{ data_interval_end }}"
        }
    )

    report_email = PythonOperator(
        task_id='sending_email',
        python_callable=partial(enviar, password_gmail=Variable.get('GMAIL_SECRET'))
    )

    dummy_end_task = DummyOperator(
        task_id="end"
    )

    dummy_start_task >> create_tables_task
    create_tables_task >> load_weather_data_task
    load_weather_data_task >> report_email
    report_email >> dummy_end_task