import datetime
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from etl import load

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'house_dag',
    default_args=default_args,
    description='group1_project',
)

load = load
task1 = PythonOperator(
    task_id='housing',
    python_callable=load,
    dag=dag,
)

task1
