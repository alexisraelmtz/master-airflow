from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, task
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta


default_args = {
    'retries': 2,
    'retry_delay': timedelta(seconds=5)
    # 'email_on_failure': True
    # 'email_on_retry': True,
    # 'email': 'admin@astro.io'
}


def _greeting(my_param):
    if my_param:
        return "Hello World from OpenShift + Airflow"
    return "Expected Failure - Exit Code 0"


with DAG(dag_id='start',
         default_args=default_args,
         start_date=days_ago(1),  # datetime(2021, 12, 9)
         schedule_interval='@daily',  # None '@daily' '*/10 * * * *', '*/5 * * * *'
         catchup=False,
         max_active_runs=5) as dag:

    init_task = DummyOperator(
        task_id='init_task'
    )

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=_greeting,
        op_kwargs={'my_param': True}
    )

    check_data = PythonOperator(
        task_id='check_data',
        python_callable=_greeting,
        op_kwargs={'my_param': False}
    )

    # right big Shift >> Dependecies(edges in DAG)
    init_task >> extract_data >> check_data


# AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
# AIRFLOW__SMTP__SMTP_USER=youremail@gmail.com
# AIRFLOW__SMTP__SMTP_PASSWORD=abasdfscasdfasdfsdf
# AIRFLOW__SMTP__SMTP_PORT=587
# AIRFLOW__SMTP__SMTP_MAIL_FROM=Airflow
