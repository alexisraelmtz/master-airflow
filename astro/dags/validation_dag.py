from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, task
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta


def report_failure(context):
    # include this check if you only want to get one email per DAG
    if(ti.xcom_pull(task_ids=None, dag_id=dag_id, key=dag_id) == True):
        logging.info("Other failing task has been notified.")
    send_email = EmailOperator(...)
    send_email.execute(context)


# Default settings applied to all tasks
default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': True,
    'email': 'admin@astro.io',
    'owner': 'turingmachine',
    'on_failure_callback': report_failure
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('example',
         start_date=datetime(2021, 12, 1),
         max_active_runs=3,
         schedule_interval=timedelta(minutes=30),
         default_args=default_args,
         catchup=False  # enable if you don't want historical dag runs to run
         ) as dag:

    t0 = DummyOperator(
        task_id='start'
    )
