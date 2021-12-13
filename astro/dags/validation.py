from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, task
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta


def report_failure(context):
    # include this check if you only want to get one email per DAG
    if(ti.xcom_pull(task_ids=None, dag_id=dag_id, key=dag_id) == True):
        logging.info("Other failing task has been notified.")
    send_email = EmailOperator(...)
    send_email.execute(context)


'''

dag = DAG(
    ...,
    default_args={
        ...,
        "on_failure_callback": report_failure
    }
)

'''
