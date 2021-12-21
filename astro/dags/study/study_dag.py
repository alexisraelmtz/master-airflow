from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, task
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os
# from airflow.models.baseoperator import chain, cross_downstream

owner = ((os.path.dirname(os.path.abspath(__file__)).split("/"))[-1]).upper()

default_args = {
    'owner': owner,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    # 'email_on_failure': True
    # 'email_on_retry': True,
    # 'email': 'admin@astro.io'
}


def _extraction(my_param, ds, **kwargs):  # context
    # print("initializing DB extraction\nLoading ...")
    if my_param:
        with open('/tmp/my_file.txt', 'w') as file:
            file.write(f"Log:\n{ds}")
        print("\nOperation Sucessful - Exit Code 0.")
    else:
        print("Operation FAILED - Exit Code 1.")
    return 10  # Value de lo que quieras
    (f"{ds}\n - Inyected data to File")


def _validation(ti):  # Task Instance: 'ti'
    cross_com = ti.xcom_pull(key='return_value', task_ids=['extract_data'])
    # cross_com= 10
    print(f"{cross_com}\nValidating\nLoading ...\n")


with DAG(dag_id='study',
         default_args=default_args,
         start_date=days_ago(5),  # days_ago(2)
         schedule_interval='@daily',  # None '@daily' '*/10 * * * *', '*/5 * * * *'
         catchup=False,
         max_active_runs=5) as dag:

    init_task = DummyOperator(
        task_id='init_task'
        # on_failure_callback=_script
        # email_on_failure= True
    )

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=_extraction,
        op_kwargs={'my_param': True}
    )

    check_data = PythonOperator(
        task_id='check_data',
        python_callable=_validation
    )

    test_file = FileSensor(
        task_id='test_file',
        fs_conn_id='fs_default',
        filepath='my_file.txt',
        poke_interval=5
    )

    proccess_data = BashOperator(
        task_id='proccess_data',
        bash_command='cat /tmp/my_file.txt && exit 0'
    )

    # right big Shift -> Dependecies(edges in DAG)
    init_task >> extract_data >> check_data >> test_file >> proccess_data

    # proccess_data << test_file << check_data << extract_data << init_task
    # chain(init_task, extract_data, test_file, proccess_data)
    # cross_downstream([check_data, extract_data], [
    #     test_file, proccess_data])
    # set_upstream && set_downstream


# CrossCommunication -> excom
# SMPT Server -> .cfg


# Default: Sequential Executor -> (Default DB: SQLite)
# Custom: Local executor --    -> (DB: Postgresql, MariaDB)
# Celery executor              -> Mas NODES
# WebServer > DB > Queue > Scheduler > Workers > ResultsBroker

# Parallelism = 32              --- max Tasks for all AF-instances at the same time
# DAG_concurrency = 16          --- max Task per DAG
# Max_active_run_per_DAG = 16   --- max runs for all DAGs at the same time per/Day
# Max_active_runs = 6           --- max runs for 1 DAG
# Concurrency = 1               --- max tasks for 1 DAG


# Operatos
# - Action
# - Transfer
# - Sensor

# Task Instance object = repr. specific RUN of a Task: DAG + TASK + Point in time
# Last Run = begining of the scheduled period. >> Latest execution date.
