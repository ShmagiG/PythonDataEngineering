from datetime import datetime, timedelta
import random
import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
import create_references, create_hubs, create_links, create_satelites

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 2, 9)
}

FILE_PATH = "/airflow/data/hosts_existing_7_Sep_2022.csv"

JOB_1_PATH = "/airflow/jobs/create_references.py"
JOB_2_PATH = "/airflow/jobs/create_hubs.py"
JOB_3_PATH = "/airflow/jobs/create_satelites.py"
JOB_4_PATH = "/airflow/jobs/create_links.py"

Initial_loading_dag = DAG(
    dag_id='Initial_loading_dag',
    default_args=default_args,
    concurrency=1
)

create_references = PythonOperator(
    python_callable=create_references.create_references,
    task_id="create_references",
    start_date=days_ago(1),
    dag=Initial_loading_dag,
)

create_hubs = PythonOperator(
    python_callable=create_hubs.create_hubs,
    task_id="create_hubs",
    start_date=days_ago(1),
    dag=Initial_loading_dag,
)
create_satelites = PythonOperator(
    python_callable=create_satelites.create_satelites,
    task_id="create_satelites",
    start_date=days_ago(1),
    dag=Initial_loading_dag,
)
create_links = PythonOperator(
    python_callable=create_links.create_links,
    task_id="create_links",
    start_date=days_ago(1),
    dag=Initial_loading_dag,
)

data_loaded = DummyOperator(
    task_id='data_loaded',
    dag=Initial_loading_dag,
    trigger_rule='all_success'
)

create_references >> create_hubs >> [create_satelites, create_links]  >> data_loaded