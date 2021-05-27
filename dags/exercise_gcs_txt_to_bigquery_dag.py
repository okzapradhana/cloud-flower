import json
import os
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
#from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator -- This class is deprecated, use DataflowCreatePythonJobOperator instead
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow import configuration

default_args = {
    'owner': 'okza',
    'email': ['okzamahendra29@gmail.com'],
}

CONF_BASE_DIR = os.path.dirname(configuration.conf.get('core', 'dags_folder'))
BASE_DIR = f"gs://{os.environ.get('BUCKET_NAME')}" if os.environ.get('ENVIRONMENT') == 'production' else CONF_BASE_DIR
DATAFLOW_FUNCTIONS_PATH =  f'{BASE_DIR}/dataflow-functions/process_citizen_txt.py' if "gs://" in BASE_DIR else os.path.join(CONF_BASE_DIR, 'dataflow-functions', 'process_citizen_txt.py')

@dag(default_args=default_args, schedule_interval='0 6 * * *', start_date=days_ago(1), tags=['dataflow-job'])
def exercise_gcs_txt_to_bigquery_dag():
    txt_bq_dataflow_job = DataflowCreatePythonJobOperator(
        task_id='etl_txt_to_bq_dataflow_job',
        gcp_conn_id='google_cloud_default',
        py_file=DATAFLOW_FUNCTIONS_PATH,
        job_name='{{task.task_id}}',
        py_interpreter='python3',
        location='asia-south1'
    )


txt_to_bigquery_dataflow_etl = exercise_gcs_txt_to_bigquery_dag()
