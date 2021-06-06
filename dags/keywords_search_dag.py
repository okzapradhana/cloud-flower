import os
from airflow.decorators import dag
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import configuration
from airflow.models import Variable
from datetime import datetime

default_args = {
    'owner': 'okza',
    'email': 'datokza@gmail.com',
}

COMPOSER_BUCKET_NAME = Variable.get("COMPOSER_BUCKET_NAME")
BUCKET_NAME = Variable.get('BUCKET_NAME')
OUTPUT = Variable.get('ALL_KEYWORDS_BQ_OUTPUT_TABLE')
PY_FILE = (
    f'gs://{COMPOSER_BUCKET_NAME}/dataflow-functions/csv_gcs_to_bigquery.py' if Variable.get('ENVIRONMENT') == 'production'
    else f"{os.path.dirname(configuration.conf.get('core', 'dags_folder'))}/dataflow-functions/csv_gcs_to_bigquery.py")
PROJECT_ID = Variable.get('PROJECT_ID')
GCS_TEMP_LOCATION = Variable.get('GCS_TEMP_LOCATION')
GCS_STG_LOCATION = Variable.get('GCS_STG_LOCATION')
INPUT_PATH = f'gs://{BUCKET_NAME}/keyword-searches'


@dag(
    default_args=default_args,
    schedule_interval='0 5 * * *',
    start_date=datetime(2021, 3, 10),
    end_date=datetime(2021, 3, 16),
    tags=['dataflow-job']
)
def keywords_search_dag():
    pipeline_options = {'tempLocation': GCS_TEMP_LOCATION,
                        'inputFile': '{}/{}'.format(INPUT_PATH,
                                                    'keyword_search_search_{{ ds_nodash }}.csv'),
                        'output': OUTPUT,
                        'stagingLocation': GCS_STG_LOCATION,
                        }

    dataflow_task = BeamRunPythonPipelineOperator(
        task_id='job_process_keyword_search_gcs_to_bigquery',
        runner='DataflowRunner',
        gcp_conn_id='google_cloud_default',
        py_file=PY_FILE,
        py_requirements=['apache-beam[gcp]==2.29.0'],
        py_system_site_packages=True,
        py_interpreter='python3',
        pipeline_options=pipeline_options,
        dataflow_config=DataflowConfiguration(
            job_name="job_keyword_search_{{ ds_nodash }}",
            project_id=PROJECT_ID,
            location="asia-south1",
            wait_until_finished=True
        )
    )

    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_most_searched_keyword_dag',
        trigger_dag_id='most_searched_keyword_dag',
        wait_for_completion=False,
        reset_dag_run=True,
        poke_interval=30,
    )

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    start >> dataflow_task >> trigger_next_dag >> end


keyword_search_etl = keywords_search_dag()
