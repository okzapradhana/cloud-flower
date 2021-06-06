from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

default_args = {
    'owner': 'okza',
    'email': 'datokza@gmail.com',
}

PROJECT_ID = Variable.get('PROJECT_ID')
KEYWORDS_BQ_TABLE = Variable.get('ALL_KEYWORDS_BQ_OUTPUT_TABLE')
EVENTS_BQ_TABLE = Variable.get('EVENTS_BQ_TABLE')


@dag(
    default_args=default_args,
    schedule_interval='0 0 */3 * *',
    start_date=days_ago(1),
    tags=['reverse-engineering', 'events']
)
def reverse_transactional_events_dag():
    bigquery_create_events_task = BigQueryExecuteQueryOperator(
        task_id='execute_query_create_events_table',
        sql='''
      SELECT
          MAX(IF(param.key = "transaction_id", param.value.int_value, NULL)) AS transaction_id,
          MAX(IF(param.key = "transaction_detail_id", param.value.int_value, NULL)) AS transaction_detail_id,
          MAX(IF(param.key = "transaction_number", param.value.int_value, NULL)) AS transaction_number,
          MAX(IF(param.key = "transaction_datetime", param.value.string_value, NULL)) AS transaction_datetime,
          MAX(IF(param.key = "purchase_quantity", param.value.int_value, NULL)) AS purchase_quantity,
          MAX(IF(param.key = "purchase_amount", param.value.float_value, NULL)) AS purchase_amount,
          MAX(IF(param.key = "purchase_payment_method", param.value.string_value, NULL)) AS purchase_payment_method,
          MAX(IF(param.key = "purchase_source", param.value.string_value, NULL)) AS purchase_source,
          MAX(IF(param.key = "product_id", param.value.int_value, NULL)) AS product_id,
          user_id,
          state,
          city,
          created_at,
          MAX(IF(param.key = "ext_created_at", param.value.string_value, NULL)) AS ext_created_at
      FROM `static-gravity-312212.unified_events.event`, UNNEST(event_params) AS param
      GROUP BY
          user_id,
          state,
          city,
          created_at
      ''',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        destination_dataset_table=f'{PROJECT_ID}:{EVENTS_BQ_TABLE}',
        location='US',
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default'
    )

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    start >> bigquery_create_events_task >> end


reverse_transactional_events_etl = reverse_transactional_events_dag()
