import os
from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.dummy import DummyOperator
from airflow import configuration
from airflow.models import Variable

default_args = {
  'owner': 'okza',
  'email': 'datokza@gmail.com',
}

PROJECT_ID = (
    os.environ.get('PROJECT_ID') if os.environ.get('ENVIRONMENT') == 'production' 
    else Variable.get('PROJECT_ID'))
KEYWORDS_BQ_TABLE = Variable.get('ALL_KEYWORDS_BQ_OUTPUT_TABLE')
EVENTS_BQ_TABLE = Variable.get('EVENTS_BQ_TABLE')
PRODUCTS_BQ_TABLE = Variable.get('PRODUCTS_BQ_TABLE')

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
              user_id,
              event_id,
              event_name,
              event_datetime,
              state,
              city,
              created_at,
              MAX(IF(param.key = "product_id", param.value.int_value, NULL)) AS product_id
          FROM `static-gravity-312212.unified_events.event`, UNNEST(event_params) AS param
          GROUP BY 1,2,3,4,5,6,7
        ''',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    destination_dataset_table= f'{PROJECT_ID}:{EVENTS_BQ_TABLE}',
    location='US',
    use_legacy_sql=False,
    gcp_conn_id='google_cloud_default'
  )

  bigquery_create_products_task = BigQueryExecuteQueryOperator(
    task_id='execute_query_create_products_table',
    sql='''
          WITH t AS (
            SELECT
                IF(param.key = "product_id", param.value.int_value, NULL) AS product_id,
                IF(param.key = 'product_name', param.value.string_value, NULL) AS product_name,
                IF(param.key = 'product_price', param.value.float_value, NULL) AS product_price,
                IF(param.key = 'product_sku', param.value.string_value, NULL) AS product_sku,
                IF(param.key = 'product_version', param.value.string_value, NULL) AS product_version,
                IF(param.key = 'product_type', param.value.string_value, NULL) AS product_type,
                IF(param.key = 'product_division', param.value.string_value, NULL) AS product_division,
                IF(param.key = 'product_group', param.value.string_value, NULL) AS product_group,
                IF(param.key = 'product_department', param.value.string_value, NULL) AS product_department,
                IF(param.key = 'product_class', param.value.string_value, NULL) AS product_class,
                IF(param.key = 'product_subclass', param.value.string_value, NULL) AS product_subclass,
                IF(param.key = 'product_is_book', param.value.bool_value, NULL) AS product_is_book,
            FROM `static-gravity-312212.unified_events.event`, UNNEST(event_params) AS param
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
          )

          SELECT * FROM t WHERE t.product_id IS NOT NULL
        ''',
    write_disposition='WRITE_TRUNCATE',
    create_disposition='CREATE_IF_NEEDED',
    destination_dataset_table= f'{PROJECT_ID}:{PRODUCTS_BQ_TABLE}',
    location='US',
    use_legacy_sql=False,
    gcp_conn_id='google_cloud_default'
  )

  start = DummyOperator(task_id='start')
  end = DummyOperator(task_id='end')

  start >> bigquery_create_events_task >> bigquery_create_products_task >> end
  
reverse_transactional_events_etl = reverse_transactional_events_dag()