import apache_beam as beam
import argparse
from apache_beam.options import pipeline_options

def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument('--input_table', dest='input', required=True,
                      help='BigQuery table source to Read')
  parser.add_argument('--output', dest='output', required=True,
                      help='BigQuery table target to Write')
  parser.add_argument('--temp_location', dest='gcs_temp_location', required=True,
                      help='GCS Temp Directory to store Temp data before write to BQ')
  parser.add_argument('--staging_location', dest='gcs_stg_location', required=True,
                      help='GCS Staging Directory to store staging data before write to BQ')
  parser.add_argument('--project', dest='project_id', required=True,
                      help='Project ID corresponding to the GCP owner')
  known_args, pipeline_args = parser.parse_known_args(argv)
  p_options = pipeline_options.PipelineOptions(
                pipeline_args,
                temp_location=known_args.gcs_temp_location,
                staging_location=known_args.gcs_stg_location,
                project=known_args.project_id
              )
  
  with beam.Pipeline(options=p_options) as pipeline:
    bq_table_schema = {
      "fields": [
        {
          "mode": "REQUIRED",
          "name": "search_keyword",
          "type": "STRING"
        },
        {
          "mode": "REQUIRED",
          "name": "created_date",
          "type": "DATE"
        },
        {
          "mode": "REQUIRED",
          "name": "search_count",
          "type": "INTEGER"
        },
      ]
    }
    
    output = ( pipeline 
                | "Read data from BigQuery" >> beam.io.ReadFromBigQuery(
                                  query='WITH partitioned_keyword_search AS (' 
                                          'SELECT '
                                          'search_keyword,' 
                                          'created_date,'
                                          'COUNT(search_result_count) AS search_count,' 
                                          'ROW_NUMBER() OVER(PARTITION BY created_date ORDER BY COUNT(search_result_count) DESC ) AS row_number ' \
                                          'FROM blankspace_de_dwh.keyword_searches '
                                          'GROUP BY created_date,search_keyword ) ' \

                                        'SELECT ' 
                                          'search_keyword,'
                                          'created_date,'
                                          'search_count ' \
                                      'FROM partitioned_keyword_search pks '
                                      'WHERE pks.row_number = 1 '
                                      'ORDER BY pks.created_date'
                                  ,
                                  use_standard_sql=True)
                | "Write data to BigQuery" >> beam.io.WriteToBigQuery(
                                      table=f'{known_args.project_id}:{known_args.output}',
                                      schema=bq_table_schema,
                                      custom_gcs_temp_location=known_args.gcs_temp_location,
                                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                  )
    )


if __name__ == '__main__':
  run()