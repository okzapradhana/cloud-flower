import argparse
import apache_beam as beam
from apache_beam.options import pipeline_options

class RowTransformer(beam.DoFn):
  def __init__(self, delimiter):
    self.delimiter = delimiter

  def process(self, row):
    import csv
    from datetime import datetime
    csv_object = csv.reader(row.splitlines(), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
    user_id, search_keyword, search_result_count, created_at = next(csv_object)
    created_date = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S %Z').strftime('%Y-%m-%d')
    return [{'user_id': int(user_id), 'search_keyword': search_keyword, 
             'search_result_count': int(search_result_count), 'created_date': created_date}]

def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument(
        '--input_file', dest='input', required=True,
        help='Input file to read.  This can be a local file or '
             'a file in a Google Storage Bucket.')
  parser.add_argument('--output', dest='output', required=True,
                        help='Output BQ table to write results to.')
  parser.add_argument('--temp_location', dest='gcs_temp_location', required=True,
                        help='GCS Temp Directory to store Temp data before write to BQ')                      
  parser.add_argument('--staging_location', dest='gcs_stg_location', required=True,
                        help='GCS Staging Directory to store staging data before write to BQ')                      
  parser.add_argument('--project', dest='project_id', required=True,
                        help='Project ID which GCP linked on.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  p_options = pipeline_options.PipelineOptions(
                pipeline_args, 
                temp_location=known_args.gcs_temp_location,
                staging_location=known_args.gcs_stg_location,
                project=known_args.project_id)

  with beam.Pipeline(options=p_options) as pipeline:
    bq_table_schema = {
      "fields": [
        {
          "mode": "REQUIRED",
          "name": "user_id",
          "type": "INTEGER"
        },
        {
          "mode": "NULLABLE",
          "name": "search_keyword",
          "type": "STRING"
        },
        {
          "mode": "NULLABLE",
          "name": "search_result_count",
          "type": "INTEGER"
        },
        {
          "mode": "NULLABLE",
          "name": "created_date",
          "type": "DATE"
        }
      ]
    }

    output = ( pipeline 
              | "Read CSV file from GCS" >> beam.io.ReadFromText(file_pattern=known_args.input, skip_header_lines=1)
              | "Transform Data" >> beam.ParDo(RowTransformer(','))
              | "Write data to BigQuery" >> beam.io.WriteToBigQuery(
                                      table=f'{known_args.project_id}:{known_args.output}',
                                      schema=bq_table_schema,
                                      custom_gcs_temp_location=known_args.gcs_temp_location,
                                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                      additional_bq_parameters = {'timePartitioning': {
                                          'type': 'DAY'
                                      }})
            )

if __name__ == '__main__':
  run()