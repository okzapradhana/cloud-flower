import apache_beam as beam
import argparse
import os
from airflow import configuration

PROJECT_ID = "static-gravity-312212"
DATASET_ID = "blankspace_de_dwh"
TABLE_ID = "exercise_citizen_data" if os.environ.get("ENVIRONMENT") == "production" else "dev_exercise_citizen_data"
TEMP_BUCKET_LOCATION = 'gs://blank-space-de-batch1-sg/tmp'
CONF_BASE_DIR = os.path.dirname(configuration.conf.get('core', 'dags_folder'))
BASE_DIR = f"gs://{os.environ.get('BUCKET_NAME')}" if os.environ.get('ENVIRONMENT') == 'production' else CONF_BASE_DIR

class TxtTransformer(beam.DoFn):
  def __init__(self, delimiter):
    self.delimiter = delimiter

  def process(self, line):
    filename, value = line
    print(f"Filename: {filename}")
    print(f"Value: {value}")

    value = value.split(self.delimiter)
    return [{
      'name': value[0],
      'citizen_number': value[1],
      'hobby': value[2],
      'favorite_game': value[3],
      'birth_date': value[4]
    }]

def run(argv=None):
  print(f"Base Directory {BASE_DIR}")
  with beam.Pipeline() as pipeline:
    file_path =  f'{BASE_DIR}/data/citizen.txt' if "gs://" in BASE_DIR else os.path.join(CONF_BASE_DIR, 'data', 'citizen.txt')
    print(f"File source path {file_path}")
    rows = pipeline | "Read txt file" >> beam.io.ReadFromTextWithFilename(file_path)
    pcoll_json = rows | "Transform txt to json/dict" >> beam.ParDo(TxtTransformer(','))
    dict_records = pcoll_json | "Print P Collection Json Value" >> beam.Map(print) 

    bq_table_schema = {
      "fields": [
        {
          "mode": "REQUIRED",
          "name": "name",
          "type": "STRING"
        },
        {
          "mode": "REQUIRED",
          "name": "citizen_number",
          "type": "INTEGER"
        },
        {
          "mode": "NULLABLE",
          "name": "hobby",
          "type": "STRING"
        },
        {
          "mode": "NULLABLE",
          "name": "favorite_game",
          "type": "STRING"
        },
        {
          "mode": "NULLABLE",
          "name": "birth_date",
          "type": "DATE"
        },
      ]
    }

    pcoll_json | "Store data to BigQuery" >> beam.io.WriteToBigQuery(
                          table=f'{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}',
                          schema=bq_table_schema,
                          custom_gcs_temp_location=TEMP_BUCKET_LOCATION,
                          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                          write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

if __name__ == '__main__':
  run()