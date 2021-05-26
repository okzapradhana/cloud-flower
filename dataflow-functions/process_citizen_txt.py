import apache_beam as beam
import argparse
import os
from airflow import configuration

PROJECT_ID = "static-gravity-312212"
DATASET_ID = "blankspace_de_dwh"
TABLE_ID = "sample_citizen"
BASE_DIR = os.path.dirname(configuration.conf.get('core', 'dags_folder'))

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
  with beam.Pipeline() as pipeline:
    rows = pipeline | "Read txt file" >> beam.io.ReadFromTextWithFilename(os.path.join(BASE_DIR, 'data', 'citizen.txt'))
    pcoll_json = rows | "Transform txt to json/dict" >> beam.ParDo(TxtTransformer(','))
    dict_records = pcoll_json | "Parse to BigQuery Rows" >> beam.Map(print) 

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
                          table=f'{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}', #PROJECT_ID:DATASET_ID.TABLE
                          schema=bq_table_schema,
                          custom_gcs_temp_location='gs://blank-space-de-batch1-sg/temp',
                          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                          write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

if __name__ == '__main__':
  run()