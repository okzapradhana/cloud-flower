import apache_beam as beam
import argparse
from apache_beam.io.gcp.internal.clients import bigquery
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
  print("Known args: ", known_args)
  print("Pipeline args: ", pipeline_args)
  p_options = pipeline_options.PipelineOptions(
                pipeline_args,
                temp_location=known_args.gcs_temp_location,
                staging_location=known_args.gcs_stg_location,
                project=known_args.project_id
              )
  source_table_spec = known_args.input
  print("Source Table: ", source_table_spec)
  
  with beam.Pipeline(options=p_options) as pipeline:
    output = ( pipeline 
                | "Read data from BigQuery" >> beam.io.ReadFromBigQuery(table=source_table_spec)
                | "Print read result" >> beam.Map(print)
              )


if __name__ == '__main__':
  run()