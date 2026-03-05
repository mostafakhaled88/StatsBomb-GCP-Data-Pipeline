import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.gcp.bigquery import WriteToBigQuery

from dataflow.schemas.matches_schema import MATCHES_SCHEMA
from dataflow.transforms.parse_matches import ParseMatchesFn
from dataflow.utils.pipeline_options import get_pipeline_options

# Configuration - Ensure these match your GCP project and bucket
INPUT_PATH = "gs://statsbomb-raw-data/matches/**/matches.json"
BQ_TABLE = "civil-hull-489201-q7:statsbomb_raw.matches"
TEMP_GCS_LOCATION = "gs://statsbomb-raw-data/temp/"

def run():
    # Load pipeline options from utils
    options = get_pipeline_options()

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Match Files" >> fileio.MatchFiles(INPUT_PATH)
            | "Read Files" >> fileio.ReadMatches()
            # Extract path (for ingestion_date) and file content
            | "Get Path & Content" >> beam.Map(lambda f: (f.metadata.path, f.read_utf8()))
            | "Parse Matches" >> beam.ParDo(ParseMatchesFn())
            | "Write to BigQuery" >> WriteToBigQuery(
                table=BQ_TABLE,
                schema=MATCHES_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method="FILE_LOADS", 
                # FIX: Explicitly set the staging area to avoid 'statsbomb-temp' forbidden error
                custom_gcs_temp_location=TEMP_GCS_LOCATION
            )
        )

if __name__ == "__main__":
    run()