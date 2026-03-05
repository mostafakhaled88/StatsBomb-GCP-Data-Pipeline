import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.gcp.bigquery import WriteToBigQuery

from dataflow.schemas.competitions_schema import COMPETITIONS_SCHEMA
from dataflow.transforms.parse_competitions import ParseCompetitionsFn
from dataflow.utils.pipeline_options import get_pipeline_options

INPUT_PATH = "gs://statsbomb-raw-data/competitions/ingestion_date=*/*.json"
BQ_TABLE = "civil-hull-489201-q7:statsbomb_raw.competitions"

def run():
    options = get_pipeline_options() # Logic for runner/project moved to utils

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Match Raw Files" >> fileio.MatchFiles(INPUT_PATH)
            | "Read Raw Files" >> fileio.ReadMatches()
            # NEW: Pass a tuple of (path, content) to the parser
            | "Extract Path & Content" >> beam.Map(lambda f: (f.metadata.path, f.read_utf8()))
            | "Parse & Flatten JSON" >> beam.ParDo(ParseCompetitionsFn())
            | "Write to BigQuery" >> WriteToBigQuery(
                table=BQ_TABLE,
                schema=COMPETITIONS_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                # Optimization: High-throughput inserts
                method="STREAMING_INSERTS" 
            )
        )

if __name__ == "__main__":
    run()