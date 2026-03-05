import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io import fileio

from dataflow.transforms.parse_events import ParseEventsFn
from dataflow.schemas.events_schema import EVENTS_SCHEMA
from dataflow.schemas.lineups_schema import LINEUP_SCHEMA

class EventsPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--input_path", required=True, help="GCS path to JSON match files")
        parser.add_argument("--output_table", required=True, help="Events table (project:dataset.table)")
        parser.add_argument("--lineup_table", required=True, help="Lineups table")
        parser.add_argument("--deadletter_table", required=True, help="Parsing errors table")
        parser.add_argument("--bq_errors_table", required=True, help="BQ Insert errors table")

def run():
    # Performance Tip: save_main_session=True is critical for pickling custom logic
    pipeline_options = PipelineOptions(save_main_session=True)
    custom_options = pipeline_options.view_as(EventsPipelineOptions)
    
    # Standard GCP options (Project/Region) should be passed via CLI
    with beam.Pipeline(options=pipeline_options) as p:

        # 1. Read files and Extract Contents
        files = (
            p
            | "Match Event Files" >> fileio.MatchFiles(custom_options.input_path)
            | "Read Event Files" >> fileio.ReadMatches()
            | "Extract Contents" >> beam.Map(lambda f: (f.metadata.path, f.read_utf8()))
            | "Reshuffle Workers" >> beam.Reshuffle() # Balances load across worker nodes
        )

        # 2. Parse Step with 3 outputs: valid, lineups, and failed (Parsing errors)
        parsed_results = (
            files
            | "Parse Events & Lineups" >> beam.ParDo(ParseEventsFn()).with_outputs(
                "lineups", "failed", main="valid"
            )
        )

        # 3. Sink: Valid Events + Capture BQ Insert Failures
        event_errors = (
            parsed_results.valid 
            | "Write Events" >> WriteToBigQuery(
                table=custom_options.output_table,
                schema=EVENTS_SCHEMA,
                method="STREAMING_INSERTS",
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_IF_NEEDED",
                insert_retry_strategy="RETRY_ON_TRANSIENT_ERROR"
            )
        )

        # 4. Sink: Lineups
        lineup_errors = (
            parsed_results.lineups 
            | "Write Lineups" >> WriteToBigQuery(
                table=custom_options.lineup_table,
                schema=LINEUP_SCHEMA,
                method="STREAMING_INSERTS",
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_IF_NEEDED"
            )
        )

        # 5. Sink: Parsing Dead-Letter (Logic/JSON errors)
        (
            parsed_results.failed 
            | "Write Parsing Errors" >> WriteToBigQuery(
                table=custom_options.deadletter_table,
                schema='file_name:STRING, error:STRING, timestamp:TIMESTAMP',
                write_disposition="WRITE_APPEND"
            )
        )

        # 6. Sink: BigQuery Insert Errors (Schema/Type mismatches)
        # We merge insert failures from both tables into one audit table
        # 6. Sink: BigQuery Insert Errors (Schema/Type mismatches)
        # In your run() function, replace the Log BQ Failures block with this:
        (
            (event_errors['FailedRows'], lineup_errors['FailedRows'])
            | "Flatten BQ Failures" >> beam.Flatten()
            | "Format BQ Error" >> beam.Map(lambda x: {
                "failed_row_data": str(x[1]),  # Extract the actual data dictionary
                "destination_table": str(x[0]) # Extract the table it tried to hit
            })
            | "Write BQ Failures" >> WriteToBigQuery(
                table=custom_options.bq_errors_table,
                schema='failed_row_data:STRING, destination_table:STRING',
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND"
            )
        )
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()