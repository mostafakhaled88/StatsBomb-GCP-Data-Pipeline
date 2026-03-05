# dataflow/utils/pipeline_options.py

from apache_beam.options.pipeline_options import PipelineOptions


def get_pipeline_options(
    runner: str = "DirectRunner",
    project: str = "football-analytics-project",
    region: str = "us-central1",
    temp_location: str = None,
    staging_location: str = None,
):
    """
    Returns Apache Beam PipelineOptions with defaults for GCS temp and staging locations.

    Args:
        runner (str): "DirectRunner" or "DataflowRunner"
        project (str): GCP project ID
        region (str): GCP region
        temp_location (str): GCS temp location, optional
        staging_location (str): GCS staging location, optional

    Returns:
        PipelineOptions object
    """
    # Set defaults if not provided
    if temp_location is None:
        temp_location = "gs://statsbomb-temp/events_temp"
    if staging_location is None:
        staging_location = "gs://statsbomb-temp/events_staging"

    return PipelineOptions(
        runner=runner,
        project=project,
        region=region,
        temp_location=temp_location,
        staging_location=staging_location,
        save_main_session=True,
        setup_file="./setup.py"
    )
