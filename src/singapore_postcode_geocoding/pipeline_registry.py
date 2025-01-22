"""Project pipelines."""

from kedro.pipeline import Pipeline

from singapore_postcode_geocoding.pipelines.data_processing import (
    create_pipeline as data_processing,
)


def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    data_processing_pipeline = data_processing()
    return {
        "__default__": data_processing_pipeline,
        "data_processing": data_processing_pipeline,
    }
