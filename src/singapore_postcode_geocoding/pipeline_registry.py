"""Project pipelines."""

from kedro.pipeline import Pipeline

from singapore_postcode_geocoding.pipelines.data_synthesis import (
    create_pipeline as create_test_postcode_data_pipeline,
)

from singapore_postcode_geocoding.pipelines.master_data_processing import (
    create_pipeline as master_data_processing_pipeline,
)
from singapore_postcode_geocoding.pipelines.postcode_identification.pipelines_comparison import (
    create_test_auto_identification_classes_pipeline,
)


def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    return {
        "__default__": master_data_processing_pipeline(),
        "master_data_processing": master_data_processing_pipeline(),
        "test_auto_identification_classes": create_test_auto_identification_classes_pipeline(),
        "test_create_postcode_data": create_test_postcode_data_pipeline(),
    }
