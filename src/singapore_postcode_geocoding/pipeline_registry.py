"""Project pipelines."""

from kedro.pipeline import Pipeline

from singapore_postcode_geocoding.pipelines.master_data_processing import (
    create_pipeline as master_data_processing,
)
from singapore_postcode_geocoding.pipelines.postcode_identification.pipelines_comparison import (
    create_test_auto_identification_classes_pipeline,
)


def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    master_data_processing_pipeline = master_data_processing()
    test_auto_identification_classes_pipeline = (
        create_test_auto_identification_classes_pipeline()
    )
    return {
        "__default__": master_data_processing_pipeline,
        "master_data_processing": master_data_processing_pipeline,
        "test_auto_identification_classes": test_auto_identification_classes_pipeline,
    }
