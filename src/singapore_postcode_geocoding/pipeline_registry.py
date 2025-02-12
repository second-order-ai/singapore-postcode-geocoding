"""Project pipelines."""

from kedro.pipeline import Pipeline

from singapore_postcode_geocoding.pipelines.data_processing import (
    create_pipeline as data_processing,
)
from singapore_postcode_geocoding.pipelines.postcode_identification.pipelines_comparison import (
    create_test_auto_identification_classes_pipeline,
)


def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    data_processing_pipeline = data_processing()
    test_auto_identification_classes_pipeline = (
        create_test_auto_identification_classes_pipeline()
    )
    return {
        "__default__": data_processing_pipeline,
        "data_processing": data_processing_pipeline,
        "test_auto_identification_classes": test_auto_identification_classes_pipeline,
    }
