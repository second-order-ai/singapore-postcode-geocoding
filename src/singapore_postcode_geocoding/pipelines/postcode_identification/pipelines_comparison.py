from kedro.pipeline import Pipeline, pipeline

from .auto_identification_classes import (
    create_pipeline as test_auto_identification_classes_pipeline,
)


def create_test_auto_identification_classes_pipeline() -> Pipeline:
    return pipeline(
        pipe=test_auto_identification_classes_pipeline(),
        namespace="post_code_identification_classes_",
        inputs={
            "input_data": "ListofGovernmentMarketsHawkerCentres",
            "singapore_postcodes_masterlist": "singapore_postcodes_masterlist",
            "singapore_postcodes_geocoded": "singapore_postcodes_geocoded",
        },
        parameters={
            "auto_identify_config": "auto_identify_config",
            "postcode_validation": "postcode_validation",
            "postcode_master_merge_config": "postcode_master_merge_config",
        },
    )  # type: ignore
