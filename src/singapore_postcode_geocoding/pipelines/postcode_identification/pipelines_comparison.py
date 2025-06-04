from kedro.pipeline import Pipeline, pipeline

from .auto_identification_classes_pipeline.pipeline import (
    create_pipeline as test_auto_identification_classes_pipeline,
    create_manual_pipeline as test_manual_identification_classes_pipeline,
)


def create_test_auto_identification_classes_pipeline() -> Pipeline:
    return pipeline(
        pipe=test_auto_identification_classes_pipeline(),
        namespace="post_code_identification_classes_indirect_small",
        inputs={
            "input_data": "ListofGovernmentMarketsHawkerCentres",
            "singapore_postcodes_masterlist": "singapore_postcodes_masterlist",
            "singapore_postcodes_geocoded": "singapore_postcodes_geocoded",
        },
        parameters={
            "auto_identify_config": "auto_identify_config",
            "postcode_validation": "postcode_validation",
            "postcode_master_merge_config": "postcode_master_merge_config",
        },  # type: ignore
    ) + pipeline(
        pipe=test_auto_identification_classes_pipeline(),
        namespace="post_code_identification_classes_direct_large",
        inputs={
            "input_data": "EntitiesRegisteredwithACRA",
            "singapore_postcodes_masterlist": "singapore_postcodes_masterlist",
            "singapore_postcodes_geocoded": "singapore_postcodes_geocoded",
        },
        parameters={
            "auto_identify_config": "auto_identify_config",
            "postcode_validation": "postcode_validation",
            "postcode_master_merge_config": "postcode_master_merge_config",
        },
    )  # type: ignore  # type: ignore


def create_test_manual_identification_classes_pipeline() -> Pipeline:
    """Create test pipelines for manual postcode identification.
    
    Creates two test pipelines:
    1. For small dataset (ListofGovernmentMarketsHawkerCentres) using indirect method
    2. For large dataset (EntitiesRegisteredwithACRA) using direct method
    
    Both pipelines use the same test files as the automatic identification pipeline
    but with manually specified configuration from parameters.yml.
    """
    return pipeline(
        pipe=test_manual_identification_classes_pipeline(),
        namespace="post_code_identification_manual_indirect_small",
        inputs={
            "input_data": "ListofGovernmentMarketsHawkerCentres",
            "singapore_postcodes_masterlist": "singapore_postcodes_masterlist",
            "singapore_postcodes_geocoded": "singapore_postcodes_geocoded",
        },
        parameters={
            "manual_postcode_config": "manual_postcode_config_indirect",
            "postcode_validation": "postcode_validation",
            "postcode_master_merge_config": "postcode_master_merge_config",
        },  # type: ignore
    ) + pipeline(
        pipe=test_manual_identification_classes_pipeline(),
        namespace="post_code_identification_manual_direct_large",
        inputs={
            "input_data": "EntitiesRegisteredwithACRA",
            "singapore_postcodes_masterlist": "singapore_postcodes_masterlist",
            "singapore_postcodes_geocoded": "singapore_postcodes_geocoded",
        },
        parameters={
            "manual_postcode_config": "manual_postcode_config_direct",
            "postcode_validation": "postcode_validation",
            "postcode_master_merge_config": "postcode_master_merge_config",
        },
    )  # type: ignore
