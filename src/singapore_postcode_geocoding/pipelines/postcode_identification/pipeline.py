from kedro.pipeline import Pipeline, node, pipeline

from .nodes.auto_identification import (
    convert_best_postcode_column,
    find_best_postcode_column,
)


def postcode_auto_conversion_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                find_best_postcode_column,
                inputs={
                    "df": "input_data",
                    "validation_config": "params:postcode_validation",
                    "auto_identify_config": "params:auto_identify_config",
                    "master_postcodes": "singapore_postcodes_masterlist",
                },
                outputs="postcode_match_test_results",
                name="find_best_postcode_column",
            ),
            node(
                convert_best_postcode_column,
                inputs={
                    "df": "input_data",
                    "validation_config": "params:postcode_validation",
                    "auto_identify_config": "params:auto_identify_config",
                    "master_postcodes": "singapore_postcodes_masterlist",
                    "convert_test_results": "postcode_match_test_results",
                },
                outputs=["converted_data", "conversion_successful"],
                name="convert_best_postcode_column",
            ),
        ]
    )


def create_pipeline() -> Pipeline:
    return pipeline(
        pipe=postcode_auto_conversion_pipeline(),
        namespace="post_code_identification",
        inputs={
            "input_data": "ListofGovernmentMarketsHawkerCentres",
            "singapore_postcodes_masterlist": "singapore_postcodes_masterlist",
        },
        parameters={
            "auto_identify_config": "auto_identify_config",
            "postcode_validation": "postcode_validation",
        },
    )  # type: ignore
