from kedro.pipeline import Pipeline, node, pipeline

from .nodes.auto_identification import (
    auto_convert_postcode_column,
    calculate_all_match_success,
    find_best_postcode_column,
)


def postcode_identification_pipeline() -> Pipeline:
    return Pipeline(
        [
            node(
                calculate_all_match_success,
                inputs=[
                    "input_data",
                    "params:auto_identify_config",
                    "params:postcode_validation",
                    "singapore_postcodes_masterlist",
                ],
                outputs="postcode_all_match_success",
                name="calculate_all_match_success",
            ),
            node(
                find_best_postcode_column,
                inputs=[
                    "postcode_all_match_success",
                    "params:auto_identify_config",
                ],
                outputs=["best_postcode_column_info", "best_column_passed"],
                name="find_best_postcode_column",
            ),
            node(
                auto_convert_postcode_column,
                inputs=[
                    "input_data",
                    "best_postcode_column_info",
                    "best_column_passed",
                    "params:postcode_validation",
                    "singapore_postcodes_masterlist",
                ],
                outputs="postcode_auto_convert",
                name="auto_convert_postcode_column",
            ),
        ]
    )


def create_pipeline() -> Pipeline:
    return pipeline(
        pipe=postcode_identification_pipeline(),
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
