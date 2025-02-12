from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    convert_best_postcode_column,
    find_best_postcode_column,
    merge_postcode_data,
)


def create_pipeline() -> Pipeline:
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
            node(
                merge_postcode_data,  # just for testing purposes for now...
                inputs={
                    "converted_df": "converted_data",
                    "master_postcode_df": "singapore_postcodes_geocoded",
                    "merge_config": "params:postcode_master_merge_config",
                    "post_code_conversion_passed": "conversion_successful",
                },
                outputs="merged_postcode_dataset",
                name="merge_postcode_data",
            ),
        ]
    )
