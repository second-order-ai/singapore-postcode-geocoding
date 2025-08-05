"""Pipeline for automatic and manual postcode identification and conversion.

This module provides two pipelines:
1. Automatic pipeline that finds the best column and method for postcode conversion
2. Manual pipeline that uses specified column and method via configuration
"""

from kedro.pipeline import Pipeline, Node
import pandas as pd

from .nodes import (
    convert_best_postcode_column,
    find_best_postcode_column,
    merge_postcode_data,
)


def create_manual_test_results_adapter(
    df: pd.DataFrame, manual_config: dict
) -> pd.DataFrame:
    """Adapter to convert manual configuration into the expected test results format.

    This allows us to reuse the existing pipeline structure without modification.

    Args:
        df: Input DataFrame (not used, but required for interface consistency)
        manual_config: Dictionary containing manual configuration:
            {
                "COLUMN": str,  # Column name containing postal codes
                "METHOD": str,  # "DIRECT" or "INDIRECT"
                "CONVERSION_SUCCESS_RATE": float,  # Optional, defaults to 1.0
                "REGEX_PATTERN": str  # Required only for INDIRECT method
            }

    Returns:
        pd.DataFrame: Test results in the format expected by the conversion pipeline
    """
    return pd.DataFrame(
        [
            {
                "CONVERSION_SUCCESS_RATE": manual_config.get(
                    "CONVERSION_SUCCESS_RATE", 1.0
                ),
                "COLUMN": manual_config["COLUMN"],
                "METHOD": manual_config["METHOD"],
                "REGEX_PATTERN": manual_config.get("REGEX_PATTERN")
                if manual_config["METHOD"] == "INDIRECT"
                else None,
            }
        ]
    )


def create_pipeline() -> Pipeline:
    """Create the automatic postcode identification pipeline.

    This pipeline automatically finds the best column and method for postcode conversion
    by testing all columns with both direct and indirect methods.
    """
    return Pipeline(
        [
            Node(
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
            Node(
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
            Node(
                merge_postcode_data,
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


def create_manual_pipeline() -> Pipeline:
    """Create the manual postcode identification pipeline.

    This pipeline uses manually specified column and method for postcode conversion,
    adapting the configuration to work with the existing pipeline structure.
    """
    return Pipeline(
        [
            Node(
                create_manual_test_results_adapter,
                inputs={
                    "df": "input_data",
                    "manual_config": "params:manual_postcode_config",
                },
                outputs="postcode_match_test_results",
                name="create_manual_test_results",
            ),
            Node(
                convert_best_postcode_column,
                inputs={
                    "df": "input_data",
                    "validation_config": "params:postcode_validation",
                    "master_postcodes": "singapore_postcodes_masterlist",
                    "convert_test_results": "postcode_match_test_results",
                },
                outputs=["converted_data", "conversion_successful"],
                name="convert_best_postcode_column",
            ),
            Node(
                merge_postcode_data,
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
