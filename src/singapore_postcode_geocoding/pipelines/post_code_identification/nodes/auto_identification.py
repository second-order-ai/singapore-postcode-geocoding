# i want a postcode extraction strategy:

# there are three relevant functions/pipelines:

# 1. score_postcode_fields, which score how close column names are to typical dedicated post code columns
# 2. extract_postcodes_from_series, which extract potential postcode-like substrings from string values
# 3. validate_and_format_postcodes, which checks if a column confirms to postcodes, and formats them accordingly.

# The strategy is to take a sample of a dataframe, then check if there are any dedicated postcode columns based on score_postcode_fields, it will then apply validate_and_format_postcodes directly to see what % of rows could be successfully converted.

# If this identifies a column with close 100% success the process stops, and that column is treated as the postcode column and all values extracted.

# If not, the process will then apply extract_postcodes_from_series to all columns, and then apply validate_and_format_postcodes to each column to see which column has the highest success rate.

# The success rate can then be compared between all columns and direct and indirect methods to see which is the most successful.

# If no column has any successes, the process stops, and no column is returned.

# If there are multiple columns with a similar success rate, the process will return all columns with a success rate above a certain threshold, in order of success rate and preference for direct methods.

# If there is a clear best column, the process will return that column.

# I need the functions that can do the above.


import pandas as pd

from singapore_postcode_geocoding.pipelines.data_validation.nodes import (
    validate_and_format_postcodes,
)
from singapore_postcode_geocoding.pipelines.post_code_identification.nodes.regex_postcode_identification import (
    extract_postcodes_from_series,
)

SEED = 42


def calculate_success_rate(df: pd.DataFrame, validation_field: str) -> float:
    """Calculate percentage of successful postcode validations."""
    return df[validation_field].mean() if not df.empty else 0.0


def calculate_direct_match_success(
    df: pd.DataFrame,
    column: str,
    postcode_validation_config: dict | None = None,
    master_postcodes: pd.DataFrame | None = None,
) -> float:
    """Calculate success rate for direct postcode matching on a column."""
    validated = validate_and_format_postcodes(
        df=df,
        input_col=column,
        postcode_validation_config=postcode_validation_config,
        master_postcodes=master_postcodes,
    )
    return calculate_success_rate(validated, "CORRECT_INPUT_POSTCODE")


def calculate_indirect_match_success(
    df: pd.DataFrame,
    column: str,
    postcode_validation_config: dict | None = None,
    master_postcodes: pd.DataFrame | None = None,
) -> float:
    """Calculate success rate for indirect postcode matching on a column."""
    extracted = extract_postcodes_from_series(df[column])
    validated = validate_and_format_postcodes(
        df=extracted,
        input_col="EXTRACTED_POSTCODE",
        postcode_validation_config=postcode_validation_config,
        master_postcodes=master_postcodes,
    )
    return calculate_success_rate(validated, "CORRECT_INPUT_POSTCODE")


def calculate_direct_match_success_multiple_fields(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    postcode_validation_config: dict | None = None,
    master_postcodes: pd.DataFrame | None = None,
) -> dict[str, float]:
    """Calculate success rates for direct postcode matching on multiple columns."""
    if columns is None:
        columns = list(df.columns)
    return {
        col: calculate_direct_match_success(
            df, col, postcode_validation_config, master_postcodes
        )
        for col in columns
    }


def calculate_indirect_match_success_multiple_fields(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    postcode_validation_config: dict | None = None,
    master_postcodes: pd.DataFrame | None = None,
) -> dict[str, float]:
    """Calculate success rates for indirect postcode matching on multiple columns."""
    if columns is None:
        columns = list(df.columns)
    return {
        col: calculate_indirect_match_success(
            df, col, postcode_validation_config, master_postcodes
        )
        for col in columns
    }


def get_best_columns(
    success_rates: dict[str, float], threshold: float, similarity_threshold: float
) -> list[tuple[str, float]]:
    """Identify best columns based on success rates and thresholds."""
    if not success_rates:
        return []

    # Sort by success rate
    sorted_rates = sorted(success_rates.items(), key=lambda x: x[1], reverse=True)

    # Get highest rate
    best_rate = sorted_rates[0][1]

    # If best rate below threshold, return empty
    if best_rate < threshold:
        return []

    # Get all columns within similarity threshold of best
    return [
        (col, rate)
        for col, rate in sorted_rates
        if (best_rate - rate) <= similarity_threshold
    ]


def calculate_all_match_success(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    postcode_validation_config: dict | None = None,
    master_postcodes: pd.DataFrame | None = None,
    sample_size: int = 100,
) -> pd.DataFrame:
    """Calculate both direct and indirect match success rates for columns.

    Args:
        df: Input DataFrame
        columns: List of columns to check, defaults to all columns
        postcode_validation_config: Validation configuration
        master_postcodes: Reference postcodes dataset

    Returns:
        DataFrame with columns:
        - field: Column name
        - direct_success_rate: Success rate for direct matching
        - indirect_success_rate: Success rate for indirect matching
    """
    if columns is None:
        columns = list(df.columns)
    df_sample = df.sample(n=min(sample_size, len(df)), random_state=SEED)
    # Get success rates
    direct_rates = calculate_direct_match_success_multiple_fields(
        df_sample, columns, postcode_validation_config, master_postcodes
    )
    indirect_rates = calculate_indirect_match_success_multiple_fields(
        df_sample, columns, postcode_validation_config, master_postcodes
    )

    # Combine into DataFrame
    return pd.concat(
        [
            pd.DataFrame(
                {
                    "FIELD": columns,
                    "SUCCESS_RATE": [direct_rates[col] for col in columns],
                }
            ).assign(TYPE="DIRECT"),
            pd.DataFrame(
                {
                    "FIELD": columns,
                    "SUCCESS_RATE": [indirect_rates[col] for col in columns],
                }
            ).assign(TYPE="INDIRECT"),
        ]
    ).sort_values(["SUCCESS_RATE", "TYPE"], ascending=[False, True])


def find_best_postcode_column(match_success, success_threshold=0.1):
    best_match_success = dict(match_success.iloc[0])
    if best_match_success["SUCCESS_RATE"] < success_threshold:
        return best_match_success, False
    else:
        return best_match_success, True


def auto_convert_postcode_column(
    df: pd.DataFrame,
    best_match_success: dict,
    postcode_validation_config: dict | None = None,
    master_postcodes: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, dict]:
    if best_match_success["TYPE"] == "DIRECT":
        df = validate_and_format_postcodes(
            df,
            best_match_success["FIELD"],
            postcode_validation_config,
            master_postcodes,
        )
    else:
        df = extract_postcodes_from_series(df[best_match_success["FIELD"]])
        df = validate_and_format_postcodes(
            df, "EXTRACTED_POSTCODE", postcode_validation_config, master_postcodes
        )
    return df, best_match_success
