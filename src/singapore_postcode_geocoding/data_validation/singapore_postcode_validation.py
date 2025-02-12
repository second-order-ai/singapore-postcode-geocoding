import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


VALIDATION_DEFAULTS = {
    "range": {"int": [18906, 918146], "len": [5, 6]},
    "drop_incorrect": False,
    "keep_formatted_postcode_field": True,
    "keep_validation_fields": True,
    "validation_field_names": {
        "correct_input_flag": "CORRECT_INPUT_POSTCODE",
        "incorrect_reason": "INCORRECT_INPUT_POSTCODE_REASON",
        "formatted_postcode": "FORMATTED_POSTCODE",
    },
}


def log_validation_step(
    df: pd.DataFrame,
    validation_field_names: dict,
    step_name: str,
) -> None:
    """Log validation statistics for current step.

    Args:
        df: DataFrame with validation results
        validation_field_names: Field name mappings
        step_name: Name of validation step
    """
    total = len(df)
    flag_field = validation_field_names["correct_input_flag"]
    reason_field = validation_field_names["incorrect_reason"]

    # Get validation counts
    valid_count = df[flag_field].sum()
    invalid_count = total - valid_count

    # Log summary
    logger.info(
        "%s - Valid: %d (%.1f%%), Invalid: %d (%.1f%%)",
        step_name,
        valid_count,
        100 * valid_count / total,
        invalid_count,
        100 * invalid_count / total,
    )

    # Log failure reasons if any invalid records
    if invalid_count > 0:
        reasons = df[~df[flag_field]][reason_field].value_counts()
        for reason, count in reasons.items():
            if pd.notna(reason):
                logger.info("\t- %s: %d (%.1f%%)", reason, count, 100 * count / total)


def parse_numeric_postcodes(
    df: pd.DataFrame, input_col: str, validation_field_names: dict
) -> pd.DataFrame:
    """Parse and validate numeric postcodes from input column.

    Performs initial validation of postcode values:
    1. Converts postcode column to numeric values
    2. Marks non-numeric values with 'NOT_NUMERIC'
    3. Marks empty values with 'NO_INPUT_PROVIDED'
    4. Sets validation flags for each row

    Args:
        df: DataFrame containing postcode column
        input_col: Name of column containing postcodes
        validation_field_names: Dictionary with field mappings containing:
            - correct_input_flag: "CORRECT_INPUT_POSTCODE"
            - incorrect_reason: "INCORRECT_INPUT_POSTCODE_REASON"
            - formatted_postcode: "FORMATTED_POSTCODE"

    Returns:
        DataFrame with added validation columns

    Example:
        >>> # Setup example data and configuration
        >>> validation_names = VALIDATION_DEFAULTS["validation_field_names"]
        >>> postal_range = VALIDATION_DEFAULTS["range"]["int"]
        >>> df = pd.DataFrame(
        ...     {
        ...         "postcode": [
        ...             "123456",  # Valid numeric, in range, but not in master list
        ...             "A123456",  # Invalid - starts with letter
        ...             "",  # Empty string
        ...             "123ABC",  # Invalid - contains letters
        ...             np.nan,  # Missing value
        ...             "123 456",  # Invalid - contains space
        ...             "123456.81",  # Valid numeric, non-integer
        ...             "018906",  # Valid numeric, in range, needs formatting
        ...             "999999",  # Valid numeric, out of range
        ...             "12345",  # Valid numeric, in range, needs padding
        ...         ]
        ...     }
        ... )
        >>> df1 = parse_numeric_postcodes(df, "postcode", validation_names)
        >>> df1
            postcode  FORMATTED_POSTCODE  CORRECT_INPUT_POSTCODE  INCORRECT_INPUT_POSTCODE_REASON
        0     123456           123456.0                    True                               NaN
        1    A123456               NaN                   False                       NOT_NUMERIC
        2                        NaN                   False               NO_INPUT_PROVIDED
        3     123ABC               NaN                   False                       NOT_NUMERIC
        4        NaN               NaN                   False               NO_INPUT_PROVIDED
        5    123 456               NaN                   False                       NOT_NUMERIC
        6  123456.81         123456.81                    True                               NaN
        7     018906           18906.0                    True                               NaN
        8     999999          999999.0                    True                               NaN
        9      12345           12345.0                    True                               NaN
    """
    logger.info("Checking for numeric postcodes...")
    incorrect_reason_field = validation_field_names["incorrect_reason"]
    correct_input_flag_field = validation_field_names["correct_input_flag"]
    formatted_postcode = validation_field_names["formatted_postcode"]

    numeric_series = pd.to_numeric(df[input_col], errors="coerce")
    df = df.assign(
        **{
            formatted_postcode: numeric_series,
            incorrect_reason_field: np.nan,
            correct_input_flag_field: True,  # this feels too implicit, but we can keep it for now, would prefer to make it explicit, check for conformity and then set to True
        }
    )

    # Mark rows where original input wasn't NaN but numeric_postcodes is NaN => NOT_NUMERIC
    non_numeric_mask = numeric_series.isna() & df[input_col].notna()
    df = df.assign(
        **{
            incorrect_reason_field: df[incorrect_reason_field].mask(
                non_numeric_mask, "NOT_NUMERIC"
            ),
            correct_input_flag_field: df[correct_input_flag_field].mask(
                non_numeric_mask, False
            ),
        }
    )

    # Mark rows with completely empty input => NO_INPUT_PROVIDED
    null_input_mask = df[input_col].isna()
    df = df.assign(
        **{
            incorrect_reason_field: df[incorrect_reason_field].mask(
                null_input_mask, "NO_INPUT_PROVIDED"
            ),
            correct_input_flag_field: df[correct_input_flag_field].mask(
                null_input_mask, False
            ),
        }
    )
    log_validation_step(
        df, validation_field_names, step_name="`parse_numeric_postcodes`"
    )
    return df


def check_is_integer(
    df: pd.DataFrame,
    validation_field_names: dict,
) -> pd.DataFrame:
    """Check if numeric values are integers without decimal parts.

    Validates that postcode values are whole numbers:
    1. Checks for decimal parts using modulo
    2. Marks non-integer values as invalid
    3. Updates validation flags and reasons

    Args:
        df: DataFrame with numeric postcodes
        validation_field_names: Dictionary with field mappings matching VALIDATION_DEFAULTS
            - correct_input_flag: "CORRECT_INPUT_POSTCODE"
            - incorrect_reason: "INCORRECT_INPUT_POSTCODE_REASON"
            - formatted_postcode: "FORMATTED_POSTCODE"

    Returns:
        DataFrame with updated validation results

    Example:
        >>> validation_names = VALIDATION_DEFAULTS["validation_field_names"]
        >>> df = pd.DataFrame(
        ...     {
        ...         "postcode": [
        ...             "123456",  # Valid numeric, in range, but not in master list
        ...             "A123456",  # Invalid - starts with letter
        ...             "",  # Empty string
        ...             "123ABC",  # Invalid - contains letters
        ...             np.nan,  # Missing value
        ...             "123 456",  # Invalid - contains space
        ...             "123456.81",  # Valid numeric, non-integer
        ...             "018906",  # Valid numeric, in range, needs formatting
        ...             "999999",  # Valid numeric, out of range
        ...             "12345",  # Valid numeric, in range, needs padding
        ...         ]
        ...     }
        ... )
        >>> df1 = parse_numeric_postcodes(df, "postcode", validation_names)
        >>> df2 = check_is_integer(df1, validation_names)
        >>> df2
            postcode  FORMATTED_POSTCODE  CORRECT_INPUT_POSTCODE  INCORRECT_INPUT_POSTCODE_REASON
        0     123456           123456.0                    True                               NaN
        1    A123456               NaN                   False                       NOT_NUMERIC
        2                        NaN                   False               NO_INPUT_PROVIDED
        3     123ABC               NaN                   False                       NOT_NUMERIC
        4        NaN               NaN                   False               NO_INPUT_PROVIDED
        5    123 456               NaN                   False                       NOT_NUMERIC
        6  123456.81               NaN                   False                       NOT_INTEGER
        7     018906           18906.0                    True                               NaN
        8     999999          999999.0                    True                               NaN
        9      12345           12345.0                    True                               NaN
    """
    logger.info("Checking that postcodes are integers...")
    incorrect_reason_field = validation_field_names["incorrect_reason"]
    correct_input_flag_field = validation_field_names["correct_input_flag"]
    formatted_postcode = validation_field_names["formatted_postcode"]

    # Check for non-integer values (has decimal part)
    non_integer_mask = df[formatted_postcode].notna() & (
        df[formatted_postcode] % 1 != 0
    )

    # Update validation fields for non-integer values
    df = df.assign(
        **{
            incorrect_reason_field: df[incorrect_reason_field].mask(
                non_integer_mask, "NOT_INTEGER"
            ),
            correct_input_flag_field: df[correct_input_flag_field].mask(
                non_integer_mask, False
            ),
            formatted_postcode: df[formatted_postcode].mask(non_integer_mask, np.nan),
        }
    )
    log_validation_step(df, validation_field_names, step_name="`check_is_integer`")
    return df


def check_postcode_range(
    df: pd.DataFrame,
    postal_int_range: list[int],
    validation_field_names: dict,
) -> pd.DataFrame:
    """Validate postcodes are within Singapore's valid range.

    Checks if numeric postcodes fall within valid range and marks invalid ones:
    1. Validates against Singapore's range [18906, 918146]
    2. Marks out-of-range values as invalid with 'OUT_OF_RANGE'
    3. Updates validation flags and formatted values

    Args:
        df: DataFrame with numeric postcodes
        postal_int_range: List with [min, max] valid values, default [18906, 918146]
        validation_field_names: Dictionary with field mappings matching VALIDATION_DEFAULTS

    Returns:
        DataFrame with updated validation results

    Example:
        >>> validation_names = VALIDATION_DEFAULTS["validation_field_names"]
        >>> postal_range = VALIDATION_DEFAULTS["range"]["int"]
        >>> df = pd.DataFrame(
        ...     {
        ...         "postcode": [
        ...             "123456",  # Valid numeric, in range, but not in master list
        ...             "A123456",  # Invalid - starts with letter
        ...             "",  # Empty string
        ...             "123ABC",  # Invalid - contains letters
        ...             np.nan,  # Missing value
        ...             "123 456",  # Invalid - contains space
        ...             "123456.81",  # Valid numeric, non-integer
        ...             "018906",  # Valid numeric, in range, needs formatting
        ...             "999999",  # Valid numeric, out of range
        ...             "12345",  # Valid numeric, in range, needs padding
        ...         ]
        ...     }
        ... )
        >>> df1 = parse_numeric_postcodes(df, "postcode", validation_names)
        >>> df2 = check_is_integer(df1, validation_names)
        >>> df3 = check_postcode_range(df2, postal_range, validation_names)
        >>> df3
            postcode  FORMATTED_POSTCODE  CORRECT_INPUT_POSTCODE  INCORRECT_INPUT_POSTCODE_REASON
        0     123456           123456.0                    True                               NaN
        1    A123456               NaN                   False                       NOT_NUMERIC
        2                        NaN                   False               NO_INPUT_PROVIDED
        3     123ABC               NaN                   False                       NOT_NUMERIC
        4        NaN               NaN                   False               NO_INPUT_PROVIDED
        5    123 456               NaN                   False                       NOT_NUMERIC
        6  123456.81               NaN                   False                       NOT_INTEGER
        7     018906           18906.0                    True                               NaN
        8     999999               NaN                   False                      OUT_OF_RANGE
        9      12345           12345.0                    True                               NaN
    """
    logger.info("Checking that postcodes are within valid range...")
    incorrect_reason_field = validation_field_names["incorrect_reason"]
    correct_input_flag_field = validation_field_names["correct_input_flag"]
    formatted_postcode = validation_field_names["formatted_postcode"]
    logger.info("Valid range: %s", postal_int_range)
    in_range = df[formatted_postcode].between(
        postal_int_range[0], postal_int_range[1], inclusive="both"
    )

    out_of_range_mask = ~in_range & df[formatted_postcode].notna()
    df = df.assign(
        **{
            incorrect_reason_field: df[incorrect_reason_field].mask(
                out_of_range_mask, "OUT_OF_RANGE"
            ),
            correct_input_flag_field: df[correct_input_flag_field].mask(
                out_of_range_mask, False
            ),
            formatted_postcode: df[formatted_postcode].mask(out_of_range_mask, np.nan),
        }
    )
    log_validation_step(df, validation_field_names, step_name="`check_postcode_range`")
    return df


def format_valid_postcodes(
    df: pd.DataFrame,
    validation_field_names: dict,
) -> pd.DataFrame:
    """Format valid postcodes to standardized 6-digit strings.

    Standardizes format of valid postcodes and handles invalid ones:
    1. Converts valid postcodes to 6-digit strings
    2. Adds leading zeros where needed
    3. Preserves NaN for invalid postcodes
    4. Only formats postcodes that passed previous validations

    Args:
        df: DataFrame with validated postcodes
        validation_field_names: Dictionary with field mappings matching VALIDATION_DEFAULTS

    Returns:
        DataFrame with formatted postcode values

    Example:
        >>> validation_names = VALIDATION_DEFAULTS["validation_field_names"]
        >>> df = pd.DataFrame(
        ...     {
        ...         "postcode": [
        ...             "123456",  # Valid numeric, in range, but not in master list
        ...             "A123456",  # Invalid - starts with letter
        ...             "",  # Empty string
        ...             "123ABC",  # Invalid - contains letters
        ...             np.nan,  # Missing value
        ...             "123 456",  # Invalid - contains space
        ...             "123456.81",  # Valid numeric, non-integer
        ...             "018906",  # Valid numeric, in range, needs formatting
        ...             "999999",  # Valid numeric, out of range
        ...             "12345",  # Valid numeric, in range, needs padding
        ...         ]
        ...     }
        ... )
        >>> df1 = parse_numeric_postcodes(df, "postcode", validation_names)
        >>> df2 = check_is_integer(df1, validation_names)
        >>> df3 = check_postcode_range(df2, postal_range, validation_names)
        >>> df4 = format_valid_postcodes(df3, validation_names)
        >>> df4
            postcode FORMATTED_POSTCODE  CORRECT_INPUT_POSTCODE  INCORRECT_INPUT_POSTCODE_REASON
        0     123456           123456                    True                               NaN
        1    A123456              NaN                   False                       NOT_NUMERIC
        2                        NaN                   False               NO_INPUT_PROVIDED
        3     123ABC              NaN                   False                       NOT_NUMERIC
        4        NaN              NaN                   False               NO_INPUT_PROVIDED
        5    123 456              NaN                   False                       NOT_NUMERIC
        6  123456.81              NaN                   False                       NOT_INTEGER
        7     018906           018906                    True                               NaN
        8     999999              NaN                   False                      OUT_OF_RANGE
        9      12345           012345                    True                               NaN
    """
    logger.info("Formatting valid postcodes...")
    correct_input_flag_field = validation_field_names["correct_input_flag"]
    formatted_postcode = validation_field_names["formatted_postcode"]
    valid_mask = df[correct_input_flag_field]
    df = df.assign(
        **{
            formatted_postcode: (
                df[formatted_postcode]
                .astype("Int64")
                .astype("string")  # avoids mixed types
                .str.zfill(6)
                .where(valid_mask, np.nan)  # need to check if this is the correct
            )
        }
    )
    logger.info("Formatted postcodes are now in column: `%s`", formatted_postcode)
    return df


def check_in_master_dataset(
    df: pd.DataFrame,
    master_postcodes: pd.Series | pd.DataFrame,
    validation_field_names: dict,
) -> pd.DataFrame:
    """Validate postcodes against master reference dataset.

    Verifies postcodes exist in master dataset and marks invalid ones:
    1. Accepts master data as Series or single-column DataFrame
    2. Checks if formatted postcodes exist in master dataset
    3. Marks non-existent codes as invalid with 'NOT_IN_MASTER_DATASET'

    Args:
        df: DataFrame with formatted postcodes
        master_postcodes: Reference Series/DataFrame with valid postcodes
        validation_field_names: Dictionary with field mappings matching VALIDATION_DEFAULTS

    Returns:
        DataFrame with master dataset validation results

    Example:
        >>> master = pd.DataFrame({"postcode": ["018906", "012345"]})
        >>> validation_names = VALIDATION_DEFAULTS["validation_field_names"]
        >>> df = pd.DataFrame(
        ...     {
        ...         "postcode": [
        ...             "123456",  # Valid numeric, in range, but not in master list
        ...             "A123456",  # Invalid - starts with letter
        ...             "",  # Empty string
        ...             "123ABC",  # Invalid - contains letters
        ...             np.nan,  # Missing value
        ...             "123 456",  # Invalid - contains space
        ...             "123456.81",  # Valid numeric, non-integer
        ...             "018906",  # Valid numeric, in range, needs formatting
        ...             "999999",  # Valid numeric, out of range
        ...             "12345",  # Valid numeric, in range, needs padding
        ...         ]
        ...     }
        ... )
        >>> df1 = parse_numeric_postcodes(df, "postcode", validation_names)
        >>> df2 = check_is_integer(df1, validation_names)
        >>> df3 = check_postcode_range(df2, postal_range, validation_names)
        >>> df4 = format_valid_postcodes(df3, validation_names)
        >>> df5 = check_in_master_dataset(df4, master, validation_names)
        >>> df5
            postcode FORMATTED_POSTCODE  CORRECT_INPUT_POSTCODE  INCORRECT_INPUT_POSTCODE_REASON
        0     123456           123456                   False             NOT_IN_MASTER_DATASET
        1    A123456              NaN                   False                       NOT_NUMERIC
        2                        NaN                   False               NO_INPUT_PROVIDED
        3     123ABC              NaN                   False                       NOT_NUMERIC
        4        NaN              NaN                   False               NO_INPUT_PROVIDED
        5    123 456              NaN                   False                       NOT_NUMERIC
        6  123456.81              NaN                   False                       NOT_INTEGER
        7     018906           018906                    True                               NaN
        8     999999              NaN                   False                      OUT_OF_RANGE
        9      12345           012345                    True                               NaN

    Raises:
        ValueError: If master_postcodes DataFrame has more than one column
    """
    logger.info("Checking postcodes against master dataset postcodes...")
    if isinstance(master_postcodes, pd.DataFrame) and master_postcodes.shape[1] != 1:
        raise ValueError(
            "Master postcode dataset should have only one column, the postcode"
        )
    if isinstance(master_postcodes, pd.DataFrame):
        master_postcodes_series = master_postcodes.iloc[:, 0]
    else:
        master_postcodes_series = master_postcodes
    master_postcodes_series = master_postcodes_series.values  # single postcode column

    incorrect_reason_field = validation_field_names["incorrect_reason"]
    correct_input_flag_field = validation_field_names["correct_input_flag"]
    formatted_postcode = validation_field_names["formatted_postcode"]
    in_master = df[formatted_postcode].isin(master_postcodes_series)
    not_in_master_mask = ~in_master & df[correct_input_flag_field]
    df = df.assign(
        **{
            incorrect_reason_field: df[incorrect_reason_field].mask(
                not_in_master_mask, "NOT_IN_MASTER_DATASET"
            ),
            correct_input_flag_field: df[correct_input_flag_field].mask(
                not_in_master_mask, False
            ),
        }
    )
    log_validation_step(
        df, validation_field_names, step_name="`check_in_master_dataset`"
    )
    return df


def validate_and_format_postcodes(
    df: pd.DataFrame,
    input_col: str,
    postcode_validation_config: dict | None = None,
    master_postcodes: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Validate and format Singapore postcodes through multiple validation steps.

    Performs complete validation pipeline:
    1. Validates numeric values and handles empty/invalid inputs
    2. Checks for integer values without decimal parts
    3. Validates against Singapore's valid postcode range
    4. Formats valid postcodes to 6-digit strings
    5. Optionally validates against master postcode dataset
    6. Handles cleanup based on configuration settings

    Args:
        df: DataFrame containing postcode column
        input_col: Name of column containing postcodes
        postcode_validation_config: Configuration dictionary, if None uses VALIDATION_DEFAULTS:
            - range: Valid integer and length ranges
            - drop_incorrect: Whether to drop invalid records
            - keep_validation_fields: Whether to keep validation columns
            - keep_formatted_postcode_field: Whether to keep formatted column
            - validation_field_names: Column name mappings
        master_postcodes: Optional reference dataset of valid postcodes

    Returns:
        DataFrame with validated and formatted postcodes

    Example:
        >>> # Setup example data and configuration
        >>> validation_names = VALIDATION_DEFAULTS["validation_field_names"]
        >>> master = pd.DataFrame({"postcode": ["018906", "012345"]})
        >>> df = pd.DataFrame(
        ...     {
        ...         "postcode": [
        ...             "123456",  # Valid numeric, in range, not in master
        ...             "A123456",  # Invalid - starts with letter
        ...             "",  # Empty string
        ...             "123ABC",  # Invalid - contains letters
        ...             np.nan,  # Missing value
        ...             "123 456",  # Invalid - contains space
        ...             "123456.81",  # Valid numeric, non-integer
        ...             "018906",  # Valid numeric, in range, in master
        ...             "999999",  # Valid numeric, out of range
        ...             "12345",  # Valid numeric, in range, in master (after padding)
        ...         ]
        ...     }
        ... )
        >>> result = validate_and_format_postcodes(
        ...     df, "postcode", VALIDATION_DEFAULTS, master
        ... )
        >>> result
             postcode FORMATTED_POSTCODE  CORRECT_INPUT_POSTCODE  INCORRECT_INPUT_POSTCODE_REASON
        0     123456              NaN                   False             NOT_IN_MASTER_DATASET
        1    A123456              NaN                   False                       NOT_NUMERIC
        2                        NaN                   False               NO_INPUT_PROVIDED
        3     123ABC              NaN                   False                       NOT_NUMERIC
        4        NaN              NaN                   False               NO_INPUT_PROVIDED
        5    123 456              NaN                   False                       NOT_NUMERIC
        6  123456.81              NaN                   False                       NOT_INTEGER
        7     018906           018906                    True                               NaN
        8     999999              NaN                   False                      OUT_OF_RANGE
        9      12345           012345                    True                               NaN
    """
    if postcode_validation_config is None:
        postcode_validation_config = VALIDATION_DEFAULTS

    validation_field_names = postcode_validation_config["validation_field_names"]
    postal_int_range = postcode_validation_config["range"]["int"]
    drop_incorrect = postcode_validation_config["drop_incorrect"]
    keep_validation_fields = postcode_validation_config["keep_validation_fields"]
    keep_formatted_postcode_field = postcode_validation_config[
        "keep_formatted_postcode_field"
    ]
    logger.info(f"Checking postcodes in column: {input_col}")
    df = parse_numeric_postcodes(df, input_col, validation_field_names)
    df = check_is_integer(df, validation_field_names)
    df = check_postcode_range(df, postal_int_range, validation_field_names)
    df = format_valid_postcodes(df, validation_field_names)

    if master_postcodes is not None:  # we can use this for the master as well...
        df = check_in_master_dataset(df, master_postcodes, validation_field_names)

    logger.info("VALIDATION AND CONVERSION SUMMARY:")
    log_validation_step(
        df, validation_field_names, step_name="`validate_and_format_postcodes`"
    )

    if drop_incorrect:
        df = df[df[validation_field_names["correct_input_flag"]]]

    if keep_validation_fields is False:
        df = df.drop(
            columns=[
                validation_field_names["correct_input_flag"],
                validation_field_names["incorrect_reason"],
            ]
        )
    if keep_formatted_postcode_field is False:
        df = df.drop(columns=[validation_field_names["formatted_postcode"]])

    return df
