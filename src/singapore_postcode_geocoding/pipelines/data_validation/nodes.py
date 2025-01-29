import numpy as np
import pandas as pd

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


def parse_numeric_postcodes(
    df: pd.DataFrame, input_col: str, validation_field_names: dict
) -> pd.DataFrame:
    """
    Steps 1 & 2:
     - Convert postcode column to numeric, coercing invalid entries to NaN.
     - Mark those that fail numeric conversion with 'NOT_NUMERIC'.
     - Mark rows with no input as 'NO_INPUT_PROVIDED'.
    """
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
    return df


def check_postcode_range(
    df: pd.DataFrame,
    postal_int_range: list[int],
    validation_field_names: dict,
) -> pd.DataFrame:
    """
    Step 3:
     - Check if numeric postcodes are in correct range, usually [18906, 918146].
     - Mark out-of-range postcodes as incorrect with 'OUT_OF_RANGE'.

    Args:
        df: DataFrame with numeric postcodes
        postal_int_range: List with [min, max] valid postcode values
        numeric_postcodes_field: Name of field containing numeric postcodes
        validation_field_names: Dictionary with field name mappings
    """
    incorrect_reason_field = validation_field_names["incorrect_reason"]
    correct_input_flag_field = validation_field_names["correct_input_flag"]
    formatted_postcode = validation_field_names["formatted_postcode"]

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
        }
    )
    return df


def format_valid_postcodes(
    df: pd.DataFrame,
    validation_field_names: dict,
) -> pd.DataFrame:
    """
    Steps 4 & 5:
     - Format valid postcodes (numeric + in range) to 6 digits.
     - Invalid remain as NaN in FORMATTED_POSTCODE.
    """
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
    return df


def check_in_master_dataset(
    df: pd.DataFrame,
    master_postcodes: pd.Series | pd.DataFrame,
    validation_field_names: dict,
) -> pd.DataFrame:
    """
    Steps 6, 7 & 8:
     - Check if FORMATTED_POSTCODE is in the master dataset.
     - Mark as incorrect if not found in the master.
    """
    if isinstance(master_postcodes, pd.DataFrame) and master_postcodes.shape[1] != 1:
        raise ValueError(
            "Master postcode dataset should have only one column, the postocde"
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
    return df


def validate_and_format_postcodes(
    df: pd.DataFrame,
    input_col: str,
    postcode_validation_config: dict | None = None,
    master_postcodes: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Orchestrates all steps to validate and format postcodes:
      1) Parse numeric
      2) Check range
      3) Format valid postcodes
      4) Check against master dataset
      5) Returns final DataFrame with:
         - 'FORMATTED_POSTCODE'
         - 'CORRECT_INPUT_POSTCODE'
         - 'INCORRECT_INPUT_POSTCODE_REASON'
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

    df = parse_numeric_postcodes(df, input_col, validation_field_names)
    df = check_postcode_range(df, postal_int_range, validation_field_names)
    df = format_valid_postcodes(df, validation_field_names)

    if master_postcodes is not None:  # we can use this for the master as well...
        df = check_in_master_dataset(df, master_postcodes, validation_field_names)

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
