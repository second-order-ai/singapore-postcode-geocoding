import pandas as pd
import numpy as np


def parse_numeric_postcodes(df: pd.DataFrame, input_col: str) -> pd.DataFrame:
    """
    Steps 1 & 2:
     - Convert postcode column to numeric, coercing invalid entries to NaN.
     - Mark those that fail numeric conversion with 'NOT_NUMERIC'.
     - Mark rows with no input as 'NO_INPUT_PROVIDED'.
    """
    numeric_series = pd.to_numeric(df[input_col], errors="coerce")
    df = df.assign(
        numeric_postcodes=numeric_series,
        INCORRECT_INPUT_POSTCODE_REASON=np.nan,
        CORRECT_INPUT_POSTCODE=True,
    )

    # Mark rows where original input wasn't NaN but numeric_postcodes is NaN => NOT_NUMERIC
    non_numeric_mask = df["numeric_postcodes"].isna() & df[input_col].notna()
    df = df.assign(
        INCORRECT_INPUT_POSTCODE_REASON=df["INCORRECT_INPUT_POSTCODE_REASON"].mask(
            non_numeric_mask, "NOT_NUMERIC"
        ),
        CORRECT_INPUT_POSTCODE=df["CORRECT_INPUT_POSTCODE"].mask(
            non_numeric_mask, False
        ),
    )

    # Mark rows with completely empty input => NO_INPUT_PROVIDED
    null_input_mask = df[input_col].isna()
    df = df.assign(
        INCORRECT_INPUT_POSTCODE_REASON=df["INCORRECT_INPUT_POSTCODE_REASON"].mask(
            null_input_mask, "NO_INPUT_PROVIDED"
        ),
        CORRECT_INPUT_POSTCODE=df["CORRECT_INPUT_POSTCODE"].mask(
            null_input_mask, False
        ),
    )

    return df


def check_postcode_range(df: pd.DataFrame) -> pd.DataFrame:
    """
    Step 3:
     - Check if numeric postcodes are in [18906, 918146].
     - Mark out-of-range postcodes as incorrect with 'OUT_OF_RANGE'.
    """
    in_range = df["numeric_postcodes"].between(18906, 918146, inclusive="both")
    df = df.assign(in_range=in_range)

    out_of_range_mask = ~df["in_range"] & df["numeric_postcodes"].notna()
    df = df.assign(
        INCORRECT_INPUT_POSTCODE_REASON=df["INCORRECT_INPUT_POSTCODE_REASON"].mask(
            out_of_range_mask, "OUT_OF_RANGE"
        ),
        CORRECT_INPUT_POSTCODE=df["CORRECT_INPUT_POSTCODE"].mask(
            out_of_range_mask, False
        ),
    )

    return df


def format_valid_postcodes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Steps 4 & 5:
     - Format valid postcodes (numeric + in range) to 6 digits.
     - Invalid remain as NaN in FORMATTED_POSTCODE.
    """
    valid_mask = df["numeric_postcodes"].notna() & df["in_range"]
    formatted_series = (
        df["numeric_postcodes"]
        .fillna(0)
        .astype(int)
        .astype(str)
        .str.zfill(6)
        .where(valid_mask, np.nan)
    )
    df = df.assign(FORMATTED_POSTCODE=formatted_series)

    return df


def check_in_master_dataset(
    df: pd.DataFrame, master_postcodes: pd.DataFrame
) -> pd.DataFrame:
    """
    Steps 6, 7 & 8:
     - Check if FORMATTED_POSTCODE is in the master dataset.
     - Mark as incorrect if not found in the master.
    """
    valid_set = set(master_postcodes["POSTAL"].dropna().unique())
    in_master = df["FORMATTED_POSTCODE"].isin(valid_set)
    df = df.assign(in_master=in_master)

    not_in_master_mask = ~df["in_master"] & df["FORMATTED_POSTCODE"].notna()
    df = df.assign(
        INCORRECT_INPUT_POSTCODE_REASON=df["INCORRECT_INPUT_POSTCODE_REASON"].mask(
            not_in_master_mask, "NOT_IN_MASTER_DATASET"
        ),
        CORRECT_INPUT_POSTCODE=df["CORRECT_INPUT_POSTCODE"].mask(
            not_in_master_mask, False
        ),
    )

    return df


def validate_and_format_postcodes(
    df: pd.DataFrame,
    input_col: str,
    master_postcodes: pd.DataFrame,
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
    df = parse_numeric_postcodes(df, input_col)
    df = check_postcode_range(df)
    df = format_valid_postcodes(df)
    df = check_in_master_dataset(df, master_postcodes)

    # Clean up helper columns
    df = df.drop(
        columns=["numeric_postcodes", "in_range", "in_master"], errors="ignore"
    )
    return df
