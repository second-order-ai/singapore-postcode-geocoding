# create synthetic data based on `EntitiesRegisteredwithACRA`.
# The file is large, so create a new address type file, with postcodes embedded in weird and wonderful ways.
# We also added a ground truth type label, on whether the postcode is valid substring or not for ones that we mess up purposely.
#
# Valid examples:
# "12345"
# "012345"
# "012345.0"
# "012345.00"
# "123456"
# "0123456"
# "0123456.0"
# "0123456.00"
# "S0123456"
# "S0123456.0"
# "S0123456.00"
# "S123456"
# "S123456.0"
# "S123456.00"
# "So this 12345 is a valid substring"
# "So this 012345.00 is a valid substring"
# "So this 012345 is a valid substring"
# "So this 0000012345.00000 is a valid substring"
# "So this 0012345.00 is a valid substring"
# "So this 0012345 is a valid substring"
# "So this 123456 is a valid substring"
# "So this 123456.00 is a valid substring"
# "So this 00123456.00 is a valid substring"
# "So this 00123456 is a valid substring"
# "So this S00123456.00 is a valid substring"
# "So this S00123456 is a valid substring"
# "So this S00123456.00S is a valid substring"
# "So this S00123456S is a valid substring"
# "So this S12345S is a valid substring"
# "So this S012345.00S is a valid substring"
#
# Invalid examples:
# "1234"
# "01234"
# "01234.0"
# "01234.00"
# "1234567"
# "01234567"
# "01234567.0"
# "01234567.00"
# "S01234567"
# "S01234567.0"
# "S01234567.00"
# "S1234567"
# "S1234567.0"
# "S1234567.00"
# "01234.1"
# "01234.123"
# "01234567.1233"
# "01234567.113"
# "S01234567"
# "S01234567.123"
# "S01234567.144"
# "S1234567"
# "S1234567.145"
# "S1234567.1523"
# "So this 1234567 is an invalid substring"
# "So this 1234567.00 is an invalid substring"
# "So this 01234567 is an invalid substring"
# "So this 0000001234567.00000 is an invalid substring"
# "So this 0001234567.00 is an invalid substring"
# "So this 0001234567 is an invalid substring"
# "So this 1234 is an invalid substring"
# "So this 1234.00 is an invalid substring"
# "So this 001234.00 is an invalid substring"
# "So this 001234 is an invalid substring"
# "So this S001234.00 is an invalid substring"
# "So this S001234 is an invalid substring"
# "So this S001234.00S is an invalid substring"
# "So this S001234S is an invalid substring"
# "So this S1234 is an invalid substring"
# "So this S01234.00S is an invalid substring"
# "So this 123 456 is an invalid substring"
# "So this 123-456 is an invalid substring"
# "So this 123456.12 is an invalid substring"
# "So this 1234.56 is an invalid substring"
# "So this 123A456 is an invalid substring"

import pandas as pd
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

"""
functions:

1. shorten_postcode, flag invalid
2. lengthen_postcode, flag invalid
3. zero_decimal_value_postcodes
4. zero_padded_postcodes
5. add_trailing_characters
6. add_preceding_characters
7. add_non_zero_decimal_values, flag invalid
8. cut_postcode, flag invalid
9. add_string_to_start
10. add_string_to_end


Function chains to create valid postcodes. Any step can be omited, but the order has to be respected.

[3, 4, 5, 6, 9, 10]

Fuction chains to create invalid postcodes. Any step can be omited, but the order has to be respected.

[1, 3, 4, 7, 5, 6, 9, 10]
[2, 3, 4, 7, 5, 6, 9, 10]
[1, 3, 4, 5, 6, 8, 9, 10]
[2, 3, 4, 5, 6, 8, 9, 10]
"""


def create_short_valid_int_postcodes(postcodes: pd.Series) -> pd.Series:
    """
    Create short postcodes that are only five numbers long
    """


def create_long_valid_int_postcodes(postcodes: pd.Series) -> pd.Series:
    """
    Create short postcodes that are six numbers long
    """


def shorten_postcode(postcodes: pd.Series) -> pd.Series:
    """
    Reduce postcode to four or less numbers, this makes it an invalid format
    """


def lengthen_postcode(postcodes: pd.Series) -> pd.Series:
    """
    Extend postcode to seven or more numbers, this makes it an invalid format
    """


def zero_decimal_value_postcodes(postcodes: pd.Series) -> pd.Series:
    """
    Add an arbitrary number of trailing decimal zeros
    """


def zero_padded_postcodes(postcodes: pd.Series) -> pd.Series:
    """
    Add an arbitrary number of zeros to the start of a postcode
    """


def add_trailing_characters(postcodes: pd.Series) -> pd.Series:
    """
    Add arbitrary characters to the end of the postcode
    """


def add_preceding_characters(postcodes: pd.Series) -> pd.Series:
    """
    Add arbitrary characters to the start of the postcode
    """


def add_non_zero_decimal_values(postcodes: pd.Series) -> pd.Series:
    """
    Add non-zero decimal values to postcode, this makes it an invalid format
    """


def cut_postcode(postcodes: pd.Series) -> pd.Series:
    """
    Split postcode at any given position with an arbitrary character.
    """


def add_string_to_start(postcodes: pd.Series, strings: pd.Series) -> pd.Series:
    """
    Add string column to start of postcode, arbitrary separated with space or comma
    """


def add_string_to_end(postcodes: pd.Series, strings: pd.Series) -> pd.Series:
    """
    Add string column to end of postcode, arbitrary separated with space or comma
    """


def create_valid_postcode_substrings(df, postcode_column, address_column):
    df[postcode_column] = df[postcode_column].astype(str)
    df[address_column] = df[address_column].astype(str)
    return df


def create_invalid_postcode_substrings(df, postcode_column, address_column):
    df[postcode_column] = df[postcode_column].astype(str)
    df[address_column] = df[address_column].astype(str)
    df["valid_substring"] = df.apply(
        lambda x: x[postcode_column] in x[address_column], axis=1
    )
    return df


def convert_to_string(postcodes):
    return pd.to_numeric(postcodes, errors="coerce").astype("Int64").astype("string")


valid_chain_functions = {
    1: shorten_postcode,
    2: lengthen_postcode,
    3: zero_decimal_value_postcodes,
    4: zero_padded_postcodes,
    5: add_trailing_characters,
    6: add_preceding_characters,
    7: add_non_zero_decimal_values,
    8: cut_postcode,
    9: add_string_to_start,
    10: add_string_to_end,
}

neutral_postcode_chains = [
    [3, 4, 5, 6, 9, 10]
]  # depends on whether the input postcode is valid, or invalid
invalididate_postcode_chains = [
    [
        3,
        4,
        7,  # add non-zero decimal value makes it invalid, has to be present
        5,
        6,
        9,
        10,
    ],
    [
        3,
        4,
        5,
        6,
        8,  # cut postcode makes it invalid, has to be present
        9,
        10,
    ],
]

# in the above, starting with a valid postcode, and applying neutral chains results in a valid postcode
# starting with an an invalid postcode, and applying neutral chains results in an invalid postcode
# starting with a valid postcode, and applying invalid chain results in an invalid postcode
# starting with an invalid postcode, and applying invalid chain results in an invalid postcode


def synthesise_postcode_data(df, synth_config):
    df = df.assign(**{"__SYNTH_ID": range(len(df))})
    postcode_column = synth_config["postcode_column"]
    address_columns = synth_config["address_columns"]
    valid_invalid_split = synth_config["valid_invalid_split"]
    truth_column = synth_config["truth_column"]
    postcodes = convert_to_string(df[postcode_column])
    n_valid_postcodes_target = len(df) * valid_invalid_split
    n_invalid_postcodes_target = len(df) - n_valid_postcodes_target
    valid_postcodes = (postcodes.str.len() == 5) | (postcodes.str.len() == 6)
    invalid_postcodes = ~valid_postcodes
    n_valid_to_invalid_postcodes = int(
        n_invalid_postcodes_target - invalid_postcodes.sum()
    )
    if n_valid_to_invalid_postcodes < 0:
        logger.error(
            "There are too many invalid postcodes already. Stopping the process."
        )
        raise ValueError(
            "There are too many invalid postcodes already. Stopping the process."
        )
    valid_postcodes = df.loc[valid_postcodes]
    invalid_postcodes = df.loc[invalid_postcodes]
    valid_to_invalid_postcodes = valid_postcodes.sample(
        n=n_valid_to_invalid_postcodes, random_state=42
    )
    final_valid_postcodes = valid_postcodes.loc[
        ~valid_postcodes["__SYNTH_ID"].isin(
            valid_to_invalid_postcodes["__SYNTH_ID"].values
        )
    ]
    # randomly apply the following to the val
    df = create_valid_postcode_substrings(df, postcode_column, address_columns)
    df = create_invalid_postcode_substrings(df, postcode_column, address_columns)
    return df
