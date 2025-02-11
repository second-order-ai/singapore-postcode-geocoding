import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_series_equal

from singapore_postcode_geocoding.pipelines.postcode_identification.nodes.auto_identification import (
    ConvertPostcodes,
    IdentifyPostcodes,
    auto_convert_postcodes,
    regex_extract,
)


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "address": ["123456 Main St", "No code here", "789012 Side St"],
            "postcode": ["123456", "789012", "111111"],
            "empty": [np.nan, np.nan, np.nan],
            "mixed": ["Address 654321", "987654", "Invalid"],
        }
    )


@pytest.fixture
def validation_config():
    return {
        "range": {"int": [18906, 918146], "len": [5, 6]},
        "drop_incorrect": False,
        "keep_validation_fields": True,
        "keep_formatted_postcode_field": True,
        "validation_field_names": {
            "candidate_postcode": "CANDIDATE_POSTCODE",
            "extracted_postcode": "EXTRACTED_POSTCODE",
            "correct_input_flag": "CORRECT_INPUT_POSTCODE",
            "incorrect_reason": "INCORRECT_INPUT_POSTCODE_REASON",
            "formatted_postcode": "FORMATTED_POSTCODE",
        },
    }


@pytest.fixture
def auto_identify_config():
    return {
        "candidate_columns": None,
        "regex_pattern": r"((?<!\d)\d{5,6}(?!\d))",
        "sample_size": 100,
        "success_threshold": 0.1,
        "seed": 42,
    }


@pytest.fixture
def master_postcodes():
    """Create sample master postcodes DataFrame within valid range."""
    return pd.DataFrame(
        {
            "POSTAL_CODE": [
                "123456",  # Valid 6-digit code
                "789012",  # Valid 6-digit code
                "654321",  # Valid 6-digit code
                "987654",  # Valid 6-digit code
                "18906",  # Valid 5-digit code (min range)
                "91814",  # Valid 5-digit code (near max range)
            ]
        }
    )


# Test regex_extract function
def test_regex_extract_basic():
    series = pd.Series(["Address 123456", "No code", "Code 12345"])
    result = regex_extract(series, r"((?<!\d)\d{6}(?!\d))")
    expected = pd.Series(["123456", np.nan, np.nan])
    assert_series_equal(result, expected, check_dtype=False)


def test_regex_extract_empty():
    series = pd.Series([])
    result = regex_extract(series, r"((?<!\d)\d{6}(?!\d))")
    assert len(result) == 0


def test_regex_extract_all_invalid():
    series = pd.Series(["abc", "def", "ghi"])
    result = regex_extract(series, r"((?<!\d)\d{6}(?!\d))")
    assert result.isna().all()


# Test TestConvertPostcodes class
class TestTestConvertPostcodes:
    def test_initialization(
        self, sample_df, validation_config, master_postcodes, auto_identify_config
    ):
        tester = IdentifyPostcodes(
            sample_df, validation_config, master_postcodes, auto_identify_config
        )
        assert tester.df.equals(sample_df)
        assert tester._regex_pattern == auto_identify_config["regex_pattern"]

    def test_direct_conversion(
        self, sample_df, validation_config, master_postcodes, auto_identify_config
    ):
        tester = IdentifyPostcodes(
            sample_df, validation_config, master_postcodes, auto_identify_config
        )
        result = tester.direct_conversion(sample_df["postcode"], validation_config)
        assert "CORRECT_INPUT_POSTCODE" in result.columns
        assert result["CORRECT_INPUT_POSTCODE"].mean() > 0

    def test_indirect_extraction(
        self, sample_df, validation_config, master_postcodes, auto_identify_config
    ):
        tester = IdentifyPostcodes(
            sample_df, validation_config, master_postcodes, auto_identify_config
        )
        result = tester.indirect_extraction(sample_df["address"], validation_config)
        assert "EXTRACTED_POSTCODE" in result.columns
        assert result["CORRECT_INPUT_POSTCODE"].mean() > 0

    def test_test_convert_all_columns(
        self, sample_df, validation_config, master_postcodes, auto_identify_config
    ):
        tester = IdentifyPostcodes(
            sample_df, validation_config, master_postcodes, auto_identify_config
        )
        tester.set_full_candidate_df()
        tester.test_convert_all_columns()
        results = tester.return_conversion_test_results()
        assert (
            len(results) == len(sample_df.columns) * 2
        )  # Direct and indirect for each column


# Test ConvertPostcodes class
class TestConvertPostcodes:
    def test_successful_conversion(
        self, sample_df, validation_config, master_postcodes
    ):
        convert_results = pd.DataFrame(
            {
                "CONVERSION_SUCCESS_RATE": [0.9],
                "COLUMN": ["postcode"],
                "METHOD": ["DIRECT"],
            }
        )
        converter = ConvertPostcodes(
            sample_df,
            validation_config,
            master_postcodes,
            convert_results,
            threshold=0.8,
        )
        converter.set_best_postcode_conversion_config()
        converter.convert_column()
        result = converter.return_converted_df()
        assert not result.empty
        assert "CORRECT_INPUT_POSTCODE" in result.columns

    def test_below_threshold(self, sample_df, validation_config, master_postcodes):
        convert_results = pd.DataFrame(
            {
                "CONVERSION_SUCCESS_RATE": [0.7],
                "COLUMN": ["postcode"],
                "METHOD": ["DIRECT"],
            }
        )
        converter = ConvertPostcodes(
            sample_df,
            validation_config,
            master_postcodes,
            convert_results,
            threshold=0.8,
        )
        converter.set_best_postcode_conversion_config()
        assert not converter.check_threshold()

    def test_invalid_column(self, sample_df, validation_config, master_postcodes):
        convert_results = pd.DataFrame(
            {
                "CONVERSION_SUCCESS_RATE": [0.9],
                "COLUMN": ["nonexistent"],
                "METHOD": ["DIRECT"],
            }
        )
        converter = ConvertPostcodes(
            sample_df, validation_config, master_postcodes, convert_results
        )
        with pytest.raises(ValueError):
            converter.set_best_postcode_conversion_config()


# Test auto_convert_postcodes function
def test_auto_convert_successful(
    sample_df, validation_config, master_postcodes, auto_identify_config
):
    result_df, success, test_results = auto_convert_postcodes(
        sample_df, validation_config, master_postcodes, auto_identify_config
    )
    assert success
    assert not result_df.empty
    assert not test_results.empty


def test_auto_convert_no_valid_columns(
    validation_config, master_postcodes, auto_identify_config
):
    invalid_df = pd.DataFrame({"col1": ["abc", "def"], "col2": ["ghi", "jkl"]})
    result_df, success, test_results = auto_convert_postcodes(
        invalid_df, validation_config, master_postcodes, auto_identify_config
    )
    assert not success
    assert result_df.equals(invalid_df)
