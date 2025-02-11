"""
Reusable classes and functions for automatically identifying and extracting postcodes from tabular data.
It's rather verbose to allow it to be used in pipelines, notebooks and streamlit front-ends.
"""

from copy import deepcopy

import pandas as pd

from singapore_postcode_geocoding.pipelines.data_validation.nodes import (
    validate_and_format_postcodes,
)


def regex_extract(candidates: pd.Series, regex_pattern: str) -> pd.Series:
    """Extract first regex pattern match from a pandas Series.

    Args:
        candidates: Series containing potential postcode values
        regex_pattern: Regular expression pattern to match postcodes

    Returns:
        Series containing first matched pattern or NaN if no match

    Example:
        >>> s = pd.Series(["Address 123456", "No code", "Code 12345"])
        >>> pattern = r"(?<!\d)\d{5,6}(?!\d)"
        >>> regex_extract(s, pattern)
        0    123456
        1       NaN
        2     12345
        dtype: object
    """
    candidates_string = candidates.astype("string")
    candidates_string = pd.Series(
        candidates_string.str.extract(f"{regex_pattern}", expand=True).iloc[:, 0].values
    )
    return candidates_string


class IdentifyPostcodes:
    """Test different postcode conversion strategies on a DataFrame.

    Args:
        df: Input DataFrame
        validation_config: Configuration for postcode validation with structure:
            {
                "validation_field_names": {
                    "candidate_postcode": "POSTCODE",
                    "extracted_postcode": "EXTRACTED",
                    "correct_input_flag": "VALID"
                },
                ...other config options...
            }
        master_postcodes: Reference DataFrame of valid postcodes
        auto_identify_config: Configuration for automatic identification with structure:
            {
                "regex_pattern": Pattern to match postcodes,
                "sample_size": Optional number of rows to sample,
                "candidate_columns": Optional list of columns to test,
                "seed": Optional random seed for sampling
            }

    Example:
        >>> df = pd.DataFrame(
        ...     {
        ...         "address": ["123456 Main St", "No code"],
        ...         "postcode": ["123456", "789012"],
        ...     }
        ... )
        >>> validation_config = {
        ...     "validation_field_names": {
        ...         "candidate_postcode": "POSTCODE",
        ...         "extracted_postcode": "EXTRACTED",
        ...         "correct_input_flag": "VALID",
        ...     }
        ... }
        >>> tester = TestConvertPostcodes(
        ...     df=df,
        ...     validation_config=validation_config,
        ...     master_postcodes=master_df,
        ...     auto_identify_config={"regex_pattern": r"\\d{6}"},
        ... )
        >>> tester.test_convert_all_columns()
        >>> results = tester.return_conversion_test_results()
    """

    def __init__(
        self,
        df: pd.DataFrame,
        validation_config: dict,
        master_postcodes: pd.DataFrame,
        auto_identify_config: dict,
    ) -> None:
        self.set_input_df(df)
        self.set_validation_config(validation_config)
        self.set_master_postcodes(master_postcodes)
        self.set_auto_identify_config(auto_identify_config)
        self.set_test_validation_config(validation_config)

    def set_input_df(self, df: pd.DataFrame):
        self.df = df.copy()

    def set_validation_config(self, config: dict) -> None:
        self.validation_config = deepcopy(config)
        self._candidate_postcode_field = config["validation_field_names"][
            "candidate_postcode"
        ]
        self._extracted_postcode_field = config["validation_field_names"][
            "extracted_postcode"
        ]
        self._correct_input_flag_field = config["validation_field_names"][
            "correct_input_flag"
        ]

    def set_master_postcodes(self, master_postcodes: pd.DataFrame) -> None:
        self.master_postcodes = master_postcodes

    def set_auto_identify_config(self, config: dict) -> None:
        self.auto_identify_config = deepcopy(config)
        self._regex_pattern = config["regex_pattern"]
        self._sample_size = config.get("sample_size")
        self._candidate_columns = config.get("candidate_columns") or None
        self._seed = config.get("seed")

    def set_test_validation_config(self, config: dict) -> None:
        test_validation_settings = deepcopy(config)
        test_validation_settings["drop_incorrect"] = False
        test_validation_settings["keep_formatted_postcode_field"] = True
        test_validation_settings["keep_validation_fields"] = True
        self.test_validation_settings = test_validation_settings

    def set_sample_candidate_df(self) -> None:
        if self._sample_size is None:
            self.candidate_df = self.df.copy()
        else:
            self.candidate_df = self.df.sample(
                n=min(self._sample_size, len(self.df)), random_state=self._seed
            )

    def set_full_candidate_df(self, df: pd.DataFrame | None = None):
        if df is None:
            df = self.df
        self.candidate_df = df.copy()

    def direct_conversion(
        self, candidates: pd.Series, validation_config: dict
    ) -> pd.DataFrame:
        """Convert series directly to postcodes without extraction.

        Args:
            candidates: Series of potential postcode values
            validation_config: Configuration for validation

        Returns:
            DataFrame with validated postcodes

        Example:
            >>> s = pd.Series(["123456", "789012"])
            >>> result = direct_conversion(s, config)
            >>> result["VALID"].mean()
            1.0
        """
        return validate_and_format_postcodes(
            df=pd.DataFrame({self._candidate_postcode_field: candidates}),
            input_col=self._candidate_postcode_field,
            postcode_validation_config=validation_config,
            master_postcodes=self.master_postcodes,
        )  # this can be a generic allowing any validation function to be created and passed

    def indirect_extraction(
        self,
        candidates: pd.Series,
        validation_config: dict,
    ) -> pd.DataFrame:
        """Extract and validate postcodes from text using regex.

        Args:
            candidates: Series containing text with potential postcodes
            validation_config: Configuration for validation

        Returns:
            DataFrame with extracted and validated postcodes

        Example:
            >>> s = pd.Series(["Address: 123456", "Location: 789012"])
            >>> result = indirect_extraction(s, config)
            >>> result["EXTRACTED"].head()
            0    123456
            1    789012
            Name: EXTRACTED, dtype: object
        """
        return (
            self.direct_conversion(
                regex_extract(candidates, self._regex_pattern), validation_config
            )
            .rename(
                columns={self._candidate_postcode_field: self._extracted_postcode_field}
            )
            .assign(**{self._candidate_postcode_field: candidates})
        )

    def test_convert(self, columns: list[str]) -> None:
        def calc_direct_conversion_stats(df, col, results):
            direct_conversion_stats = self.direct_conversion(
                df[col], self.test_validation_settings
            )[self._correct_input_flag_field].mean()
            results.append(
                {
                    "CONVERSION_SUCCESS_RATE": direct_conversion_stats,
                    "COLUMN": col,
                    "METHOD": "DIRECT",
                }
            )

        def calc_indirect_conversion_stats(df, col, results):
            indirect_conversion_stats = self.indirect_extraction(
                df[col], self.test_validation_settings
            )[self._correct_input_flag_field].mean()
            results.append(
                {
                    "CONVERSION_SUCCESS_RATE": indirect_conversion_stats,
                    "COLUMN": col,
                    "METHOD": "INDIRECT",
                    "REGEX_PATTERN": self._regex_pattern,
                }
            )

        results = []
        df = self.candidate_df
        for col in columns:
            calc_direct_conversion_stats(df, col, results)
            calc_indirect_conversion_stats(df, col, results)

        self.test_convert_results = pd.DataFrame(results).sort_values(
            ["CONVERSION_SUCCESS_RATE", "METHOD"], ascending=[False, True]
        )

    def test_convert_all_columns(self) -> None:
        self.test_convert(list(self.candidate_df.columns))

    def test_convert_columns(self, columns=None | list[str]) -> None:
        if columns is None:
            if self._candidate_columns is None:
                pass
                columns = list(self.candidate_df.columns)
            else:
                columns = self._candidate_columns
        self.test_convert(columns)

    def return_conversion_test_results(self) -> pd.DataFrame:
        return self.test_convert_results


class ConvertPostcodes:
    """Convert postcodes using the best identified method.

    Applies the most successful conversion method (direct or indirect)
    based on test results.

    Args:
        df: Input DataFrame
        validation_config: Configuration for postcode validation
        master_postcodes: Reference DataFrame of valid postcodes
        convert_results: Results from TestConvertPostcodes
        threshold: Minimum success rate required (0-1)

    Example:
        >>> converter = ConvertPostcodes(
        ...     df=df,
        ...     validation_config=config,
        ...     master_postcodes=master_df,
        ...     convert_results=test_results,
        ...     threshold=0.8,
        ... )
        >>> converter.convert_column()
        >>> result_df = converter.return_converted_df()
    """

    def __init__(
        self,
        df: pd.DataFrame,
        validation_config: dict,
        master_postcodes: pd.DataFrame,
        convert_results: pd.DataFrame,
        threshold: float | None = None,
    ) -> None:
        self.set_input_df(df)
        self.threshold = threshold
        self.set_validation_config(validation_config)
        self.set_master_postcodes(master_postcodes)
        self.set_convert_results(convert_results)
        self.converted_df = pd.DataFrame()

    def set_input_df(self, df: pd.DataFrame):
        self.df = df.copy()

    def set_validation_config(self, config: dict) -> None:
        self.validation_config = deepcopy(config)
        self._candidate_postcode_field = config["validation_field_names"][
            "candidate_postcode"
        ]
        self._extracted_postcode_field = config["validation_field_names"][
            "extracted_postcode"
        ]

    def set_master_postcodes(self, master_postcode: pd.DataFrame) -> None:
        self.master_postcodes = master_postcode

    def set_convert_results(self, convert_test_results: pd.DataFrame) -> None:
        self.convert_results = convert_test_results

    def set_conversion_column_method(self) -> None:
        self._success_rate = (
            self.postcode_conversion_config.get("CONVERSION_SUCCESS_RATE") or 0
        )
        self._conversion_column = self.postcode_conversion_config["COLUMN"]
        self._conversion_method = self.postcode_conversion_config["METHOD"]
        self._regex_pattern = self.postcode_conversion_config.get("REGEX_PATTERN")
        if self._conversion_column not in self.df.columns:
            raise ValueError("Selected column not in DataFrame")
        if self._conversion_method not in ["DIRECT", "INDIRECT"]:
            raise ValueError("Invalid conversion method selected")
        if self._conversion_method == "INDIRECT" and self._regex_pattern is None:
            raise ValueError("No regex pattern provided for indirect conversion")

    def check_threshold(self) -> bool:
        if self.threshold is None:
            return True
        else:
            return self._success_rate > self.threshold

    def set_best_postcode_conversion_config(self) -> None:
        self.postcode_conversion_config = dict(
            deepcopy(
                self.convert_results.sort_values(
                    ["CONVERSION_SUCCESS_RATE", "METHOD"], ascending=[False, True]
                ).iloc[0]
            )
        )
        self.set_conversion_column_method()

    def set_postcode_conversion_config(
        self, column, method=None, regex_pattern=None
    ) -> None:
        self.postcode_conversion_config = dict(
            self.convert_results.filter(f"COLUMN == {column}").iloc[0]
        )
        if method:
            self.postcode_conversion_config["METHOD"] = method
        if regex_pattern:
            self.postcode_conversion_config["REGEX_PATTERN"] = regex_pattern
        self.set_conversion_column_method()

    def convert_column(self):
        df = self.df.copy()
        if self._conversion_method == "INDIRECT":
            df = df.assign(
                **{
                    self._extracted_postcode_field: regex_extract(
                        df[self._conversion_column], self._regex_pattern
                    )
                }
            )
            conversion_column = self._extracted_postcode_field
        else:
            conversion_column = self._conversion_column

        df = validate_and_format_postcodes(
            df=df,
            input_col=conversion_column,
            postcode_validation_config=self.validation_config,
            master_postcodes=self.master_postcodes,
        )
        self.converted_df = df.copy()

    def return_converted_df(self) -> pd.DataFrame:
        return self.converted_df


def auto_convert_postcodes(
    df, validation_config, master_postcodes, auto_identify_config
) -> tuple[pd.DataFrame, bool, pd.DataFrame]:
    """Automatically identify and convert postcodes in a DataFrame.

    Tests all columns for potential postcodes and applies the best
    conversion method if it meets the success threshold.

    Args:
        df: Input DataFrame
        validation_config: Configuration for postcode validation
        master_postcodes: Reference DataFrame of valid postcodes
        auto_identify_config: Configuration for automatic identification
            Including:
            - regex_pattern: Pattern to match postcodes
            - success_threshold: Minimum success rate required
            - sample_size: Number of rows to test (optional)

    Returns:
        tuple containing:
        - Converted DataFrame
        - Success flag (True if conversion met threshold)
        - Test results DataFrame

    Example:
        >>> df = pd.DataFrame(
        ...     {
        ...         "address": ["123456 Main St", "No code"],
        ...         "postcode": ["123456", "789012"],
        ...     }
        ... )
        >>> config = {"regex_pattern": r"\\d{6}", "success_threshold": 0.8}
        >>> result_df, success, test_results = auto_convert_postcodes(
        ...     df=df,
        ...     validation_config=validation_config,
        ...     master_postcodes=master_df,
        ...     auto_identify_config=config,
        ... )
    """
    test_convert_postcodes = IdentifyPostcodes(
        df, validation_config, master_postcodes, auto_identify_config
    )
    test_convert_postcodes.set_sample_candidate_df()
    test_convert_postcodes.test_convert_all_columns()
    convert_test_results = test_convert_postcodes.return_conversion_test_results()
    convert_postcodes = ConvertPostcodes(
        df,
        validation_config,
        master_postcodes,
        convert_test_results,
        auto_identify_config["success_threshold"],
    )
    convert_postcodes.set_best_postcode_conversion_config()
    if convert_postcodes.check_threshold() is False:
        return df, False, convert_test_results
    else:
        convert_postcodes.convert_column()
        return convert_postcodes.return_converted_df(), True, convert_test_results
