"""
Data validation utilities for Singapore postcode geocoding.

This module provides functions for validating and formatting Singapore postcodes
according to official standards and ranges.
"""

from .singapore_postcode_validation import (
    VALIDATION_DEFAULTS,
    check_in_master_dataset,
    check_is_integer,
    check_postcode_range,
    format_valid_postcodes,
    parse_numeric_postcodes,
    validate_and_format_postcodes,
)

__all__ = [
    "validate_and_format_postcodes",
    "VALIDATION_DEFAULTS",
    "parse_numeric_postcodes",
    "check_is_integer",
    "check_postcode_range",
    "format_valid_postcodes",
    "check_in_master_dataset",
]
