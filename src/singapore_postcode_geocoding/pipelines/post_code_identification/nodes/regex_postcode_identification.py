from pandas import DataFrame, Series
import pandas as pd
import logging
from typing import Optional

logger = logging.getLogger(__name__)

POSTCODE_PATTERN = r"(?<!\d)\d{5,6}(?!\d)"


def safe_encode_text(text: str) -> str:
    """Safely encode text to ASCII, replacing invalid characters."""
    try:
        # Handle different string types
        if isinstance(text, bytes):
            return text.decode("ascii", "ignore")
        return str(text).encode("ascii", "ignore").decode("ascii", "ignore")
    except Exception as e:
        logger.warning(f"Encoding error: {e}")
        return ""


def clean_text_series(series: Series) -> Series:
    """Clean and standardize text series."""
    # Handle numeric values
    numeric_mask = pd.to_numeric(series, errors="coerce").notna()

    # Initialize result series
    result = pd.Series(index=series.index, data="")

    # Process non-numeric values only
    non_numeric = ~numeric_mask & series.notna()
    result[non_numeric] = series[non_numeric].apply(safe_encode_text)

    return result


def extract_postcode_matches(series: Series, pattern: str) -> DataFrame:
    """Extract all postcode matches using regex pattern."""
    clean_series = clean_text_series(series)
    return clean_series.str.extract(f"({pattern})", expand=True)


def format_extracted_postcodes(df: DataFrame) -> DataFrame:
    """Format extracted postcodes with standardized column names."""
    n_postcodes = df.shape[1]
    if n_postcodes == 1:
        return df.rename(columns={0: "EXTRACTED_POSTCODE"})

    return df.rename(columns={i: f"EXTRACTED_POSTCODE_{i}" for i in range(n_postcodes)})


def extract_postcodes_from_series(
    series: Series, postcode_pattern: Optional[str] = None
) -> DataFrame:
    """Extract postcodes from text series."""
    pattern = postcode_pattern or POSTCODE_PATTERN
    matches = extract_postcode_matches(series, pattern)
    return format_extracted_postcodes(matches)
