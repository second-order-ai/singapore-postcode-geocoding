"""
The idea here is to search columns for typical post code aliases.
It could be used to break-ties for automated post code identification.
Or extended to identify other address type fields, in case of fuzzy matching.
"""

import re

import pandas as pd
from rapidfuzz import distance, fuzz


def substring_match(s1: str, s2: str) -> bool:
    """Check if s1 is a substring of s2."""
    return {True: 100, False: 0}[s1.lower() in s2.lower()]


def levenshtein_normalized_inv(s1: str, s2: str) -> float:
    """Calculate the inverse of the normalized Levenshtein distance."""
    return (1 - distance.Levenshtein.normalized_distance(s1, s2)) * 100


PATTERNS = ["postalcode", "postcode", "zipcode", "postal", "post", "zip"]
METRICS = {
    "ratio": fuzz.ratio,
    "partial_ratio": fuzz.partial_ratio,
    "levenshtein_normalized_inv": levenshtein_normalized_inv,
    "substring_match": substring_match,
}


def clean_field_name(field_name: str) -> str:
    """Clean field name by removing non-alphabet characters and converting to lowercase."""
    return re.sub(r"[^a-zA-Z]", "", str(field_name).lower())


def score_postcode_fields(
    df: pd.DataFrame,
    postcode_field_name_formatted_alias: list | None = None,
    metrics: list | None = None,
) -> pd.DataFrame:
    """Score DataFrame column names against typical postal code patterns."""
    if postcode_field_name_formatted_alias is None:
        postcode_field_name_formatted_alias = PATTERNS
    if metrics is None:
        metrics = ["levenshtein_normalized_inv", "substring_match"]
    # Clean column names
    field_names = {col: clean_field_name(str(col)) for col in df.columns}

    scores = []
    for original_name, clean_name in field_names.items():
        for pattern in postcode_field_name_formatted_alias:
            # Calculate various fuzzy match scores
            for metric in metrics:
                scores.append(
                    {
                        "field_name": original_name,
                        "clean_name": clean_name,
                        "pattern": pattern,
                        "metric": metric,
                        "score": METRICS[metric](pattern, clean_name),
                        "clean_name_length": len(clean_name),
                        "pattern_length": len(pattern),
                    }
                )

    return pd.DataFrame(scores).sort_values(
        ["score", "pattern_length", "pattern"], ascending=[False, False, True]
    )
