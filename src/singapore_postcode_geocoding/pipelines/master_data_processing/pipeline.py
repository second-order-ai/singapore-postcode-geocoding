"""Master data processing pipeline for Singapore postal codes.

This module defines the Kedro pipeline for processing and standardizing Singapore postal code data
from multiple sources. The pipeline follows these main steps:

1. Data Formatting:
   - Format OneMap data (official Singapore geospatial platform)
   - Format OpenData (from opendatasoft)
   - Format PostcodeBase data

2. Data Enrichment:
   - Enrich OpenData with additional address information from PostcodeBase

3. Data Extension:
   - Extend OneMap data with additional postal codes from enriched OpenData

4. Data Standardization:
   - Convert all columns to appropriate data types
   - Create a master list of unique postal codes

The pipeline ensures data quality through:
- Standardization of postal codes to 6-digit format
- Validation against known valid postal codes
- Type checking and conversion
- Deduplication of records
- Source tracking and attribution

Input Datasets:
    - one_map_scrape: Raw data from OneMap
    - open_data_postal_code: Raw data from OpenData
    - sg_postcode_based_via_getdata: Raw data from PostcodeBase

Output Datasets:
    - singapore_postcodes_geocoded: Complete geocoded dataset
    - singapore_postcodes_masterlist: Deduplicated list of valid postal codes
"""

from kedro.pipeline import Pipeline, node

from .nodes import (
    enrich_open_postcode,
    extend_onemap,
    format_onemap,
    format_opendata,
    format_postcodebase,
    postcode_full_type_conversion,
    return_postcode_master_list,
)


def create_pipeline(**kwargs) -> Pipeline:
    """Create the master data processing pipeline.

    This function creates a Kedro pipeline that processes and standardizes Singapore postal code data
    from multiple sources. The pipeline is structured as follows:

    1. Format raw data from each source:
       - OneMap data formatting
       - OpenData formatting
       - PostcodeBase formatting

    2. Enrich and extend the data:
       - Enrich OpenData with PostcodeBase addresses
       - Extend OneMap data with additional postal codes

    3. Standardize and create master list:
       - Convert all columns to appropriate types
       - Create deduplicated master list

    Returns:
        Pipeline: A Kedro pipeline object containing all the data processing nodes

    Note:
        The pipeline uses the postcode_validation configuration parameter for
        standardizing and validating postal codes across all processing steps.
    """
    return Pipeline(
        [
            # Format OneMap data
            node(
                func=format_onemap,
                inputs=["one_map_scrape", "params:postcode_validation"],
                outputs="formatted_onemap",
                name="format_onemap",
            ),
            # Format OpenData
            node(
                func=format_opendata,
                inputs=["open_data_postal_code", "params:postcode_validation"],
                outputs="formatted_opendata",
                name="format_opendata",
            ),
            # Format PostcodeBase data
            node(
                func=format_postcodebase,
                inputs=["sg_postcode_based_via_getdata", "params:postcode_validation"],
                outputs="formatted_postcodebase",
                name="format_postcodebase",
            ),
            # Enrich OpenData with PostcodeBase addresses
            node(
                func=enrich_open_postcode,
                inputs=[
                    "formatted_opendata",
                    "formatted_postcodebase",
                    "params:postcode_validation",
                ],
                outputs="enriched_opendata",
                name="enrich_opendata",
            ),
            # Extend OneMap data with additional postal codes
            node(
                func=extend_onemap,
                inputs=[
                    "formatted_onemap",
                    "enriched_opendata",
                    "params:postcode_validation",
                ],
                outputs="extended_onemap",
                name="extend_onemap",
            ),
            # Convert all columns to appropriate types
            node(
                func=postcode_full_type_conversion,
                inputs=["extended_onemap", "params:postcode_validation"],
                outputs="singapore_postcodes_geocoded",
                name="convert_types",
            ),
            # Create deduplicated master list, with only postcodes
            node(
                func=return_postcode_master_list,
                inputs=["singapore_postcodes_geocoded", "params:postcode_validation"],
                outputs="singapore_postcodes_masterlist",
                name="return_postcode_master_list",
            ),
        ]
    )
