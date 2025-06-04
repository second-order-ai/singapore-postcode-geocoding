# Master Data Processing Pipeline

This pipeline processes and standardizes Singapore postal code data from multiple sources, combining them into a comprehensive geocoded dataset.

## Overview

The pipeline integrates data from three primary sources:
1. OneMap (Singapore's official geospatial platform)
2. OpenData (from opendatasoft)
3. PostcodeBase

## Pipeline Steps

### 1. Data Formatting
- **OneMap Formatting**
  - Standardizes postal codes to 6-digit format
  - Adds source attribution and URLs
  - Required columns: ['POSTAL', 'X', 'Y']

- **OpenData Formatting**
  - Filters for Singapore postal codes
  - Standardizes postal codes
  - Formats addresses and coordinates
  - Required columns: ['country_code', 'postal_code', 'place_name', 'latitude', 'longitude']

- **PostcodeBase Formatting**
  - Standardizes postal codes
  - Formats addresses
  - Required columns: ['address', 'postcode']

### 2. Data Enrichment
- Enriches OpenData with additional address information from PostcodeBase
- Merges data based on postal codes
- Preserves all source information

### 3. Data Extension
- Extends OneMap data with additional postal codes from enriched OpenData
- Ensures no duplicate postal codes
- Maintains data quality and consistency

### 4. Type Conversion
Converts all columns to appropriate data types:
- POSTAL: Int64
- ADDRESS: string
- BLK_NO: string
- BUILDING: string
- ROAD_NAME: string
- LATITUDE: float
- LONGITUDE: float
- SOURCE: string
- URL: string

### 5. Master List Creation
- Creates a deduplicated master list of all Singapore postal codes
- Serves as the reference dataset for postal code validation

## Output Datasets

1. **singapore_postcodes_geocoded**
   - Complete dataset with all postal codes and their geocoding information
   - Includes source attribution and URLs
   - Standardized format and data types

2. **singapore_postcodes_masterlist**
   - Deduplicated list of all valid Singapore postal codes
   - Used for validation in other pipelines

## Data Model

### singapore_postcodes_geocoded
| Column Name | Type | Description | Example |
|------------|------|-------------|---------|
| POSTAL | Int64 | 6-digit Singapore postal code | 238801 |
| ADDRESS | string | Full address in uppercase | "1 RAFFLES PLACE SINGAPORE 238801" |
| BLK_NO | string | Block number (if applicable) | "1" |
| BUILDING | string | Building name (if applicable) | "RAFFLES PLACE" |
| ROAD_NAME | string | Street name in uppercase | "RAFFLES PLACE" |
| LATITUDE | float | Geographic latitude in decimal degrees | 1.2847 |
| LONGITUDE | float | Geographic longitude in decimal degrees | 103.8517 |
| SOURCE | string | Primary data source | "OneMap" |
| URL | string | Primary source URL | "https://www.onemap.gov.sg/apidocs/search" |
| SOURCE_2 | string | Secondary data source (if applicable) | "Postcodebase" |
| URL_2 | string | Secondary source URL (if applicable) | "https://sgp.postcodebase.com/all" |
| FORMATTED_POSTCODE | string | Standardized 6-digit postal code with leading zeros | "238801" |

### singapore_postcodes_masterlist
| Column Name | Type | Description | Example |
|------------|------|-------------|---------|
| FORMATTED_POSTCODE | string | Standardized 6-digit postal code with leading zeros | "238801" |

Notes:
- All postal codes are standardized to 6 digits with leading zeros
- Addresses are stored in uppercase without commas
- Geographic coordinates are in decimal degrees (WGS84)
- Source attribution is maintained for data lineage
- The masterlist contains only unique postal codes

## Usage

This pipeline is the default pipeline in the project and can be run using:
```bash
kedro run
```

To run only this pipeline:
```bash
kedro run --pipeline=master_data_processing
```

## Dependencies

- pandas
- kedro
- Custom validation modules from the project

## Configuration

The pipeline uses the following configuration parameters:
- postcode_validation: Configuration for postal code validation and formatting
- Various source-specific parameters for data processing

## Data Quality

The pipeline ensures data quality through:
- Standardization of postal codes to 6-digit format
- Validation against known valid postal codes
- Type checking and conversion
- Deduplication of records
- Source tracking and attribution