import pandas as pd
from pandas import Series


def get_postal_codes(postal_codes: Series) -> Series:
    """Convert postal codes to standardized 6-digit string format.

    Args:
        postal_codes: Series containing postal codes to standardize

    Returns:
        Series: Postal codes as 6-digit strings with leading zeros
    """
    return postal_codes.astype("string").str.zfill(6)


def format_onemap(df: pd.DataFrame) -> pd.DataFrame:
    """Format OneMap source data into standardized schema.

    Args:
        df: Raw OneMap DataFrame containing postal codes and coordinates
            Required columns: ['POSTAL', 'X', 'Y']

    Returns:
        pd.DataFrame: Formatted DataFrame with columns:
            - POSTAL (str): 6-digit postal code
            - SOURCE (str): Fixed value 'OneMap'
            - URL (str): Primary data source URL
            - URL_2 (str): Secondary data source URL
    """
    return df.assign(
        POSTAL=get_postal_codes(df["POSTAL"]),
        SOURCE="OneMap",
        URL="https://www.onemap.gov.sg/apidocs/search",
        URL_2="https://github.com/xuancong84/singapore-address-heatmap/blob/master/database.csv.gz",
    ).drop(columns=["X", "Y"])


def format_opendata(df: pd.DataFrame) -> pd.DataFrame:
    """Format OpenData source into standardized schema.

    Args:
        df: Raw OpenData DataFrame with global postal codes
            Required columns: ['country_code', 'postal_code', 'place_name',
                             'latitude', 'longitude']

    Returns:
        pd.DataFrame: Formatted DataFrame with columns:
            - ADDRESS (str): Uppercase full address with postal code
            - ROAD_NAME (str): Uppercase street name
            - LATITUDE (float): Decimal degrees
            - LONGITUDE (float): Decimal degrees
            - POSTAL (str): 6-digit postal code
            - SOURCE (str): Fixed value 'opendatasoft'
            - URL (str): Data source URL
    """
    df = df.loc[df["country_code"] == "SG"]
    df = df.assign(postal_code=get_postal_codes(df["postal_code"]))
    df = df.assign(
        ADDRESS=(df["place_name"] + " singapore " + df["postal_code"])
        .str.replace(",", "")
        .str.upper(),
        ROAD_NAME=df["place_name"].str.replace(",", "").str.upper(),
    )
    return df.rename(
        columns={
            "postal_code": "POSTAL",
            "latitude": "LATITUDE",
            "longitude": "LONGITUDE",
        },
        inplace=False,
    )[["ADDRESS", "ROAD_NAME", "LATITUDE", "LONGITUDE", "POSTAL"]].assign(
        SOURCE="opendatasoft",
        URL="https://public.opendatasoft.com/explore/dataset/geonames-postal-code/table/?refine.country_code=SG",
    )


def format_postcodebase(df: pd.DataFrame) -> pd.DataFrame:
    """Format PostcodeBase source into standardized schema.

    Args:
        df: Raw PostcodeBase DataFrame
            Required columns: ['address', 'postcode']

    Returns:
        pd.DataFrame: Formatted DataFrame with columns:
            - ADDRESS (str): Uppercase full address with postal code
            - POSTAL (str): 6-digit postal code
            - SOURCE (str): Fixed value 'Postcodebase'
            - URL (str): Data source URL
    """
    df = df.assign(postcode=get_postal_codes(df["postcode"]))
    df = df.assign(
        ADDRESS=(
            df["address"].str.replace(r" is located in Singapore.*$", "", regex=True)
            + " singapore "
            + df["postcode"]
        )
        .str.replace(",", "")
        .str.upper()
    )
    return df.rename(
        columns={
            "postcode": "POSTAL",
        },
        inplace=False,
    )[["ADDRESS", "POSTAL"]].assign(
        SOURCE="Postcodebase",
        URL="https://sgp.postcodebase.com/all",
    )


def enrich_open_postcode(
    open_postcode: pd.DataFrame, postcode_base: pd.DataFrame
) -> pd.DataFrame:
    """Enrich OpenData with PostcodeBase addresses.

    Args:
        open_postcode: DataFrame containing OpenData postal codes
        postcode_base: DataFrame containing PostcodeBase addresses

    Returns:
        pd.DataFrame: Enriched DataFrame with merged addresses
    """
    return open_postcode.drop(columns=["ADDRESS"]).merge(
        postcode_base,
        left_on="POSTAL",
        right_on="POSTAL",
        how="left",
        validate="m:1",
        suffixes=("", "_2"),
    )


def extend_onemap(
    onemap_postal_codes: pd.DataFrame, open_postcode_enriched: pd.DataFrame
) -> pd.DataFrame:
    """Extend OneMap data with additional postcodes from enriched OpenData.

    Args:
        onemap_postal_codes: DataFrame containing OneMap postal codes
        open_postcode_enriched: DataFrame containing enriched OpenData

    Returns:
        pd.DataFrame: Extended OneMap DataFrame with additional postcodes
    """
    additional_postal_codes = open_postcode_enriched.loc[
        ~open_postcode_enriched["POSTAL"].isin(onemap_postal_codes["POSTAL"].values)
    ].reset_index(drop=True)
    return pd.concat(
        [onemap_postal_codes, additional_postal_codes], ignore_index=True, axis=0
    )


def postcode_full_type_conversion(df: pd.DataFrame) -> pd.DataFrame:
    """Convert DataFrame columns to specified types.

    Args:
        df: DataFrame to convert

    Returns:
        pd.DataFrame: DataFrame with converted column types
    """
    rename_columns = {
        "POSTAL": "string",
        "ADDRESS": "string",
        "BLK_NO": "string",
        "BUILDING": "string",
        "ROAD_NAME": "string",
        "LATITUDE": "float",
        "LONGITUDE": "float",
        "SOURCE": "string",
        "URL": "string",
        "SOURCE_2": "string",
        "URL_2": "string",
    }
    return df.astype(rename_columns)[list(rename_columns.keys())]
