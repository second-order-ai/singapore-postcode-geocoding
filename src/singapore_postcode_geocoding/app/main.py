from pathlib import Path

import pandas as pd
import streamlit as st
from kedro.framework.project import configure_project
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
import time
import humanize

st.logo(
    "src/singapore_postcode_geocoding/app/assets/512x512_fav_512x512_logomark.png",
    size="large",
    link=None,
    icon_image="src/singapore_postcode_geocoding/app/assets/512x512_fav_512x512_logomark.png",
)

# Page setting
st.set_page_config(layout="wide", page_title="Singapore postcode geocoder")
st.sidebar.title("How to use the app")
st.sidebar.write(
    """
    Easily geocode your Singapore postal codes:
    1. Upload a file with a column of postal codes
    2. The will then geocoded data with latitude and longitude coordinates, and other information from the mater postal-code geodataset.
    3. View results in a table and on a map.
    4. Download the results as a CSV, Excel or Parquet file.

    An example file can be downloaded below.
    """
)

if "context" not in st.session_state:
    # Initialize Kedro context and catalog
    path = Path.cwd()
    project_path = path
    metadata = bootstrap_project(project_path)
    configure_project(metadata.package_name)
    session = KedroSession.create(
        project_path,
        env="local",
        extra_params=None,
        conf_source="conf",
    )
    context = session.load_context()
    catalog = context.catalog

    st.session_state["context"] = context
    st.session_state["catalog"] = context.catalog
    st.session_state["data"] = {}
    st.rerun()


def load_data(filename):
    if "catalog" in st.session_state:
        catalog = st.session_state["catalog"]
        return catalog.load(filename)
    else:
        raise ValueError("Catalog not found in session state")


def load_geocoded_postcodes():
    return load_data("singapore_postcodes_geocoded")


@st.cache_data
def return_postcodes():
    if "geocoded_postcodes" not in st.session_state["data"]:
        st.session_state["data"]["geocoded_postcodes"] = load_geocoded_postcodes()
    return st.session_state["data"]["geocoded_postcodes"]


postcode = return_postcodes()

# Streamlit app
st.title("Singapore postcode geocoder")
st.write("")

# File uploader
uploaded_files = st.file_uploader(
    "Upload your file that contains a postcode column:",
    type=None,
    help="Accepted file types include .csv, .xlsx, .parquet, and their compressed versions (.zip, .gzip, .bz2). Multiple files can be uploaded at once, if they have the same structure.",
    accept_multiple_files=True,
)

if uploaded_files:
    # Read the uploaded file
    if len(uploaded_files) > 1:
        user_df = pd.concat(
            [
                pd.read_csv(uploaded_file).assign(FILENAME=uploaded_file.name)
                for uploaded_file in uploaded_files
            ]
        ).reset_index(drop=True)
        name = f"{uploaded_files[0].name}--{uploaded_files[1].name}"
    else:
        user_df = pd.read_csv(uploaded_files[0])
        name = uploaded_files[0].name

    # Display the uploaded file
    heading_col, view_all_column = st.columns(2)
    with heading_col:
        st.write("Uploaded DataFrame:")
    with view_all_column:
        view_all = st.checkbox("View full table")
    if not view_all:
        st.write(user_df.head())
    else:
        st.write(user_df)
    st.write(postcode["POSTAL"].astype(int).min())
    st.write(postcode["POSTAL"].astype(int).max())
    # Select the column with postal codes
    postal_code_column = st.selectbox(
        "Select the column with postal codes",
        user_df.columns,
        index=None,
    )

    start_time = time.perf_counter()
    if postal_code_column in user_df.columns:
        # Do some basic tests first to check if it is a singapore postcode, and remove those that don't meet the format.
        # Has to be numbers, digits and
        user_df_format = user_df.assign(
            __merge_postal_code=user_df[postal_code_column]
            .astype("Int64")
            .astype("string")
            .str.zfill(6)
        )
        # Merge the uploaded file with the internal dataset
        merged_df = user_df_format.merge(
            postcode.drop_duplicates("POSTAL"),
            left_on="__merge_postal_code",
            right_on="POSTAL",
            how="left",
            suffixes=("", "_GEOCODED_DATASET"),
            validate="m:1",
        ).drop(columns=["__merge_postal_code"])

        total_records = len(user_df)
        matched_records = merged_df["ADDRESS"].notna().sum()
        fraction_merged = matched_records / total_records
        process_time = time.perf_counter() - start_time
        human_readable_time = humanize.naturaldelta(
            process_time, minimum_unit="milliseconds"
        )
        st.caption(
            f"Processed **{total_records}** records in **{human_readable_time}** and matched **{matched_records}** records ({fraction_merged:.2%})."
        )
        geo_heading_col, geo_view_all_column = st.columns(2)
        with geo_heading_col:
            st.markdown(
                "Geocoded postal codes:",
                help=f"""
The geo-info (latitude and longitude info) are added as new fields, together with other fields from the master postal code geodataset.
The fields include:

1. `POSTAL`: the postal code.
2. `ADDRESS`: full address, including Singapore and the postal code.
3. `BLK_NO`: block or house number.
4. `BUILDING`: building name, if available.
5. `ROAD_NAME`: road name.
6. `LATITUDE` and `LONGITUDE`: coordinates of the postal code in `EPSG:4326` projection.
7. `SOURCE`: source of the geocoded data.
8. `URL`: URL of the source, where the original data can be downloaded from.
9. `SOURCE_2`: additional source of the geocoded data, when two datasets were combined.
10. `URL_2`: URL of the additional source, where the original data can be downloaded from.

If these field names were in the uploaded file, they will have the `_GEOCODED_DATASET` suffix.
""",
            )
        with geo_view_all_column:
            view_all_geo = st.checkbox("View full geocoded table")
        if not view_all_geo:
            st.write(merged_df.head())
        else:
            st.write(merged_df)

        # Option to download the merged DataFrame
        csv = merged_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download the geocoded data as a CSV file",
            data=csv,
            file_name=f"{name}_postal_geocoded.csv",
            mime="text/csv",
        )
