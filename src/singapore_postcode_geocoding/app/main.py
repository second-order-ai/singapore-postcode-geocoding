import re
import time
from pathlib import Path
import pydeck as pdk
from pydeck.data_utils.viewport_helpers import compute_view
import humanize
import pandas as pd
import streamlit as st
from kedro.framework.project import configure_project
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from singapore_postcode_geocoding.pipelines.data_validation.singapore_postcode_validation import (
    validate_and_format_postcodes,
)
from singapore_postcode_geocoding.pipelines.postcode_identification.nodes.auto_identification import (
    calculate_all_match_success,
    find_best_postcode_column,
    auto_convert_postcode_column,
)

import pandas as pd


# Page setting
st.set_page_config(layout="wide", page_title="Singapore postcode geocoder")
st.sidebar.title("How to use the app")
st.sidebar.write(
    """
    Easily geocode your Singapore postal codes:
    1. Upload a file that contains postal code information. Multiple files can be uploaded at once, if they have the same structure.
    2. View the geocoding results in a table and on a map.
    3. Download the results as a CSV file at the bottom of the page.
    4. Clear the files in the upload box, and upload new files to geocode.
    """
)
st.sidebar.caption(
    """
    How it works:
     * The app will automatically try to find and extract postal code information. 
     * Postal codes can be in a dedicated field, or imbedded in a field (eg. full address).
     * You can manually select the field with postal code info if the automatic detection fails.
     * The postal codes are geocoded with latitude and longitude coordinates, and other information from the master postal-code geo-dataset.
    """
)
csv_test = (
    pd.read_csv(
        "https://raw.githubusercontent.com/second-order-ai/singapore-postcode-geocoding/refs/heads/main/data/01_raw/test/GreenMarkBuildings.csv"
    )
    .to_csv(index=False)
    .encode("utf-8")
)
st.sidebar.download_button(
    label="An example file can be downloaded below here",
    data=csv_test,
    file_name=f"GreenMarkBuildings__test_file.csv",
    mime="text/csv",
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


def load_geocoded_postcodes_master_list():
    return load_data("singapore_postcodes_masterlist")


@st.cache_data
def return_postcodes():
    if "geocoded_postcodes" not in st.session_state["data"]:
        st.session_state["data"]["geocoded_postcodes"] = load_geocoded_postcodes()
    return st.session_state["data"]["geocoded_postcodes"]


@st.cache_data
def return_postcodes_masterlist():
    if "geocoded_postcodes_master_list" not in st.session_state["data"]:
        st.session_state["data"]["geocoded_postcodes_master_list"] = (
            load_geocoded_postcodes_master_list()
        )
    return st.session_state["data"]["geocoded_postcodes_master_list"]


def generate_map(plot_df) -> None:
    point_layer = pdk.Layer(
        "ScatterplotLayer",
        data=plot_df,
        get_position=["LONGITUDE", "LATITUDE"],
        get_color=[255, 0, 0, 128],  # Red with 50% transparency
        pickable=True,
        auto_highlight=True,
        get_radius=75,
    )
    computed_view = compute_view(
        points=plot_df[["LONGITUDE", "LATITUDE"]],
        view_proportion=0.9,
    )
    # Set view state centered on Singapore
    view_state = pdk.ViewState(
        longitude=computed_view.longitude,
        latitude=computed_view.latitude,
        zoom=computed_view.zoom,
        min_zoom=9,  # Show at least all of Singapore
        max_zoom=20,  # Max zoom for building level detail
        pitch=0,  # Top-down view
        bearing=0,  # Align to true north
    )
    # Create and display deck
    deck = pdk.Deck(
        layers=[point_layer],
        initial_view_state=view_state,
        tooltip={"text": "{ADDRESS}\nPostal Code: {POSTAL}"},
        map_style=None,
    )

    st.pydeck_chart(deck)


postcode = return_postcodes()
postcode_masterlist = return_postcodes_masterlist()

# Streamlit app
st.title("Singapore postcode geocoder")

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
        filename_1 = re.sub(r"\..*$", "", uploaded_files[0].name)
        filename_2 = re.sub(r"\..*$", "", uploaded_files[1].name)
        name = f"{filename_1}--{filename_2}"
    else:
        user_df = pd.read_csv(uploaded_files[0])
        name = re.sub(r"\..*$", "", uploaded_files[0].name)

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
    # Select the column with postal codes

    # auto-extract postcode here:
    auto_extract_prediction = calculate_all_match_success(
        df=user_df, master_postcodes=postcode_masterlist
    )
    best_match, success_candidate_flag = find_best_postcode_column(
        auto_extract_prediction, success_threshold=0.1
    )
    if success_candidate_flag:
        if best_match["TYPE"] == "DIRECT":
            st.caption(
                f"`{best_match['FIELD']}` is the best candidate postcode column. The predicated success rate for using it is {best_match['SUCCESS_RATE'] * 100}%"
            )
        else:
            st.caption(
                f"The `{best_match['FIELD']}` is the best candidate column with imbedded postcode info. The predicated success rate for using it is {best_match['SUCCESS_RATE'] * 100}%"
            )
    else:
        st.caption(
            f"Unfortunately, there are no good candidate columns with postcode info. The best-predicated success rate was only {best_match['SUCCESS_RATE']}. Please select one manually to proceed."
        )

    postal_code_column = st.selectbox(
        "Select or change the column with postal codes:",
        user_df.columns,
        index=None,
        help="Select the column with postal code info to geocode. If the automatic detection failed, or if you want to use a different column, please select the correct one manually.",
    )

    start_time = time.perf_counter()
    if postal_code_column in user_df.columns and postal_code_column is not None:
        # Do some basic tests first to check if it is a singapore postcode, and remove those that don't meet the format.
        # Has to be numbers, digits and
        extraction_method = st.radio(
            "Select postcode extraction method:",
            options=[
                "dedicated",  # Direct column matching
                "embedded",  # Extract from text
                "auto-detect",  # Try both methods
            ],
            help="Choose how to extract postcodes from the selected column:\n"
            "- Dedicated: The column only contains postcodes\n"
            "- Embedded: Extract postcodes from within column-text, such as addresses\n"
            "- Auto-detect: Automatically find and use the best method based on the highest success rate",
        )

        user_df_format = validate_and_format_postcodes(
            df=user_df,
            input_col=postal_code_column,
            master_postcodes=postcode_masterlist,
        )

        # Merge the uploaded file with the internal dataset
        merged_df = user_df_format.merge(
            postcode.drop_duplicates("FORMATTED_POSTCODE"),
            left_on="FORMATTED_POSTCODE",
            right_on="FORMATTED_POSTCODE",
            how="left",
            suffixes=("", "_GEOCODED_DATASET"),
            validate="m:1",
        )

        total_records = len(user_df)
        matched_records = merged_df["ADDRESS"].notna().sum()
        fraction_merged = matched_records / total_records
        process_time = time.perf_counter() - start_time
        human_readable_time = humanize.naturaldelta(
            process_time, minimum_unit="milliseconds"
        )
        st.caption(
            f"Processed **{total_records}** records in **{human_readable_time}** and matched **{matched_records}** records ({fraction_merged:.2%}) in the `{postal_code_column}` column."
        )
        geo_heading_col, geo_view_all_column = st.columns(2)
        with geo_heading_col:
            st.markdown(
                "Geocoded postal codes:",
                help="""
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
        non_merged = merged_df.loc[merged_df["LATITUDE"].isna()]
        if len(non_merged) > 0:
            st.caption(
                f"**{len(non_merged)}** ({len(non_merged) / len(merged_df) * 100:.2f}%) postal codes could not be geocoded."
            )
            tab1, tab2 = st.tabs(["Geocoded data map", "Failed geocoded data"])
            with tab2:
                st.write(
                    "Below are the reasons for the failed matches and the number and \% of records at fault:"
                )
                wrong_stat = pd.DataFrame(
                    non_merged["INCORRECT_INPUT_POSTCODE_REASON"].value_counts()
                )
                wrong_stat = wrong_stat.assign(
                    **{"%": (wrong_stat["count"] / len(non_merged)).round(2) * 100}
                )
                st.write(wrong_stat)
                st.write("The records at fault:")
                st.write(
                    "Below are the records at fault. See the `INCORRECT_INPUT_POSTCODE_REASON` column at the end of the table:"
                )
                st.write(non_merged)
                st.write(
                    "Note that the download file contains both the geocoded and failed geocoded data."
                )
            with tab1:
                st.caption(
                    "The map below shows the location of the successfully geocoded postal codes. The address and postcode of each location are displayed when hovering over the points."
                )
                generate_map(merged_df.dropna(subset=["LATITUDE", "LONGITUDE"]))
        else:
            st.caption(
                "The map below shows the location of the successfully geocoded postal codes. The address and postcode of each location are displayed when hovering over the points."
            )
            generate_map(merged_df.dropna(subset=["LATITUDE", "LONGITUDE"]))

        # Option to download the merged DataFrame
        csv = merged_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download the geocoded data as a CSV file",
            data=csv,
            file_name=f"{name}__postal_geocoded.csv",
            mime="text/csv",
            help="Note that the postal codes that could not be geocoded are included in the download.",
        )
