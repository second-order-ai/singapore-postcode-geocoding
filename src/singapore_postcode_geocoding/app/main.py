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
from singapore_postcode_geocoding.data_validation.singapore_postcode_validation import (
    validate_and_format_postcodes,
)
from singapore_postcode_geocoding.pipelines.postcode_identification.auto_identification_classes_pipeline.nodes import (
    find_best_postcode_column,
    auto_convert_postcodes,
    IdentifyPostcodes,
)
from singapore_postcode_geocoding.app.config.app_config import APP_CONFIG
from singapore_postcode_geocoding.app.services.session_manager import SessionManager
from singapore_postcode_geocoding.app.components.sidebar import render_sidebar
from singapore_postcode_geocoding.app.components.file_uploader import handle_file_upload
from singapore_postcode_geocoding.app.components.map_view import generate_map
from singapore_postcode_geocoding.app.components.data_table import render_data_table
from singapore_postcode_geocoding.app.services.geocoding import process_geocoding

import pandas as pd


def main():
    # Initialize app
    st.set_page_config(**APP_CONFIG["page_config"])
    session_manager = SessionManager()
    
    # Render UI components
    render_sidebar()
    st.title("Singapore postcode geocoder")
    st.markdown("If there is a column in your data that contains Singapore postal codes, you can upload your file and use this app to geocode the data. The app will find the postal code automatically, even if it's imbedded in another column.")
    
    with st.expander("Watch a quick tutorial on how to use this app"):
        st.video("https://youtu.be/fsV5l-ndSuc")
    
    # Handle file upload and processing
    uploaded_files = st.file_uploader(
        "Upload your file that contains a postcode column:",
        type=None,
        help="Accepted file types include .csv, .xlsx, .parquet, and their compressed versions (.zip, .gzip, .bz2). Multiple files can be uploaded at once, if they have the same structure.",
        accept_multiple_files=True,
    )
    
    if uploaded_files:
        # Process uploaded files
        result = handle_file_upload(uploaded_files)
        if result:
            user_df, name = result
            # Geocode the data
            geocoding_result = process_geocoding(
                user_df=user_df,
                postcode_masterlist=session_manager.get_postcodes_masterlist(),
                postcode=session_manager.get_geocoded_postcodes(),
                validation_config=APP_CONFIG["validation_config"],
                auto_identify_config=APP_CONFIG["auto_identify_config"]
            )
            
            if geocoding_result.success:
                # Display results
                render_data_table(geocoding_result.merged_df, geocoding_result)
                generate_map(geocoding_result.merged_df.dropna(subset=["LATITUDE", "LONGITUDE"]))
                
                # Download option
                st.download_button(
                    label="Download the geocoded data as a CSV file",
                    data=geocoding_result.merged_df.to_csv(index=False).encode("utf-8"),
                    file_name=f"{name}__postal_geocoded.csv",
                    mime="text/csv",
                    help="Note that the postal codes that could not be geocoded are included in the download.",
                )
            else:
                st.error(geocoding_result.error_message)

if __name__ == "__main__":
    main()
