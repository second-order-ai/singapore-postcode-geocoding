import streamlit as st
import pandas as pd

def render_sidebar():
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
    
    # Example file download
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
        file_name="GreenMarkBuildings__test_file.csv",
        mime="text/csv",
    ) 