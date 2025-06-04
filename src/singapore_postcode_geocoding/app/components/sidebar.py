import streamlit as st
import pandas as pd

def render_sidebar():
    st.sidebar.title("How to use the app")
    st.sidebar.markdown(
        """
        **Step-by-step guide:**
        1. **Upload your data file(s):** Upload CSV, Excel (.xlsx), Parquet, or compressed files (.zip, .gzip, .bz2). Multiple files with the same structure can be uploaded at once.
        2. **Automatic postcode detection:** The app automatically finds and extracts postal code information from your data.
        3. **View results:** Explore geocoded results in a table and on an interactive map, including both successful and failed geocoding attempts.
        4. **Download results:** Download the full geocoded dataset (including failed matches) as a CSV file.
        5. **Repeat as needed:** Clear the upload box to process new files.
        """,
        #help="The app scans your data for Singapore postal codes (6 digits) and extracts them automatically. It can find postal codes even if they're part of a full address."
    )
    # st.sidebar.caption(
    #     """
    #     **How it works:**
    #     - The app automatically detects and validates Singapore postal codes, even if they are embedded in address fields.
    #     - Geocoding is performed using a master dataset of Singapore postcodes, returning latitude, longitude, and address details.
    #     - All results, including reasons for failed geocoding, are available for review and download.
    #     """
    # )
    
    # Example files section
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Example files:**", help="To download a file: Right-click the link and select 'Save link as...' or use your browser's 'Save as...' option when viewing the file. Then upload the file to the app to see how it works.")
    
    # Example files
    example_files = [
        {
            "name": "GreenMarkBuildings.csv",
            "url": "https://raw.githubusercontent.com/second-order-ai/singapore-postcode-geocoding/main/data/01_raw/test/GreenMarkBuildings.csv"
        },
        {
            "name": "ListofGovernmentMarketsHawkerCentres.csv",
            "url": "https://raw.githubusercontent.com/second-order-ai/singapore-postcode-geocoding/main/data/01_raw/test/ListofGovernmentMarketsHawkerCentres.csv"
        },
        {
            "name": "MCSTinformation.xlsx",
            "url": "https://raw.githubusercontent.com/second-order-ai/singapore-postcode-geocoding/main/data/01_raw/test/MCSTinformation.xlsx"
        },
    ]
    
    for file in example_files:
        st.sidebar.markdown(f"[⬇️ {file['name']}]({file['url']})")

    st.sidebar.markdown("[See more example files on GitHub](https://github.com/second-order-ai/singapore-postcode-geocoding/tree/main/data/01_raw/test)") 