import streamlit as st
import pandas as pd
import humanize

def render_data_table(merged_df: pd.DataFrame, geocoding_result) -> None:
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
        human_readable_time = humanize.naturaldelta(
            geocoding_result.process_time, minimum_unit="milliseconds"
        )
        fraction_merged = geocoding_result.matched_records / geocoding_result.total_records
        
        st.caption(
            f"Best column: `{geocoding_result.test_results.iloc[0]['COLUMN']}` • "
            f"{geocoding_result.total_records:,} records in {human_readable_time} • "
            f"{geocoding_result.matched_records:,} ({fraction_merged * 100:.1f}%) matched • "
            f"{len(non_merged)} ({len(non_merged) / len(merged_df) * 100:.1f}%) failed"
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
    else:
        st.caption(
            "The map below shows the location of the successfully geocoded postal codes. The address and postcode of each location are displayed when hovering over the points."
        ) 