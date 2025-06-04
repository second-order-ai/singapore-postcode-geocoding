from dataclasses import dataclass
from typing import Optional
import pandas as pd
import time
import humanize
import streamlit as st
from singapore_postcode_geocoding.pipelines.postcode_identification.auto_identification_classes_pipeline.nodes import (
    auto_convert_postcodes,
)

@dataclass
class GeocodingResult:
    success: bool
    merged_df: Optional[pd.DataFrame] = None
    error_message: Optional[str] = None
    test_results: Optional[pd.DataFrame] = None
    process_time: Optional[float] = None
    total_records: Optional[int] = None
    matched_records: Optional[int] = None

def process_geocoding(user_df, postcode_masterlist, postcode, validation_config, auto_identify_config):
    start_time = time.perf_counter()
    
    with st.spinner('Geocoding your data... This may take a few moments.'):
        converted_df, success, test_results = auto_convert_postcodes(
            df=user_df,
            validation_config=validation_config,
            master_postcodes=postcode_masterlist,
            auto_identify_config=auto_identify_config
        )
    
    process_time = time.perf_counter() - start_time
    total_records = len(user_df)
    
    if success:
        # Use the converted DataFrame directly
        user_df = converted_df
        
        # Merge the uploaded file with the internal dataset
        merged_df = user_df.merge(
            postcode.drop_duplicates("FORMATTED_POSTCODE"),
            left_on="FORMATTED_POSTCODE",
            right_on="FORMATTED_POSTCODE",
            how="left",
            suffixes=("", "_GEOCODED_DATASET"),
            validate="m:1",
        )
        
        matched_records = merged_df["ADDRESS"].notna().sum()
        
        return GeocodingResult(
            success=True,
            merged_df=merged_df,
            test_results=test_results,
            process_time=process_time,
            total_records=total_records,
            matched_records=matched_records
        )
    else:
        return GeocodingResult(
            success=False,
            error_message=f"❌ No suitable postcode column found. Best predicted success rate: {test_results.iloc[0]['CONVERSION_SUCCESS_RATE'] if not test_results.empty else 0:.1f}%",
            test_results=test_results,
            process_time=process_time,
            total_records=total_records
        ) 