import re
import pandas as pd
import streamlit as st
from typing import Tuple, Optional

def read_file(file) -> pd.DataFrame:
    """Read a file into a pandas DataFrame supporting .csv, .xlsx, .parquet and their compressed versions."""
    filename = file.name.lower()
    
    if filename.endswith('.xlsx'):
        return pd.read_excel(file)
    elif filename.endswith('.parquet'):
        return pd.read_parquet(file)
    elif filename.endswith('.csv'):
        return pd.read_csv(file, encoding='latin1', on_bad_lines='skip')
    elif filename.endswith('.csv.zip'):
        return pd.read_csv(file, encoding='latin1', compression='zip', on_bad_lines='skip')
    elif filename.endswith('.csv.gz'):
        return pd.read_csv(file, encoding='latin1', compression='gzip', on_bad_lines='skip')
    elif filename.endswith('.csv.bz2'):
        return pd.read_csv(file, encoding='latin1', compression='bz2', on_bad_lines='skip')
    else:
        raise ValueError("Unsupported file type. Please upload .csv, .xlsx, .parquet or their compressed versions.")

def handle_file_upload(uploaded_files) -> Optional[Tuple[pd.DataFrame, str]]:
    try:
        if len(uploaded_files) > 1:
            user_df = pd.concat(
                [
                    read_file(uploaded_file).assign(FILENAME=uploaded_file.name)
                    for uploaded_file in uploaded_files
                ]
            ).reset_index(drop=True)
            filename_1 = re.sub(r"\..*$", "", uploaded_files[0].name)
            filename_2 = re.sub(r"\..*$", "", uploaded_files[1].name)
            name = f"{filename_1}--{filename_2}"
        else:
            user_df = read_file(uploaded_files[0])
            name = re.sub(r"\..*$", "", uploaded_files[0].name)
        
        return user_df, name
    except Exception as e:
        st.error(f"Error processing uploaded files: {str(e)}")
        return None 