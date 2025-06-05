import re
import pandas as pd
import streamlit as st
from typing import Tuple, Optional

def read_file(file) -> pd.DataFrame:
    """Read a file into a pandas DataFrame using pd.read_table with latin1 encoding."""
    return pd.read_table(file, encoding='latin1')

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