import streamlit as st
from pathlib import Path
from kedro.framework.project import configure_project
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

class SessionManager:
    def __init__(self):
        self.initialize_kedro()
    
    def initialize_kedro(self):
        if "context" not in st.session_state:
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
            st.session_state["context"] = context
            st.session_state["catalog"] = context.catalog
            st.session_state["data"] = {}
    
    def load_data(self, filename):
        if "catalog" in st.session_state:
            catalog = st.session_state["catalog"]
            return catalog.load(filename)
        else:
            raise ValueError("Catalog not found in session state")
    
    def get_postcodes_masterlist(self):
        if "geocoded_postcodes_master_list" not in st.session_state["data"]:
            st.session_state["data"]["geocoded_postcodes_master_list"] = self.load_data("singapore_postcodes_masterlist")
        return st.session_state["data"]["geocoded_postcodes_master_list"]
    
    def get_geocoded_postcodes(self):
        if "geocoded_postcodes" not in st.session_state["data"]:
            st.session_state["data"]["geocoded_postcodes"] = self.load_data("singapore_postcodes_geocoded")
        return st.session_state["data"]["geocoded_postcodes"] 