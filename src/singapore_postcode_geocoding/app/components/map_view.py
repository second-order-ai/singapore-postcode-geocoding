import streamlit as st
import pydeck as pdk
from pydeck.data_utils.viewport_helpers import compute_view
import pandas as pd

def generate_map(plot_df: pd.DataFrame) -> None:
    # Get first 5 column names
    first_five = plot_df.columns[:5].tolist()
    
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
        tooltip={
            "html": f"""
                <b>Input data:</b><br/>
                {first_five[0]}: {{{first_five[0]}}}<br/>
                {first_five[1]}: {{{first_five[1]}}}<br/>
                {first_five[2]}: {{{first_five[2]}}}<br/>
                {first_five[3]}: {{{first_five[3]}}}<br/>
                {first_five[4]}: {{{first_five[4]}}}<br/>
                <b>Matched postal Info:</b><br/>
                Postal: {{POSTAL}}<br/>
                Address: {{ADDRESS}}
            """,
            "style": {
                "backgroundColor": "white",
                "color": "black",
                "padding": "10px",
                "border-radius": "5px"
            }
        },
        map_style=None,
    )

    st.pydeck_chart(deck) 