import streamlit as st
import pandas as pd
import plotly.express as px
import pydeck as pdk

class PlotsRepository:
    
    @staticmethod
    def plot_bar(df, x, y, title):
        fig = px.bar(
            df,
            y=y,
            x=x,
            title=title
        )

        return fig
    
    @staticmethod
    def plot_map(df, tooltip, radius=80, lat=None, long=None):
        if lat is None:
            lat = df["lat"].mean()
        if long is None:
            long = df["lon"].mean()

        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df,
            get_position='[lon, lat]',
            get_radius=radius,
            get_fill_color='[255, 0, 0, 160]',
            pickable=True
        )

        view_state = pdk.ViewState(
            latitude=lat,
            longitude=long,
            zoom=11,
            pitch=0
        )

        r = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip=tooltip
        )

        return r