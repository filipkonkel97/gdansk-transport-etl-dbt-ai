import pydeck as pdk


class MapPlotter:
    def __init__(self, center_lat=54.45, center_lon=18.55, zoom=11):
        self.center_lat = center_lat
        self.center_lon = center_lon
        self.zoom = zoom

    def plot_map(self, df, tooltip=None, radius=60):
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df,
            get_position="[LON, LAT]",
            get_radius=radius,
            get_fill_color=[255, 0, 0],
            pickable=True,
        )

        view_state = pdk.ViewState(
            latitude=self.center_lat, longitude=self.center_lon, zoom=self.zoom, pitch=0
        )

        return pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip)
