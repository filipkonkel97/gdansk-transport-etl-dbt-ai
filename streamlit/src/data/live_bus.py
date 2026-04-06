import pandas as pd
from src.api_calls.psn_fetcher import (
    extract_vehicles_psn_data,
    transform_vehicles_psn_data,
)


class LiveBusData:
    def __init__(
        self,
        fetch_func=extract_vehicles_psn_data,
        transform_func=transform_vehicles_psn_data,
    ):
        self.fetch_func = fetch_func
        self.transform_func = transform_func

    def get_data(self):
        data = self.fetch_func()
        df = self.transform_func(data)
        df["LAT"] = pd.to_numeric(df["LAT"], errors="coerce")
        df["LON"] = pd.to_numeric(df["LON"], errors="coerce")
        return df.dropna(subset=["LAT", "LON"])
