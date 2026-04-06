import os

import pandas as pd


def extract_vehicles_psn_data():
    from dotenv import load_dotenv

    from .api_client import APIClient

    load_dotenv()
    vehicles_psn_api = os.getenv("vehicles_psn_api")

    client = APIClient(vehicles_psn_api)
    data = client.fetch_data()
    return data


def transform_vehicles_psn_data(data):
    from .psn_transformer import transform_vehicle_psn_data

    transformed_data = transform_vehicle_psn_data(data)

    df = pd.DataFrame(transformed_data)

    df.columns = df.columns.str.upper()

    return df
