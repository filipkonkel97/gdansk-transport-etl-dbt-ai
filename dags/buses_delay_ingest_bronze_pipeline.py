from airflow.sdk import dag, task
from datetime import datetime, timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig, LoadMode
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
import os
import pandas as pd

@dag(
    dag_id="gdansk_public_transport_delays_bronze_ingestion_pipeline",
    description='Python + Snowflake',
    start_date=datetime(2024, 1, 1),
    schedule='1/3 * * * *',
    catchup=False,
    tags=['python', 'snowflake','bronze', 'delays']
)

def delays_pipeline():
    @task
    def extract_buses_delays_data():
        from etl_logic.api_client import APIClient
        from dotenv import load_dotenv

        load_dotenv()
        delays_api = os.getenv("delays_api")

        client = APIClient(delays_api)
        data = client.fetch_data()
        return data

    @task
    def transform_buses_delays_data(data):
        from etl_logic.data_transformations import transform_delays_data

        transformed_data = transform_delays_data(data)

        df = pd.DataFrame(transformed_data)

        df.columns = df.columns.str.upper()
    
        return df

    @task
    def load_buses_delays_data(df):
        from etl_logic.snowflake_loader import SnowflakeLoader

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        db_conn = hook.get_sqlalchemy_engine()

        loader = SnowflakeLoader(db_conn)

        loader.load_data(df, schema='buses.bronze', table_name='bronze_delays')

    extracted = extract_buses_delays_data()
    transformed = transform_buses_delays_data(extracted)
    loaded = load_buses_delays_data(transformed)

    extracted >> transformed >> loaded

delays_pipeline()