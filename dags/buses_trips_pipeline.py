from airflow.sdk import dag, task
from datetime import datetime, timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import os
import pandas as pd

@dag(
    dag_id="gdansk_public_transport_trips_pipeline_w_dbt",
    description='Python + Snowflake + dbt pipeline',
    start_date=datetime.utcnow() - timedelta(days=1),
    schedule='@daily',
    catchup=False,
    tags=['python', 'snowflake','dbt']
)

def trips_pipeline():
    @task
    def extract_buses_trips_data():
        from etl_logic.api_client import APIClient
        from dotenv import load_dotenv

        load_dotenv()
        trips_api = os.getenv("trips_api")
        
        client = APIClient(trips_api)
        data = client.fetch_data()
        return data
    
    @task
    def transform_buses_trips_data(data):
        from etl_logic.data_transformations import transform_trips_data

        transformed_data = transform_trips_data(data)

        df = pd.DataFrame(transformed_data)

        df.columns = df.columns.str.upper()
    
        return df

    @task
    def load_buses_trips_data(df):
        from etl_logic.snowflake_loader import SnowflakeLoader

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        db_conn = hook.get_sqlalchemy_engine()

        loader = SnowflakeLoader(db_conn)

        loader.load_data(df, schema='buses.bronze', table_name='bronze_trips')

    extracted = extract_buses_trips_data()
    transformed = transform_buses_trips_data(extracted)
    loaded = load_buses_trips_data(transformed)

    extracted >> transformed >> loaded

trips_pipeline()