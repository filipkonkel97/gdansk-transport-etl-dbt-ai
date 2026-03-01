from airflow.sdk import dag, task
from datetime import datetime, timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import os
import pandas as pd

@dag(
    dag_id="gdansk_public_transport_stops_pipeline_w_dbt",
    description='Python + Snowflake + dbt pipeline',
    start_date=datetime.utcnow() - timedelta(days=1),
    schedule='@daily',
    catchup=False,
    tags=['python', 'snowflake','dbt']
)

def stops_pipeline():
    @task
    def extract_stops_data():
        from etl_logic.api_client import APIClient
        from dotenv import load_dotenv

        load_dotenv()
        stops_api = os.getenv("stops_api")
        
        client = APIClient(stops_api)
        data = client.fetch_data()
        return data
    
    @task
    def transform_stops_data(data):
        from etl_logic.stops_data_transformation import transform_stops_data

        transformed_data = transform_stops_data(data)

        df = pd.DataFrame(transformed_data)

        df.columns = df.columns.str.upper()
    
        return df

    @task
    def load_stops_data(df):
        from etl_logic.snowflake_loader import SnowflakeLoader

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        db_conn = hook.get_sqlalchemy_engine()

        loader = SnowflakeLoader(db_conn)

        loader.load_data(df, schema='buses.bronze', table_name='bronze_stops')

    extracted = extract_stops_data()
    transformed = transform_stops_data(extracted)
    loaded = load_stops_data(transformed)

    extracted >> transformed >> loaded

stops_pipeline()