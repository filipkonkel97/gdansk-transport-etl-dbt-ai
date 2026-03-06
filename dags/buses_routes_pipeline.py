from airflow.sdk import dag, task
from datetime import datetime, timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import os
import pandas as pd

@dag(
    dag_id="gdansk_public_transport_routes_pipeline_w_dbt",
    description='Python + Snowflake + dbt pipeline',
    start_date=datetime.utcnow() - timedelta(days=1),
    schedule='@daily',
    catchup=False,
    tags=['python', 'snowflake','dbt']
)

def routes_pipeline():
    @task
    def extract_routes_data():
        from etl_logic.api_client import APIClient
        from dotenv import load_dotenv

        load_dotenv()
        routes_api = os.getenv("routes_api")
        
        client = APIClient(routes_api)
        data = client.fetch_data()
        return data
    
    @task
    def transform_routes_data(data):
        from etl_logic.data_transformations import transform_routes_data

        transformed_data = transform_routes_data(data)

        df = pd.json_normalize(transformed_data, 'routes')

        df.columns = df.columns.str.upper()
    
        return df

    @task
    def load_routes_data(df):
        from etl_logic.snowflake_loader import SnowflakeLoader

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        db_conn = hook.get_sqlalchemy_engine()

        loader = SnowflakeLoader(db_conn)

        loader.load_data(df, schema='buses.bronze', table_name='bronze_routes')

    extracted = extract_routes_data()
    transformed = transform_routes_data(extracted)
    loaded = load_routes_data(transformed)

    extracted >> transformed >> loaded

routes_pipeline()