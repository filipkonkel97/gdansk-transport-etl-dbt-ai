from airflow.sdk import dag, task
from datetime import datetime, timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig, LoadMode
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
import os
import pandas as pd

@dag(
    dag_id="gdansk_public_transport_stops_pipeline_w_dbt",
    description='Python + Snowflake + dbt pipeline',
    start_date=datetime(2024, 1, 1),
    schedule='0 20 * * 1',
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
        from etl_logic.data_transformations import transform_stops_data

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

    profile_config = ProfileConfig(
        profile_name = "snowflake_profile",
        target_name = "dev",
        profile_mapping = SnowflakeUserPasswordProfileMapping(
            conn_id = "snowflake_conn",
            profile_args = {
                "schema": "buses",
                "database": "buses"
            }
        )
    )
    
    stops_dbt_group = DbtTaskGroup(
        project_config = ProjectConfig(
            dbt_project_path = "/usr/local/airflow/dags/dbt"
        ),
        profile_config = profile_config,
        execution_config = ExecutionConfig(
            dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt"
        ),
        group_id = "dbt_stops_run",
        render_config = RenderConfig(
            selector="stops_only",
            load_method=LoadMode.DBT_LS,
        )
    )

    extracted = extract_stops_data()
    transformed = transform_stops_data(extracted)
    loaded = load_stops_data(transformed)

    extracted >> transformed >> loaded >> stops_dbt_group

stops_pipeline()