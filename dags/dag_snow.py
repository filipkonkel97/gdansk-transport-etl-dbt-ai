from airflow.sdk import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bootstrap", "snowflake"],
)
def bootstrap_snowflake():

    @task
    def create_database():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        hook.run("""
        CREATE DATABASE IF NOT EXISTS buses;
        """
    )

    @task
    def create_schema():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        hook.run("""
        CREATE SCHEMA IF NOT EXISTS buses.bronze;
        """
    )

    @task
    def create_trips_table():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        hook.run("""
        CREATE OR REPLACE TABLE buses.bronze.bronze_trips (
                        ID VARCHAR(20),
                        ROUTEID NUMBER(38,0),
                        TRIPID NUMBER(38,0),
                        TRIPHEADSIGN VARCHAR(200),
                        TRIPSHORTNAME VARCHAR(50),
                        DIRECTIONID NUMBER(38,0),
                        ACTIVATIONDATE DATE,
                        TYPE VARCHAR(20)
                        );
        """
    )
        
    @task
    def create_delays_table():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        hook.run("""
        CREATE OR REPLACE TABLE buses.bronze.bronze_delays (
                        ID VARCHAR(20),
                        TRIP NUMBER(38,0),
                        STATUS VARCHAR(20),
                        STOPID VARCHAR(20),
                        TRIPID NUMBER(38,0),
                        ROUTEID NUMBER(38,0),
                        HEADSIGN VARCHAR(200),
                        TIMESTAMP VARCHAR(50),
                        VEHICLEID NUMBER(38,0),
                        VEHICLECODE NUMBER(38,0),
                        ESTIMATEDTIME VARCHAR(50),
                        DELAYINSECONDS NUMBER(38,0),
                        ROUTESHORTNAME VARCHAR(50),
                        VEHICLESERVICE VARCHAR(50),
                        THEORETICALTIME VARCHAR(50),
                        SCHEDULEDTRIPSTARTTIME VARCHAR(50)
                        );
        """
    )

    create_database() >> create_schema() >> [create_trips_table(), create_delays_table()] 

bootstrap_snowflake()