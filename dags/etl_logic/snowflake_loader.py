import pandas as pd
from sqlalchemy import create_engine

class SnowflakeLoader:
    def __init__(self, engine):
        self.engine = engine

    def load_data(self, df: pd.DataFrame, table_name: str, schema: str = 'bronze', if_exists: str = 'append'):
        """Loads the transformed data into a Snowflake database.
        args:
            None
        returns:
            None
        """
        try:
            df.to_sql(
                table_name, 
                con=self.engine, 
                schema=schema, 
                if_exists=if_exists, 
                index=False
                )

            print("Data loaded successfully into the database.")
        
        except Exception as e:
            raise Exception(f"❌Error loading data into the database: {e}") from e