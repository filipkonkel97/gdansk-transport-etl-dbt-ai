import pandas as pd
from .connection import get_engine

class SnowflakeDB:
    def __init__(self):
        self.engine = get_engine()

    def query(self, sql: str):
        with self.engine.connect() as conn:
            return pd.read_sql(sql, conn)