import streamlit as st
from sqlalchemy import create_engine
from urllib.parse import quote_plus

@st.cache_resource
def get_engine():
    return create_engine(
        f"snowflake://{st.secrets['user']}:{quote_plus(st.secrets['password'])}@"
        f"{st.secrets['account']}/{st.secrets['database']}/{st.secrets['schema']}"
        f"?warehouse={st.secrets['warehouse']}"
    )