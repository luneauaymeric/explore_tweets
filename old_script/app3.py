import streamlit as st
import pandas as pd
import io

from datetime import datetime, timedelta


import psql_to_stream
import requests
from io import StringIO
import re
import os


@st.cache_resource
def init_connection():
    return st.connection("postgresql", type="sql")


def rebase_count():
    st.session_state.count = 0

if 'count' not in st.session_state:
    st.session_state.count = 0

### Front hand


st.session_state.bdd = "PSQL"

st.sidebar.divider()
st.sidebar.markdown("## Requête vers la base de données")
st.sidebar.info("Les variables ci-dessous permettent d'obtenir un tableau correspondant aux options choisies.", icon="ℹ️")
plateform = st.sidebar.selectbox("Choix de la plateforme", ("Twitch", "Twitter","Youtube", "Instagram"), index = None, on_change=rebase_count)

_conn = init_connection()

if plateform:
    

    #_conn.query(f'select * from information_schema.columns where table_name = "public.twitch_comment"', ttl="10m")
    rows = _conn.query(f'select * from information_schema.columns WHERE table_schema = \'public\' and table_name=\'twitch_comment\'', ttl="10m")
    print(rows.columns)
    list_columns = [x for x in rows.column_name]
    variable = st.sidebar.selectbox("Choix de la variable", list_columns, index = None)

code = f'''
SELECT * FROM public.twitch_comment WHERE {variable}
'''
st.code(code, language="sql")



