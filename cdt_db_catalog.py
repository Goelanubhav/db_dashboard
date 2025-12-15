# app.py
import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Fetch database credentials
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
HOST = os.getenv("DB_HOST")
PORT = os.getenv("DB_PORT")
DBNAME = os.getenv("DB_NAME")

st.set_page_config(page_title="Cadents DB Catalog", layout="wide")
st.title("Cadents DB Catalog")
st.caption("Browse tables/views and column metadata (including descriptions)")

def get_engine():
    if not all([USER, PASSWORD, HOST, PORT, DBNAME]):
        st.error("Database environment variables are not fully set.")
        st.stop()
    url = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"
    return create_engine(url, pool_pre_ping=True)

# --- Queries ---
TABLES_SQL = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = :schema
  AND table_type IN ('BASE TABLE', 'VIEW')
ORDER BY table_name;
"""

# Catalog query including BOTH table + column descriptions
CATALOG_SQL = """
SELECT
    c.table_schema,
    c.table_name,

    -- Table description (objsubid = 0)
    td.description AS table_description,

    c.column_name,
    c.data_type,
    c.is_nullable,

    -- Column description (objsubid = ordinal_position)
    cd.description AS column_description

FROM information_schema.columns c
LEFT JOIN pg_catalog.pg_statio_all_tables st
  ON c.table_schema = st.schemaname AND c.table_name = st.relname

LEFT JOIN pg_catalog.pg_description td
  ON td.objoid = st.relid AND td.objsubid = 0

LEFT JOIN pg_catalog.pg_description cd
  ON cd.objoid = st.relid AND cd.objsubid = c.ordinal_position

WHERE c.table_schema = :schema
  AND (:all_tables = TRUE OR c.table_name = ANY(:tables))
ORDER BY c.table_name, c.ordinal_position;
"""

@st.cache_data(ttl=300)
def fetch_tables(schema: str) -> list[str]:
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text(TABLES_SQL), conn, params={"schema": schema})
    return df["table_name"].tolist()

@st.cache_data(ttl=300)
def fetch_catalog(schema: str, tables: list[str]) -> pd.DataFrame:
    engine = get_engine()
    all_tables = (len(tables) == 0)
    with engine.connect() as conn:
        df = pd.read_sql(
            text(CATALOG_SQL),
            conn,
            params={
                "schema": schema,
                "tables": tables,
                "all_tables": all_tables,
            },
        )
    df["table_description"] = df["table_description"].fillna("")
    df["column_description"] = df["column_description"].fillna("")
    return df

# --- Sidebar filters ---
st.sidebar.header("Filters")

schema_choices = ["org", "src", "stg"]
selected_schema = st.sidebar.selectbox("Schema", options=schema_choices, index=0)

tables_in_schema = fetch_tables(selected_schema)

table_options = ["All"] + tables_in_schema

selected_table = st.sidebar.selectbox(
    "Table / View",
    options=table_options,
    index=0,
    help="Select a single table or choose All",
)

# Convert to list for query compatibility
selected_tables = [] if selected_table == "All" else [selected_table]


text_filter = st.sidebar.text_input(
    "Text filter (table / column / description contains)",
    value="",
    help="Case-insensitive substring match",
)

# --- Load data ---
df = fetch_catalog(selected_schema, selected_tables)

if text_filter.strip():
    f = text_filter.strip().lower()
    df = df[
        df["table_name"].str.lower().str.contains(f, na=False)
        | df["column_name"].str.lower().str.contains(f, na=False)
        | df["table_description"].str.lower().str.contains(f, na=False)
        | df["column_description"].str.lower().str.contains(f, na=False)
    ].copy()

# --- Metrics ---
col1, col2, col3 = st.columns(3)
col1.metric("Schema", selected_schema)
col2.metric("Tables / Views", df[["table_name"]].drop_duplicates().shape[0])
col3.metric("Columns", df.shape[0])

st.divider()

if df.empty:
    st.info("No results match the current filters.")
    st.stop()

# --- Table description (single-table sidebar driven) ---
st.subheader("Table Description")

if selected_table == "All":
    st.info("Select a table in the left sidebar to view its description.")
else:
    table_for_desc = selected_table

    desc_series = (
        df.loc[df["table_name"] == table_for_desc, "table_description"]
          .dropna()
          .astype(str)
          .drop_duplicates()
    )

    desc_val = desc_series.iloc[0] if not desc_series.empty else "(No table description found)"

    st.markdown(f"#### {table_for_desc}")
    st.text_area(
        label="",
        value=desc_val,
        height=120,
        disabled=True,
    )

# --- Column Catalog ---
st.subheader("Column Catalog")
st.dataframe(
    df[[
        #"table_schema",
        #"table_name",
        #"table_description",
        "column_name",
        "data_type",
        #"is_nullable",
        "column_description",
    ]],
    use_container_width=True,
    hide_index=True,
    column_config={
        #"table_schema": st.column_config.TextColumn("Schema", width="small"),
        #"table_name": st.column_config.TextColumn("Table Name", width="medium"),
        #"table_description": st.column_config.TextColumn("Table Description", width="large"),
        "column_name": st.column_config.TextColumn("Column Name", width="medium"),
        "data_type": st.column_config.TextColumn("Data Type", width="small"),
        #"is_nullable": st.column_config.TextColumn("Nullable", width="small"),
        "column_description": st.column_config.TextColumn("Column Description", width="large"),
    },
)
