# Databricks notebook source
# DBTITLE 0,Config Overview
# MAGIC %md
# MAGIC # Shared Configuration & Utilities
# MAGIC
# MAGIC This notebook defines everything shared across the data engineering pipeline:
# MAGIC - **Constants** — catalog, schema, API base URL, seasons
# MAGIC - **`fetch_openf1()`** — fetch data from any OpenF1 endpoint with error handling
# MAGIC - **`write_bronze()`** — write raw API data to a Delta table
# MAGIC
# MAGIC Run this notebook first, or include it with `%run ./00_Config` from any sibling notebook.

# COMMAND ----------

# DBTITLE 0,Imports
import requests
import json
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 0,Constants
BASE_URL = "https://api.openf1.org/v1"
CATALOG  = "f1_genie_code"
SCHEMA   = "f1_workshop"
YEARS    = [2023, 2024, 2025]

FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA}")
print(f"Schema : {FULL_SCHEMA}")
print(f"API    : {BASE_URL}")
print(f"Seasons: {YEARS}")

# COMMAND ----------

# DBTITLE 0,fetch_openf1()
def fetch_openf1(endpoint: str, params: dict = None, verbose: bool = True) -> list:
    """
    Fetch JSON data from an OpenF1 API endpoint.

    Returns a list of dicts. Gracefully returns [] on HTTP 404
    (some sessions have no data for certain endpoints).
    """
    url  = f"{BASE_URL}/{endpoint}"
    resp = requests.get(url, params=params, timeout=30)

    if resp.status_code == 404:
        if verbose:
            print(f"  {endpoint} ({params}): 404 — skipped")
        return []

    resp.raise_for_status()
    data = resp.json()

    if verbose:
        print(f"  {endpoint} ({params}): {len(data)} records")
    return data

# COMMAND ----------

# DBTITLE 0,write_bronze()
def write_bronze(data: list, table_name: str) -> None:
    """
    Write a list of dicts to a bronze Delta table.

    Overwrites the table each time (full-refresh pattern).
    Prints row count and column count on success.
    """
    full_name = f"{FULL_SCHEMA}.{table_name}"

    if not data:
        print(f"  ⚠ No data for {full_name}, skipping.")
        return

    df = spark.createDataFrame(data)
    df.write.mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(full_name)

    count = spark.table(full_name).count()
    print(f"  ✓ {full_name}: {count:,} rows, {len(df.columns)} columns")

# COMMAND ----------

# DBTITLE 0,table()
def table(name: str):
    """Shorthand to read a table from our workshop schema."""
    return spark.table(f"{FULL_SCHEMA}.{name}")
