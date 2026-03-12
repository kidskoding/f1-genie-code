"""
Shared configuration and utilities for the F1 Workshop data engineering pipeline.

Usage (from any sibling notebook):
    from config import *
"""

import requests
import json
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession

# ── Constants ────────────────────────────────────────────────
BASE_URL    = "https://api.openf1.org/v1"
CATALOG     = "f1_genie_code"
SCHEMA      = "f1_workshop"
YEARS       = [2023, 2024, 2025]
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"


def _spark():
    """Get the active SparkSession."""
    return SparkSession.getActiveSession()


# ── API Helper ───────────────────────────────────────────────
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


# ── Delta Write Helper ───────────────────────────────────────
def write_bronze(data: list, table_name: str) -> None:
    """
    Write a list of dicts to a bronze Delta table.

    Overwrites the table each time (full-refresh pattern).
    Prints row count and column count on success.
    """
    spark = _spark()
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


# ── Table Reader ─────────────────────────────────────────────
def table(name: str):
    """Shorthand to read a table from our workshop schema."""
    return _spark().table(f"{FULL_SCHEMA}.{name}")


# ── Schema Init ──────────────────────────────────────────────
def init_schema():
    """Create the target schema if it doesn't exist."""
    _spark().sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA}")
    print(f"Schema : {FULL_SCHEMA}")
    print(f"API    : {BASE_URL}")
    print(f"Seasons: {YEARS}")
