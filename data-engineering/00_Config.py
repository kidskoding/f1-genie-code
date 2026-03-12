# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# DBTITLE 0,Config Overview
# MAGIC %md
# MAGIC # Shared Configuration & Utilities
# MAGIC
# MAGIC All shared constants and helper functions live in **`config.py`** (same directory).  
# MAGIC Other notebooks import it as a standard Python module:
# MAGIC
# MAGIC ```python
# MAGIC from config import *
# MAGIC ```
# MAGIC
# MAGIC ### What's in `config.py`
# MAGIC | Symbol | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `BASE_URL` | `str` | OpenF1 API base URL |
# MAGIC | `CATALOG`, `SCHEMA`, `FULL_SCHEMA` | `str` | Unity Catalog target |
# MAGIC | `YEARS` | `list` | Seasons to ingest |
# MAGIC | `fetch_openf1()` | function | Fetch JSON from any OpenF1 endpoint (handles 404s) |
# MAGIC | `write_bronze()` | function | Write a list of dicts to a bronze Delta table |
# MAGIC | `table()` | function | Shorthand to read a table from the workshop schema |
# MAGIC | `init_schema()` | function | Create the target schema if it doesn't exist |

# COMMAND ----------

# DBTITLE 0,Imports
from config import *

init_schema()

# COMMAND ----------

# DBTITLE 0,Constants
# Quick smoke test
data = fetch_openf1("meetings", {"year": 2024})
print(f"fetch_openf1 works: {len(data)} meetings")
print(f"table() works: {table('bronze_meetings').count()} rows in bronze_meetings")
