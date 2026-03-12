# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# DBTITLE 1,Workshop Introduction
# MAGIC %md
# MAGIC # Formula 1 Analytics Workshop — From API to AI with Databricks
# MAGIC
# MAGIC This hands-on workshop walks through a complete data pipeline using **live Formula 1 data** from the [OpenF1 API](https://openf1.org). You'll move through three modules that mirror real-world data team roles:
# MAGIC
# MAGIC | Module | Role | What You'll Build |
# MAGIC |---|---|---|
# MAGIC | **1 — Data Engineering** | Data Engineer | Ingest F1 API → Bronze → Silver → Gold (Medallion Architecture) |
# MAGIC | **2 — Data Science** | Data Scientist | EDA, feature engineering, train a podium-prediction model with MLflow |
# MAGIC | **3 — Data Analytics** | Data Analyst | SQL insights, Dashboards, and a Genie space for self-service Q&A |
# MAGIC
# MAGIC **Data Pipeline:**
# MAGIC
# MAGIC ```
# MAGIC OpenF1 API  →  Bronze (raw JSON)  →  Silver (cleaned, joined)  →  Gold (aggregated KPIs)  →  ML Model  →  Dashboard / Genie
# MAGIC ```
# MAGIC
# MAGIC **Prerequisites:** A Databricks workspace with Unity Catalog enabled. No API keys needed — OpenF1 is free and open.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Module 1 Header
# MAGIC %md
# MAGIC # Module 1 — Data Engineering
# MAGIC ### From API to Medallion Architecture
# MAGIC
# MAGIC In this module you'll:
# MAGIC 1. **Ingest** raw F1 data from 6 API endpoints into **Bronze** Delta tables
# MAGIC 2. **Transform** and enrich the data into **Silver** tables (joins, type casting, derived columns)
# MAGIC 3. **Aggregate** into **Gold** tables with driver stats, circuit characteristics, and constructor performance
# MAGIC
# MAGIC **Target schema:** `f1_genie_code.f1_workshop`  
# MAGIC **Data range:** 2023–2025 seasons (\~24 races per year)

# COMMAND ----------

# DBTITLE 1,Setup — Imports and Schema Creation
import requests
import json
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ---------- Configuration ----------
BASE_URL = "https://api.openf1.org/v1"
CATALOG = "f1_genie_code"
SCHEMA = "f1_workshop"
YEARS = [2023, 2024, 2025]

# Create schema (catalog already exists)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Helper: fetch data from OpenF1
def fetch_openf1(endpoint, params=None, verbose=True):
    """Fetch data from an OpenF1 endpoint. Returns a list of dicts.
    Gracefully returns [] on 404 (missing data for that session)."""
    url = f"{BASE_URL}/{endpoint}"
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

# Helper: write a list of dicts as a Delta table
def write_bronze(data, table_name):
    """Write raw API data to a bronze Delta table."""
    full_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    if not data:
        print(f"  \u26a0 No data for {full_name}, skipping.")
        return
    df = spark.createDataFrame(data)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)
    count = spark.table(full_name).count()
    print(f"  \u2713 {full_name}: {count} rows, {len(df.columns)} columns")

print(f"Schema ready: {CATALOG}.{SCHEMA}")
print(f"API base URL: {BASE_URL}")
print(f"Seasons: {YEARS}")

# COMMAND ----------

# DBTITLE 1,Bronze Layer Intro
# MAGIC %md
# MAGIC ## Bronze Layer — Raw API Ingestion
# MAGIC
# MAGIC The bronze layer captures **raw data exactly as it comes from the API**. No transformations, no cleaning — just a faithful snapshot stored in Delta format. We ingest 6 endpoints:
# MAGIC
# MAGIC | Endpoint | What it contains | Grain |
# MAGIC |---|---|---|
# MAGIC | `meetings` | Grand Prix weekends (name, location, country) | 1 row per GP weekend |
# MAGIC | `sessions` | Individual sessions (Practice, Qualifying, Race) | 1 row per session |
# MAGIC | `drivers` | Driver info per session (name, team, number) | 1 row per driver per session |
# MAGIC | `laps` | Lap-by-lap timing with sector splits | 1 row per driver per lap |
# MAGIC | `pit` | Pit stop events with durations | 1 row per pit stop |
# MAGIC | `weather` | Track conditions sampled throughout sessions | 1 row per weather reading |

# COMMAND ----------

# DBTITLE 1,Bronze — Ingest Meetings
# ── Ingest Meetings ─────────────────────────────────────────
# One API call per year. Contains GP name, location, country.

all_meetings = []
for year in YEARS:
    data = fetch_openf1("meetings", {"year": year})
    for d in data:
        d["year"] = year
    all_meetings.extend(data)

print(f"\nTotal meetings across {YEARS}: {len(all_meetings)}")
write_bronze(all_meetings, "bronze_meetings")

display(spark.table(f"{CATALOG}.{SCHEMA}.bronze_meetings").limit(5))

# COMMAND ----------

# DBTITLE 1,Bronze — Ingest Sessions
# ── Ingest Sessions ─────────────────────────────────────────
# All session types: Practice 1-3, Sprint, Qualifying, Race.

all_sessions = []
for year in YEARS:
    data = fetch_openf1("sessions", {"year": year})
    all_sessions.extend(data)

print(f"\nTotal sessions across {YEARS}: {len(all_sessions)}")
write_bronze(all_sessions, "bronze_sessions")

display(
    spark.table(f"{CATALOG}.{SCHEMA}.bronze_sessions")
    .groupBy("session_type")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# DBTITLE 1,Bronze — Ingest Drivers
# ── Ingest Drivers ──────────────────────────────────────────
# Pull driver roster for each Race session.
# This captures team changes and mid-season swaps.

race_sessions = spark.table(f"{CATALOG}.{SCHEMA}.bronze_sessions") \
    .filter(F.col("session_type") == "Race") \
    .select("session_key") \
    .collect()

race_session_keys = [row.session_key for row in race_sessions]
print(f"Race sessions to ingest drivers for: {len(race_session_keys)}")

all_drivers = []
for sk in race_session_keys:
    data = fetch_openf1("drivers", {"session_key": sk}, verbose=False)
    all_drivers.extend(data)

print(f"\nTotal driver-session records: {len(all_drivers)}")
write_bronze(all_drivers, "bronze_drivers")

display(
    spark.table(f"{CATALOG}.{SCHEMA}.bronze_drivers")
    .select("full_name", "team_name", "driver_number", "country_code")
    .dropDuplicates(["full_name"])
    .orderBy("team_name")
)

# COMMAND ----------

# DBTITLE 1,Bronze — Ingest Laps
# ── Ingest Laps ────────────────────────────────────────────
# Largest dataset — every lap for every driver in every race.
# Contains sector times (duration_sector_1/2/3), lap duration, pit out info.

print(f"Ingesting laps for {len(race_session_keys)} race sessions...")

all_laps = []
for i, sk in enumerate(race_session_keys):
    data = fetch_openf1("laps", {"session_key": sk}, verbose=False)
    all_laps.extend(data)
    if (i + 1) % 20 == 0:
        print(f"  Progress: {i + 1}/{len(race_session_keys)} sessions ({len(all_laps)} laps so far)")

print(f"\nTotal lap records: {len(all_laps)}")
write_bronze(all_laps, "bronze_laps")

display(spark.table(f"{CATALOG}.{SCHEMA}.bronze_laps").limit(5))

# COMMAND ----------

# DBTITLE 1,Bronze — Ingest Pit Stops
# ── Ingest Pit Stops ───────────────────────────────────────
# Pit stop events: duration in pit lane, stop duration, lap number.

print(f"Ingesting pit stops for {len(race_session_keys)} race sessions...")

all_pits = []
for sk in race_session_keys:
    data = fetch_openf1("pit", {"session_key": sk}, verbose=False)
    all_pits.extend(data)

print(f"\nTotal pit stop records: {len(all_pits)}")
write_bronze(all_pits, "bronze_pit_stops")

display(spark.table(f"{CATALOG}.{SCHEMA}.bronze_pit_stops").limit(5))

# COMMAND ----------

# DBTITLE 1,Bronze — Ingest Weather
# ── Ingest Weather ─────────────────────────────────────────
# Track conditions: air/track temperature, humidity, wind, rainfall.

print(f"Ingesting weather for {len(race_session_keys)} race sessions...")

all_weather = []
for sk in race_session_keys:
    data = fetch_openf1("weather", {"session_key": sk}, verbose=False)
    all_weather.extend(data)

print(f"\nTotal weather records: {len(all_weather)}")
write_bronze(all_weather, "bronze_weather")

display(spark.table(f"{CATALOG}.{SCHEMA}.bronze_weather").limit(5))

# COMMAND ----------

# DBTITLE 1,Silver Layer Intro
# MAGIC %md
# MAGIC ## Silver Layer — Cleaned, Typed, and Enriched
# MAGIC
# MAGIC The silver layer adds **structure and context**:
# MAGIC - Join sessions with meetings to get circuit name, country, and location
# MAGIC - Enrich lap data with driver name and team
# MAGIC - Cast string durations to numeric seconds
# MAGIC - Derive new columns: `lap_time_seconds`, `is_pit_lap`
# MAGIC - Remove invalid/null lap times (formation laps, red flags)
# MAGIC
# MAGIC Output tables: `silver_sessions`, `silver_laps`

# COMMAND ----------

# DBTITLE 1,Silver — Transform Sessions and Laps
# ============================================================
# SILVER LAYER — Transform & Enrich
# ============================================================

# ---------- silver_sessions: sessions + meeting context ----------
bronze_sessions = spark.table(f"{CATALOG}.{SCHEMA}.bronze_sessions")
bronze_meetings = spark.table(f"{CATALOG}.{SCHEMA}.bronze_meetings")

# Sessions already has country_name, country_code, year, location, circuit_short_name.
# From meetings we only need meeting_name (unique to that table).
silver_sessions = bronze_sessions.join(
    bronze_meetings.select(
        F.col("meeting_key"),
        F.col("meeting_name")
    ),
    on="meeting_key",
    how="left"
).withColumn("date_start", F.to_timestamp("date_start")) \
 .withColumn("date_end", F.to_timestamp("date_end"))

silver_sessions.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_sessions")
print(f"\u2713 silver_sessions: {silver_sessions.count()} rows")

# ---------- silver_laps: laps + driver/team + session context ----------
bronze_laps = spark.table(f"{CATALOG}.{SCHEMA}.bronze_laps")
bronze_drivers = spark.table(f"{CATALOG}.{SCHEMA}.bronze_drivers")

drivers_deduped = bronze_drivers.dropDuplicates(["session_key", "driver_number"]) \
    .select(
        "session_key", "driver_number",
        F.col("full_name").alias("driver_name"),
        F.col("name_acronym").alias("driver_code"),
        "team_name", "team_colour",
        F.col("country_code").alias("driver_country_code")
    )

silver_laps = bronze_laps \
    .join(drivers_deduped, on=["session_key", "driver_number"], how="left") \
    .join(
        spark.table(f"{CATALOG}.{SCHEMA}.silver_sessions").select(
            "session_key", "session_type", "meeting_name",
            F.col("location").alias("meeting_location"),
            "country_name", "year",
            F.col("circuit_short_name").alias("circuit")
        ),
        on="session_key",
        how="left"
    ) \
    .withColumn("duration_sector_1", F.col("duration_sector_1").cast("double")) \
    .withColumn("duration_sector_2", F.col("duration_sector_2").cast("double")) \
    .withColumn("duration_sector_3", F.col("duration_sector_3").cast("double")) \
    .withColumn("lap_duration", F.col("lap_duration").cast("double")) \
    .withColumn(
        "lap_time_seconds",
        F.coalesce(
            F.col("lap_duration"),
            F.col("duration_sector_1") + F.col("duration_sector_2") + F.col("duration_sector_3")
        )
    ) \
    .withColumn("is_pit_lap", F.col("is_pit_out_lap").cast("boolean").cast("int")) \
    .withColumn("lap_number", F.col("lap_number").cast("int"))

# Filter out invalid laps
silver_laps_clean = silver_laps.filter(
    (F.col("lap_time_seconds").isNotNull()) & (F.col("lap_time_seconds") > 0)
)

silver_laps_clean.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_laps")

print(f"\u2713 silver_laps: {silver_laps_clean.count()} rows (filtered from {silver_laps.count()})")
display(silver_laps_clean.select(
    "meeting_name", "circuit", "driver_name", "team_name",
    "lap_number", "duration_sector_1", "duration_sector_2", "duration_sector_3",
    "lap_time_seconds", "is_pit_lap"
).limit(10))

# COMMAND ----------

# DBTITLE 1,Gold Layer Intro
# MAGIC %md
# MAGIC ## Gold Layer — Aggregated Business-Ready Tables
# MAGIC
# MAGIC Three analytics-ready tables that power dashboards, ML, and Genie:
# MAGIC
# MAGIC | Table | Grain | Key Metrics |
# MAGIC |---|---|---|
# MAGIC | `gold_driver_stats` | Driver × Year | Races, avg lap time, best lap, total laps, sector averages |
# MAGIC | `gold_circuit_stats` | Circuit × Year | Avg lap time, lap time spread, weather conditions |
# MAGIC | `gold_constructor_performance` | Team × Year | Avg pit duration, driver consistency, total laps completed |

# COMMAND ----------

# DBTITLE 1,Gold — Aggregate Driver, Circuit, and Constructor Stats
# ============================================================
# GOLD LAYER — Aggregated Analytics Tables
# ============================================================

silver_laps = spark.table(f"{CATALOG}.{SCHEMA}.silver_laps")
bronze_pit_stops = spark.table(f"{CATALOG}.{SCHEMA}.bronze_pit_stops")

# ---------- gold_driver_stats ----------
gold_driver_stats = silver_laps \
    .filter(F.col("session_type") == "Race") \
    .groupBy("driver_name", "driver_code", "team_name", "year") \
    .agg(
        F.countDistinct("session_key").alias("races"),
        F.count("lap_number").alias("total_laps"),
        F.round(F.avg("lap_time_seconds"), 3).alias("avg_lap_time_sec"),
        F.round(F.min("lap_time_seconds"), 3).alias("best_lap_time_sec"),
        F.round(F.stddev("lap_time_seconds"), 3).alias("lap_time_stddev"),
        F.round(F.avg("duration_sector_1"), 3).alias("avg_sector_1"),
        F.round(F.avg("duration_sector_2"), 3).alias("avg_sector_2"),
        F.round(F.avg("duration_sector_3"), 3).alias("avg_sector_3"),
        F.sum(F.col("is_pit_lap").cast("int")).alias("total_pit_laps")
    ) \
    .orderBy("year", "avg_lap_time_sec")

gold_driver_stats.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_driver_stats")
print(f"✓ gold_driver_stats: {gold_driver_stats.count()} rows")

# ---------- gold_circuit_stats ----------
bronze_weather = spark.table(f"{CATALOG}.{SCHEMA}.bronze_weather")
silver_sessions_df = spark.table(f"{CATALOG}.{SCHEMA}.silver_sessions")

weather_agg = bronze_weather.groupBy("session_key").agg(
    F.round(F.avg("air_temperature"), 1).alias("avg_air_temp"),
    F.round(F.avg("track_temperature"), 1).alias("avg_track_temp"),
    F.round(F.avg("humidity"), 1).alias("avg_humidity"),
    F.round(F.avg("wind_speed"), 1).alias("avg_wind_speed"),
    F.max(F.when(F.col("rainfall") == True, 1).otherwise(0)).alias("had_rain")
)

gold_circuit_stats = silver_laps \
    .filter(F.col("session_type") == "Race") \
    .groupBy("circuit", "meeting_name", "country_name", "year", "session_key") \
    .agg(
        F.round(F.avg("lap_time_seconds"), 3).alias("avg_lap_time_sec"),
        F.round(F.min("lap_time_seconds"), 3).alias("fastest_lap_sec"),
        F.round(F.stddev("lap_time_seconds"), 3).alias("lap_time_spread"),
        F.countDistinct("driver_number").alias("drivers_count"),
        F.count("*").alias("total_laps")
    ) \
    .join(weather_agg, on="session_key", how="left") \
    .drop("session_key")

gold_circuit_stats.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_circuit_stats")
print(f"✓ gold_circuit_stats: {gold_circuit_stats.count()} rows")

# ---------- gold_constructor_performance ----------
bronze_drivers_deduped = spark.table(f"{CATALOG}.{SCHEMA}.bronze_drivers") \
    .dropDuplicates(["session_key", "driver_number"]) \
    .select("session_key", "driver_number", "team_name")

pit_with_team = bronze_pit_stops \
    .join(bronze_drivers_deduped, on=["session_key", "driver_number"], how="left") \
    .join(silver_sessions_df.select("session_key", "year"), on="session_key", how="left")

gold_constructor = pit_with_team.groupBy("team_name", "year").agg(
    F.count("*").alias("total_pit_stops"),
    F.round(F.avg("pit_duration"), 3).alias("avg_pit_duration_sec"),
    F.round(F.min("pit_duration"), 3).alias("fastest_pit_stop_sec"),
    F.round(F.stddev("pit_duration"), 3).alias("pit_stop_consistency"),
    F.countDistinct("driver_number").alias("drivers_fielded")
)

team_lap_stats = silver_laps \
    .filter(F.col("session_type") == "Race") \
    .groupBy("team_name", "year") \
    .agg(
        F.count("*").alias("team_total_laps"),
        F.round(F.avg("lap_time_seconds"), 3).alias("team_avg_lap_time"),
        F.round(F.stddev("lap_time_seconds"), 3).alias("team_lap_consistency")
    )

gold_constructor_perf = gold_constructor.join(
    team_lap_stats, on=["team_name", "year"], how="left"
).orderBy("year", "avg_pit_duration_sec")

gold_constructor_perf.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_constructor_performance")
print(f"✓ gold_constructor_performance: {gold_constructor_perf.count()} rows")

print("\n── Gold layer complete! ──")
display(spark.table(f"{CATALOG}.{SCHEMA}.gold_driver_stats").filter(F.col("year") == 2024).limit(10))
