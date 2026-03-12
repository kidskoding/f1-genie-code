# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "1"
# ///
# DBTITLE 0,Bronze Overview
# MAGIC %md
# MAGIC # Bronze Layer — Raw API Ingestion
# MAGIC
# MAGIC The bronze layer captures **raw data exactly as it comes from the API**.  
# MAGIC No transformations, no cleaning — just a faithful snapshot in Delta format.
# MAGIC
# MAGIC | Endpoint | Description | Grain |
# MAGIC |---|---|---|
# MAGIC | `meetings` | Grand Prix weekends | 1 row per GP weekend |
# MAGIC | `sessions` | Practice, Qualifying, Race sessions | 1 row per session |
# MAGIC | `drivers` | Driver roster per race session | 1 row per driver per session |
# MAGIC | `laps` | Lap-by-lap timing with sector splits | 1 row per driver per lap |
# MAGIC | `pit` | Pit stop events with durations | 1 row per pit stop |
# MAGIC | `weather` | Track conditions throughout sessions | 1 row per weather reading |

# COMMAND ----------

# DBTITLE 0,Load Config
from config import *

init_schema()

# COMMAND ----------

# DBTITLE 0,Meetings & Sessions
# MAGIC %md
# MAGIC ### Meetings & Sessions
# MAGIC One API call per year. Meetings are GP weekends; sessions are the individual track events within each weekend.

# COMMAND ----------

# DBTITLE 0,Ingest Meetings
all_meetings = []
for year in YEARS:
    data = fetch_openf1("meetings", {"year": year})
    for d in data:
        d["year"] = year
    all_meetings.extend(data)

print(f"Total meetings: {len(all_meetings)}")
write_bronze(all_meetings, "bronze_meetings")
display(table("bronze_meetings").limit(5))

# COMMAND ----------

# DBTITLE 0,Ingest Sessions
all_sessions = []
for year in YEARS:
    data = fetch_openf1("sessions", {"year": year})
    all_sessions.extend(data)

print(f"Total sessions: {len(all_sessions)}")
write_bronze(all_sessions, "bronze_sessions")

display(
    table("bronze_sessions")
    .groupBy("session_type").count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# DBTITLE 0,Get Race Session Keys
# All subsequent endpoints are fetched per Race session
race_session_keys = [
    row.session_key
    for row in table("bronze_sessions")
        .filter(F.col("session_type") == "Race")
        .select("session_key")
        .collect()
]
print(f"Race sessions: {len(race_session_keys)}")

# COMMAND ----------

# DBTITLE 0,Per-Race Endpoints
# MAGIC %md
# MAGIC ### Per-Race Endpoints
# MAGIC Drivers, laps, pit stops, and weather are pulled **per race session**. The `fetch_openf1` helper silently skips any 404s (future or cancelled sessions).

# COMMAND ----------

# DBTITLE 0,Ingest Drivers
all_drivers = []
for sk in race_session_keys:
    all_drivers.extend(fetch_openf1("drivers", {"session_key": sk}, verbose=False))

print(f"Total driver-session records: {len(all_drivers)}")
write_bronze(all_drivers, "bronze_drivers")

display(
    table("bronze_drivers")
    .select("full_name", "team_name", "driver_number", "country_code")
    .dropDuplicates(["full_name"])
    .orderBy("team_name")
)

# COMMAND ----------

# DBTITLE 0,Ingest Laps
all_laps = []
for i, sk in enumerate(race_session_keys):
    all_laps.extend(fetch_openf1("laps", {"session_key": sk}, verbose=False))
    if (i + 1) % 20 == 0:
        print(f"  Progress: {i+1}/{len(race_session_keys)} sessions ({len(all_laps):,} laps)")

print(f"Total laps: {len(all_laps):,}")
write_bronze(all_laps, "bronze_laps")
display(table("bronze_laps").limit(5))

# COMMAND ----------

# DBTITLE 0,Ingest Pit Stops
all_pits = []
for sk in race_session_keys:
    all_pits.extend(fetch_openf1("pit", {"session_key": sk}, verbose=False))

print(f"Total pit stops: {len(all_pits)}")
write_bronze(all_pits, "bronze_pit_stops")
display(table("bronze_pit_stops").limit(5))

# COMMAND ----------

# DBTITLE 0,Ingest Weather
all_weather = []
for sk in race_session_keys:
    all_weather.extend(fetch_openf1("weather", {"session_key": sk}, verbose=False))

print(f"Total weather records: {len(all_weather):,}")
write_bronze(all_weather, "bronze_weather")
display(table("bronze_weather").limit(5))

# COMMAND ----------

# DBTITLE 0,Bronze Summary
# MAGIC %md
# MAGIC ### Summary
# MAGIC
# MAGIC Bronze ingestion complete. Six tables written to `f1_genie_code.f1_workshop`:
# MAGIC
# MAGIC | Table | Source |
# MAGIC |---|---|
# MAGIC | `bronze_meetings` | Grand Prix weekends |
# MAGIC | `bronze_sessions` | All session types |
# MAGIC | `bronze_drivers` | Driver rosters per race |
# MAGIC | `bronze_laps` | Lap-level timing data |
# MAGIC | `bronze_pit_stops` | Pit stop events |
# MAGIC | `bronze_weather` | Track conditions |
# MAGIC
# MAGIC **Next:** Run `02_Silver_Transforms` to clean, type, and enrich this data.
