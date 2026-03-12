# Databricks notebook source
# DBTITLE 0,Silver Overview
# MAGIC %md
# MAGIC # Silver Layer — Cleaned, Typed, and Enriched
# MAGIC
# MAGIC The silver layer adds **structure and context** to the raw bronze data:
# MAGIC
# MAGIC | Transformation | Detail |
# MAGIC |---|---|
# MAGIC | **Join** | Sessions ← Meetings (adds `meeting_name`) |
# MAGIC | **Enrich** | Laps ← Drivers (adds `driver_name`, `team_name`) ← Sessions (adds `circuit`, `year`) |
# MAGIC | **Cast** | Sector durations → `DOUBLE`, timestamps → `TIMESTAMP` |
# MAGIC | **Derive** | `lap_time_seconds` (coalesce of lap duration and sum of sectors), `is_pit_lap` |
# MAGIC | **Filter** | Remove invalid laps (nulls, formation laps, red-flag periods) |
# MAGIC
# MAGIC **Output tables:** `silver_sessions`, `silver_laps`

# COMMAND ----------

# DBTITLE 0,Load Config
./00_Config

# COMMAND ----------

# DBTITLE 0,Silver Sessions Header
# MAGIC %md
# MAGIC ### silver_sessions
# MAGIC Join `bronze_sessions` with `bronze_meetings` to add the GP weekend name. Cast timestamps.

# COMMAND ----------

# DBTITLE 0,Silver Sessions
bronze_sessions = table("bronze_sessions")
bronze_meetings = table("bronze_meetings")

# Sessions already has country, circuit, year, location.
# From meetings we only pull meeting_name (the friendly GP name).
silver_sessions = (
    bronze_sessions
    .join(
        bronze_meetings.select("meeting_key", "meeting_name"),
        on="meeting_key",
        how="left"
    )
    .withColumn("date_start", F.to_timestamp("date_start"))
    .withColumn("date_end",   F.to_timestamp("date_end"))
)

silver_sessions.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{FULL_SCHEMA}.silver_sessions")

print(f"✓ silver_sessions: {silver_sessions.count():,} rows")
display(silver_sessions.select(
    "session_key", "session_type", "meeting_name",
    "circuit_short_name", "country_name", "year", "date_start"
).limit(5))

# COMMAND ----------

# DBTITLE 0,Silver Laps Header
# MAGIC %md
# MAGIC ### silver_laps
# MAGIC Enrich every lap with driver info and session context. Cast durations to numeric. Derive `lap_time_seconds` and `is_pit_lap`. Filter out invalid rows.

# COMMAND ----------

# DBTITLE 0,Silver Laps
bronze_laps    = table("bronze_laps")
bronze_drivers = table("bronze_drivers")
silver_sess    = table("silver_sessions")

# --- Deduplicate drivers (one record per session + driver_number) ---
drivers_deduped = (
    bronze_drivers
    .dropDuplicates(["session_key", "driver_number"])
    .select(
        "session_key", "driver_number",
        F.col("full_name").alias("driver_name"),
        F.col("name_acronym").alias("driver_code"),
        "team_name", "team_colour",
        F.col("country_code").alias("driver_country_code"),
    )
)

# --- Build silver_laps ---
silver_laps = (
    bronze_laps
    # Add driver info
    .join(drivers_deduped, on=["session_key", "driver_number"], how="left")
    # Add session context
    .join(
        silver_sess.select(
            "session_key", "session_type", "meeting_name",
            F.col("location").alias("meeting_location"),
            "country_name", "year",
            F.col("circuit_short_name").alias("circuit"),
        ),
        on="session_key",
        how="left",
    )
    # Cast durations
    .withColumn("duration_sector_1", F.col("duration_sector_1").cast("double"))
    .withColumn("duration_sector_2", F.col("duration_sector_2").cast("double"))
    .withColumn("duration_sector_3", F.col("duration_sector_3").cast("double"))
    .withColumn("lap_duration",      F.col("lap_duration").cast("double"))
    .withColumn("lap_number",        F.col("lap_number").cast("int"))
    # Derived columns
    .withColumn(
        "lap_time_seconds",
        F.coalesce(
            F.col("lap_duration"),
            F.col("duration_sector_1") + F.col("duration_sector_2") + F.col("duration_sector_3"),
        ),
    )
    .withColumn("is_pit_lap", F.col("is_pit_out_lap").cast("boolean").cast("int"))
)

# --- Filter out invalid laps ---
raw_count = silver_laps.count()
silver_laps_clean = silver_laps.filter(
    F.col("lap_time_seconds").isNotNull() & (F.col("lap_time_seconds") > 0)
)

silver_laps_clean.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{FULL_SCHEMA}.silver_laps")

print(f"✓ silver_laps: {silver_laps_clean.count():,} rows (filtered from {raw_count:,})")
display(
    silver_laps_clean.select(
        "meeting_name", "circuit", "driver_name", "team_name",
        "lap_number", "duration_sector_1", "duration_sector_2",
        "duration_sector_3", "lap_time_seconds", "is_pit_lap",
    ).limit(10)
)

# COMMAND ----------

# DBTITLE 0,Silver Summary
# MAGIC %md
# MAGIC ### Summary
# MAGIC
# MAGIC | Table | Description |
# MAGIC |---|---|
# MAGIC | `silver_sessions` | Sessions enriched with GP meeting name, timestamps cast |
# MAGIC | `silver_laps` | Laps enriched with driver/team, session context, derived timing columns, invalid laps removed |
# MAGIC
# MAGIC **Next:** Run `03_Gold_Aggregations` to build analytics-ready tables.
