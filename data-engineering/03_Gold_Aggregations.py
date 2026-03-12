# Databricks notebook source
# DBTITLE 0,Gold Overview
# MAGIC %md
# MAGIC # Gold Layer — Aggregated Business-Ready Tables
# MAGIC
# MAGIC Three analytics-ready tables that power dashboards, ML models, and Genie spaces:
# MAGIC
# MAGIC | Table | Grain | Key Metrics |
# MAGIC |---|---|---|
# MAGIC | `gold_driver_stats` | Driver × Year | Races, avg/best lap time, sector averages, consistency |
# MAGIC | `gold_circuit_stats` | Circuit × Year | Avg/fastest lap, lap time spread, weather conditions |
# MAGIC | `gold_constructor_performance` | Team × Year | Pit stop efficiency, team pace, lap consistency |

# COMMAND ----------

# DBTITLE 0,Load Config
from config import *

# COMMAND ----------

# DBTITLE 0,Load Silver Tables
silver_laps     = table("silver_laps")
silver_sessions = table("silver_sessions")
bronze_pits     = table("bronze_pit_stops")
bronze_weather  = table("bronze_weather")
bronze_drivers  = table("bronze_drivers")

race_laps = silver_laps.filter(F.col("session_type") == "Race")
print(f"Race laps: {race_laps.count():,}")

# COMMAND ----------

# DBTITLE 0,Driver Stats Header
# MAGIC %md
# MAGIC ### gold_driver_stats
# MAGIC Season-level driver performance: races entered, total laps completed, average and best lap times, sector-by-sector averages, and consistency (stddev).

# COMMAND ----------

# DBTITLE 0,Gold — Driver Stats
gold_driver_stats = (
    race_laps
    .groupBy("driver_name", "driver_code", "team_name", "year")
    .agg(
        F.countDistinct("session_key").alias("races"),
        F.count("lap_number").alias("total_laps"),
        F.round(F.avg("lap_time_seconds"), 3).alias("avg_lap_time_sec"),
        F.round(F.min("lap_time_seconds"), 3).alias("best_lap_time_sec"),
        F.round(F.stddev("lap_time_seconds"), 3).alias("lap_time_stddev"),
        F.round(F.avg("duration_sector_1"), 3).alias("avg_sector_1"),
        F.round(F.avg("duration_sector_2"), 3).alias("avg_sector_2"),
        F.round(F.avg("duration_sector_3"), 3).alias("avg_sector_3"),
        F.sum(F.col("is_pit_lap").cast("int")).alias("total_pit_laps"),
    )
    .orderBy("year", "avg_lap_time_sec")
)

gold_driver_stats.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{FULL_SCHEMA}.gold_driver_stats")

print(f"✓ gold_driver_stats: {gold_driver_stats.count()} rows")
display(gold_driver_stats.filter(F.col("year") == 2024).limit(10))

# COMMAND ----------

# DBTITLE 0,Circuit Stats Header
# MAGIC %md
# MAGIC ### gold_circuit_stats
# MAGIC Circuit-level stats per year: average and fastest laps, lap time spread, plus race-day weather (temperature, humidity, wind, rain).

# COMMAND ----------

# DBTITLE 0,Gold — Circuit Stats
# Aggregate weather per session
weather_agg = (
    bronze_weather
    .groupBy("session_key")
    .agg(
        F.round(F.avg("air_temperature"), 1).alias("avg_air_temp"),
        F.round(F.avg("track_temperature"), 1).alias("avg_track_temp"),
        F.round(F.avg("humidity"), 1).alias("avg_humidity"),
        F.round(F.avg("wind_speed"), 1).alias("avg_wind_speed"),
        F.max(F.when(F.col("rainfall") == 1, 1).otherwise(0)).alias("had_rain"),
    )
)

gold_circuit_stats = (
    race_laps
    .groupBy("circuit", "meeting_name", "country_name", "year", "session_key")
    .agg(
        F.round(F.avg("lap_time_seconds"), 3).alias("avg_lap_time_sec"),
        F.round(F.min("lap_time_seconds"), 3).alias("fastest_lap_sec"),
        F.round(F.stddev("lap_time_seconds"), 3).alias("lap_time_spread"),
        F.countDistinct("driver_number").alias("drivers_count"),
        F.count("*").alias("total_laps"),
    )
    .join(weather_agg, on="session_key", how="left")
    .drop("session_key")
)

gold_circuit_stats.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{FULL_SCHEMA}.gold_circuit_stats")

print(f"✓ gold_circuit_stats: {gold_circuit_stats.count()} rows")
display(gold_circuit_stats.filter(F.col("year") == 2024).limit(10))

# COMMAND ----------

# DBTITLE 0,Constructor Header
# MAGIC %md
# MAGIC ### gold_constructor_performance
# MAGIC Team-level performance per year: pit stop speed and consistency, overall race pace, and lap-to-lap reliability.

# COMMAND ----------

# DBTITLE 0,Gold — Constructor Performance
# Pit stop metrics by team
drivers_deduped = (
    bronze_drivers
    .dropDuplicates(["session_key", "driver_number"])
    .select("session_key", "driver_number", "team_name")
)

pit_with_team = (
    bronze_pits
    .join(drivers_deduped, on=["session_key", "driver_number"], how="left")
    .join(silver_sessions.select("session_key", "year"), on="session_key", how="left")
)

gold_pits = pit_with_team.groupBy("team_name", "year").agg(
    F.count("*").alias("total_pit_stops"),
    F.round(F.avg("pit_duration"), 3).alias("avg_pit_duration_sec"),
    F.round(F.min("pit_duration"), 3).alias("fastest_pit_stop_sec"),
    F.round(F.stddev("pit_duration"), 3).alias("pit_stop_consistency"),
    F.countDistinct("driver_number").alias("drivers_fielded"),
)

# Team lap metrics
team_laps = race_laps.groupBy("team_name", "year").agg(
    F.count("*").alias("team_total_laps"),
    F.round(F.avg("lap_time_seconds"), 3).alias("team_avg_lap_time"),
    F.round(F.stddev("lap_time_seconds"), 3).alias("team_lap_consistency"),
)

gold_constructor = (
    gold_pits
    .join(team_laps, on=["team_name", "year"], how="left")
    .orderBy("year", "avg_pit_duration_sec")
)

gold_constructor.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{FULL_SCHEMA}.gold_constructor_performance")

print(f"✓ gold_constructor_performance: {gold_constructor.count()} rows")
display(gold_constructor.filter(F.col("year") == 2024))

# COMMAND ----------

# DBTITLE 0,Gold Summary
# MAGIC %md
# MAGIC ### Summary
# MAGIC
# MAGIC Gold layer complete. Three tables ready for downstream consumption:
# MAGIC
# MAGIC | Table | Rows | Use Case |
# MAGIC |---|---|---|
# MAGIC | `gold_driver_stats` | \~70 | ML features, driver comparison dashboards |
# MAGIC | `gold_circuit_stats` | \~88 | Circuit analysis, weather impact studies |
# MAGIC | `gold_constructor_performance` | \~30 | Team strategy analysis, pit stop benchmarking |
# MAGIC
# MAGIC **Next:** Module 2 (Data Science) or Module 3 (Data Analytics).
