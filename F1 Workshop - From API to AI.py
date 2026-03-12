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
# MAGIC ```
# MAGIC OpenF1 API  →  Bronze (raw JSON)  →  Silver (cleaned, joined)  →  Gold (aggregated KPIs)  →  ML Model  →  Dashboard / Genie
# MAGIC ```
# MAGIC
# MAGIC **Prerequisites:** A Databricks workspace with Unity Catalog enabled. No API keys needed — OpenF1 is free and open.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Module 1 — Data Engineering
# MAGIC
# MAGIC **Folder:** `data-engineering/`
# MAGIC
# MAGIC Run these notebooks in order:
# MAGIC
# MAGIC | # | Notebook | Description |
# MAGIC |---|---|---|
# MAGIC | 0 | `00_Config` | Shared constants, imports, and helper functions |
# MAGIC | 1 | `01_Bronze_Ingestion` | Ingest 6 API endpoints into raw Delta tables |
# MAGIC | 2 | `02_Silver_Transforms` | Clean, type-cast, join, and enrich into silver tables |
# MAGIC | 3 | `03_Gold_Aggregations` | Aggregate into driver stats, circuit stats, and constructor performance |
# MAGIC
# MAGIC **Target schema:** `f1_genie_code.f1_workshop` &nbsp;•&nbsp; **Seasons:** 2023–2025
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Module 2 — Data Science
# MAGIC *Coming soon*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Module 3 — Data Analytics
# MAGIC *Coming soon*
