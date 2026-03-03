# Databricks notebook source
# MAGIC %md
# MAGIC # FDA MAUDE — Extract Becton Dickinson Reports → Volume + Bronze
# MAGIC
# MAGIC Pulls adverse event reports for **Becton Dickinson** from the openFDA API
# MAGIC and stores them in two places:
# MAGIC - **Raw JSON files** → `/Volumes/main/fda_maude/raw_json/{run_id}/page_NNNN.json`
# MAGIC - **Bronze Delta table** → `main.fda_maude.maude_events_bronze`

# COMMAND ----------

dbutils.widgets.text("date_from",    "2023-01-01")
dbutils.widgets.text("date_to",      "2025-12-31")
dbutils.widgets.text("manufacturer", "becton dickinson")

# COMMAND ----------

import sys, logging
sys.path.insert(0, "/Workspace/Repos/fda_maude")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

from maude_ingestion import MAUDEPipeline

# Override config with widget values
pipeline = MAUDEPipeline(config_path="/Workspace/Repos/fda_maude/config.yaml", spark=spark)
pipeline.extractor.manufacturer = dbutils.widgets.get("manufacturer")
pipeline.extractor.date_from    = dbutils.widgets.get("date_from").replace("-", "")
pipeline.extractor.date_to      = dbutils.widgets.get("date_to").replace("-", "")

# COMMAND ----------
# MAGIC %md ## Dry run — count total records first

total = pipeline.extractor.count_total()
print(f"Total records to extract: {total:,}")

# COMMAND ----------
# MAGIC %md ## Run extraction

summary = pipeline.run()

# COMMAND ----------
# MAGIC %md ## Summary

import pandas as pd

summary_display = {k: v for k, v in summary.items() if k != "volume_files"}
display(pd.DataFrame([summary_display]).T.rename(columns={0: "value"}))

print(f"\nRaw JSON files saved to: {summary['volume_path']}")
print(f"Bronze table: {summary['bronze_table']}")
print(f"Total records: {summary['total_records']:,}")
