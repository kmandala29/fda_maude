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

# DBTITLE 1,Cell 3
import sys, logging
import importlib
sys.path.insert(0, "/Workspace/Users/koushik.mandala@databricks.com/fda_maude")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

import maude_ingestion
importlib.reload(maude_ingestion)  # Reload to pick up the extractor.py fix
from maude_ingestion import MAUDEPipeline

# Override config with widget values
pipeline = MAUDEPipeline(config_path="/Workspace/Users/koushik.mandala@databricks.com/fda_maude/config.yaml", spark=spark)
pipeline.extractor.manufacturer = dbutils.widgets.get("manufacturer")
pipeline.extractor.date_from    = dbutils.widgets.get("date_from").replace("-", "")
pipeline.extractor.date_to      = dbutils.widgets.get("date_to").replace("-", "")

# COMMAND ----------

# DBTITLE 1,Run extraction
# Run extraction

summary = pipeline.run()

# COMMAND ----------

# DBTITLE 1,Display summary
# Summary

import pandas as pd

summary_display = {k: v for k, v in summary.items() if k != "volume_files"}
display(pd.DataFrame([summary_display]).T.rename(columns={0: "value"}))

print(f"\nRaw JSON files saved to: {summary['volume_path']}")
print(f"Bronze table: {summary['bronze_table']}")
print(f"Total records: {summary['total_records']:,}")
