# Databricks notebook source
# MAGIC %md
# MAGIC # FDA MAUDE — Silver Transformation (Becton Dickinson)
# MAGIC
# MAGIC Reads from Bronze, cleans and explodes nested arrays, and produces structured Silver tables:
# MAGIC - `maude_events_silver`     — one row per MDR report
# MAGIC - `maude_devices_silver`    — one row per device within a report
# MAGIC - `maude_narratives_silver` — one row per narrative text block

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/fda_maude")

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, explode_outer

CATALOG = "main"
SCHEMA  = "fda_maude"

bronze = spark.table(f"{CATALOG}.{SCHEMA}.maude_events_bronze")
print(f"Bronze rows: {bronze.count():,}")

# COMMAND ----------
# MAGIC %md ### 1. Events Silver — one row per report

events_silver = (
    bronze
    .dropDuplicates(["report_number"])
    .select(
        "report_number",
        "mdr_report_key",
        "report_source_code",
        F.to_date(F.col("date_received"), "yyyyMMdd").alias("date_received"),
        F.to_date(F.col("date_of_event"), "yyyyMMdd").alias("date_of_event"),
        F.to_date(F.col("date_report"),   "yyyyMMdd").alias("date_report"),
        "event_type",
        "adverse_event_flag",
        "product_problem_flag",
        "single_use_flag",
        "manufacturer_g_name",
        "manufacturer_name",
        "manufacturer_country",
        "brand_name",
        "generic_name",
        "model_number",
        "catalog_number",
        "lot_number",
        "implant_flag",
        "medical_specialty",
        "device_class",
        "regulation_number",
        "patient_outcome",
        "_ingested_at",
    )
    .withColumn("year",  F.year("date_received"))
    .withColumn("month", F.month("date_received"))
    .withColumn("is_adverse_event",   F.col("adverse_event_flag")   == "Y")
    .withColumn("is_product_problem", F.col("product_problem_flag") == "Y")
    .withColumn("is_implant",         F.col("implant_flag")         == "Y")
)

(events_silver.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("year", "month")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.maude_events_silver"))

print(f"Events silver: {events_silver.count():,} rows")

# COMMAND ----------
# MAGIC %md ### 2. Devices Silver — explode device array

device_schema = ArrayType(StructType([
    StructField("brand_name",                   StringType()),
    StructField("generic_name",                 StringType()),
    StructField("manufacturer_d_name",          StringType()),
    StructField("model_number",                 StringType()),
    StructField("catalog_number",               StringType()),
    StructField("lot_number",                   StringType()),
    StructField("device_sequence_no",           StringType()),
    StructField("implant_flag",                 StringType()),
    StructField("device_availability",          StringType()),
    StructField("device_report_product_code",   StringType()),
]))

devices_silver = (
    bronze.select("report_number", "device_raw", "_ingested_at")
    .withColumn("device_array", from_json("device_raw", device_schema))
    .withColumn("device", explode_outer("device_array"))
    .select(
        "report_number",
        F.col("device.brand_name").alias("brand_name"),
        F.col("device.generic_name").alias("generic_name"),
        F.col("device.manufacturer_d_name").alias("manufacturer_name"),
        F.col("device.model_number").alias("model_number"),
        F.col("device.catalog_number").alias("catalog_number"),
        F.col("device.lot_number").alias("lot_number"),
        F.col("device.device_sequence_no").alias("device_sequence_no"),
        F.col("device.implant_flag").alias("implant_flag"),
        F.col("device.device_report_product_code").alias("product_code"),
        "_ingested_at",
    )
    .dropDuplicates(["report_number", "device_sequence_no"])
)

(devices_silver.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.maude_devices_silver"))

print(f"Devices silver: {devices_silver.count():,} rows")

# COMMAND ----------
# MAGIC %md ### 3. Narratives Silver — explode mdr_text array

narrative_schema = ArrayType(StructType([
    StructField("text_type_code", StringType()),
    StructField("foi_text",       StringType()),
    StructField("mdr_text_key",   StringType()),
]))

narratives_silver = (
    bronze.select("report_number", "mdr_text_raw", "date_received", "_ingested_at")
    .withColumn("text_array", from_json("mdr_text_raw", narrative_schema))
    .withColumn("text_block", explode_outer("text_array"))
    .select(
        "report_number",
        F.to_date(F.col("date_received"), "yyyyMMdd").alias("date_received"),
        F.col("text_block.text_type_code").alias("text_type_code"),
        F.col("text_block.foi_text").alias("narrative_text"),
        F.col("text_block.mdr_text_key").alias("mdr_text_key"),
        "_ingested_at",
    )
    .filter(F.col("narrative_text").isNotNull())
)

(narratives_silver.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.maude_narratives_silver"))

print(f"Narratives silver: {narratives_silver.count():,} rows")

# COMMAND ----------
# MAGIC %md ## Silver Tables Summary

for t in ["maude_events_silver", "maude_devices_silver", "maude_narratives_silver"]:
    cnt = spark.table(f"{CATALOG}.{SCHEMA}.{t}").count()
    print(f"  {t}: {cnt:,} rows")
