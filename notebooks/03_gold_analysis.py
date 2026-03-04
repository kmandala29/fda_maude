# Databricks notebook source
# MAGIC %md
# MAGIC # FDA MAUDE — Gold Analysis: Becton Dickinson
# MAGIC
# MAGIC Business-ready aggregations for compliance and safety analysis:
# MAGIC - `bd_event_summary`       — event counts by type, year, month
# MAGIC - `bd_product_risk`        — adverse event rate by product/brand
# MAGIC - `bd_outcome_breakdown`   — patient outcome distribution
# MAGIC - `bd_narrative_flagged`   — reports containing high-risk keywords

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Users/koushik.mandala@databricks.com/fda_maude")

from pyspark.sql import functions as F

CATALOG = "bd_demo_kam"
SCHEMA  = "fda_maude"
GOLD    = "fda_maude_gold"   # same schema, prefixed tables

events     = spark.table(f"{CATALOG}.{SCHEMA}.maude_events_silver")
devices    = spark.table(f"{CATALOG}.{SCHEMA}.maude_devices_silver")
narratives = spark.table(f"{CATALOG}.{SCHEMA}.maude_narratives_silver")

# COMMAND ----------

# DBTITLE 1,Create Gold Schema
# Create Gold schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD}")
print(f"Schema {CATALOG}.{GOLD} ready")

# COMMAND ----------

 ### 1. Event Summary — volume by type, year, month

event_summary = (
    events
    .groupBy("year", "month", "event_type")
    .agg(
        F.count("report_number").alias("report_count"),
        F.sum(F.col("is_adverse_event").cast("int")).alias("adverse_event_count"),
        F.sum(F.col("is_product_problem").cast("int")).alias("product_problem_count"),
    )
    .orderBy("year", "month", "event_type")
)

(event_summary.write.format("delta").mode("overwrite")
    .saveAsTable(f"{CATALOG}.{GOLD}.bd_event_summary"))

display(event_summary)

# COMMAND ----------

 ### 2. Product Risk — adverse event rate by brand/generic name

product_risk = (
    events
    .groupBy("brand_name", "generic_name", "device_class")
    .agg(
        F.count("report_number").alias("total_reports"),
        F.sum(F.col("is_adverse_event").cast("int")).alias("adverse_events"),
        F.sum(F.col("is_product_problem").cast("int")).alias("product_problems"),
        F.countDistinct("report_number").alias("unique_reports"),
        F.min("date_received").alias("first_report_date"),
        F.max("date_received").alias("last_report_date"),
    )
    .withColumn("adverse_event_rate",
        F.round(F.col("adverse_events") / F.col("total_reports") * 100, 1))
    .withColumn("risk_tier",
        F.when(F.col("adverse_event_rate") >= 50, "High")
         .when(F.col("adverse_event_rate") >= 20, "Medium")
         .otherwise("Low"))
    .orderBy(F.desc("total_reports"))
)

(product_risk.write.format("delta").mode("overwrite")
    .saveAsTable(f"{CATALOG}.{GOLD}.bd_product_risk"))

display(product_risk.limit(20))

# COMMAND ----------

 ### 3. Outcome Breakdown — patient outcomes distribution

from pyspark.sql.functions import from_json, explode_outer
from pyspark.sql.types import ArrayType, StringType

outcome_map = {
    "1": "Death",
    "2": "Life Threatening",
    "3": "Hospitalization",
    "4": "Disability",
    "5": "Congenital Anomaly",
    "6": "Required Intervention",
    "7": "Other",
    "8": "No Information",
}

outcome_breakdown = (
    events
    .withColumn("outcome_array", from_json("patient_outcome", ArrayType(StringType())))
    .withColumn("outcome_code", explode_outer("outcome_array"))
    .withColumn("outcome_label",
        F.when(F.col("outcome_code") == "1", "Death")
         .when(F.col("outcome_code") == "2", "Life Threatening")
         .when(F.col("outcome_code") == "3", "Hospitalization")
         .when(F.col("outcome_code") == "4", "Disability")
         .when(F.col("outcome_code") == "5", "Congenital Anomaly")
         .when(F.col("outcome_code") == "6", "Required Intervention")
         .when(F.col("outcome_code") == "7", "Other")
         .otherwise("No Information"))
    .groupBy("year", "outcome_label")
    .agg(F.count("report_number").alias("report_count"))
    .orderBy("year", F.desc("report_count"))
)

(outcome_breakdown.write.format("delta").mode("overwrite")
    .saveAsTable(f"{CATALOG}.{GOLD}.bd_outcome_breakdown"))

display(outcome_breakdown)

# COMMAND ----------

# DBTITLE 1,Cell 6
 ### 4. Flagged Narratives — keyword scan for high-risk terms

HIGH_RISK_KEYWORDS = [
    "death", "died", "fatal", "deceased",
    "severe", "serious injury", "permanent",
    "infection", "sepsis", "contamination",
    "malfunction", "failure", "breakage", "leakage",
    "recall", "field correction",
]

keyword_pattern = "|".join(HIGH_RISK_KEYWORDS)

flagged_narratives = (
    narratives
    .join(events.select("report_number", 
                        F.col("date_received").alias("event_date_received"),
                        "brand_name", "generic_name", "event_type"),
          "report_number", "left")
    .withColumn("narrative_lower", F.lower("narrative_text"))
    .withColumn("matched_keywords",
        F.array_join(
            F.array(*[
                F.when(F.col("narrative_lower").contains(kw), F.lit(kw))
                 .otherwise(F.lit(None))
                for kw in HIGH_RISK_KEYWORDS
            ]),
            ", "
        )
    )
    .filter(F.col("narrative_lower").rlike(keyword_pattern))
    .select(
        "report_number",
        F.col("event_date_received").alias("date_received"),
        "brand_name",
        "generic_name",
        "event_type",
        "text_type_code",
        "narrative_text",
        "matched_keywords",
    )
    .orderBy(F.desc("date_received"))
)

(flagged_narratives.write.format("delta").mode("overwrite")
    .saveAsTable(f"{CATALOG}.{GOLD}.bd_narrative_flagged"))

print(f"Flagged narratives: {flagged_narratives.count():,}")
display(flagged_narratives.limit(20))

# COMMAND ----------

# DBTITLE 1,Cell 7
 ## Gold Tables Summary

for t in ["bd_event_summary", "bd_product_risk", "bd_outcome_breakdown", "bd_narrative_flagged"]:
    cnt = spark.table(f"{CATALOG}.{GOLD}.{t}").count()
    print(f"  {t}: {cnt:,} rows")
