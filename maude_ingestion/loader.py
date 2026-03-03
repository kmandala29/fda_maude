"""
Loads raw JSON pages into a Databricks Volume (as .json files)
and optionally writes flattened records to a Delta Bronze table.
"""
import json
import logging
import os
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class VolumeLoader:
    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        schema: str,
        volume: str,
        bronze_table: str,
    ):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self.bronze_table = bronze_table
        self.volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
        self._setup()

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    def _setup(self):
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        self.spark.sql(f"""
            CREATE VOLUME IF NOT EXISTS {self.catalog}.{self.schema}.{self.volume}
        """)
        logger.info(f"Volume ready: {self.volume_path}")

    # ------------------------------------------------------------------
    # Write raw JSON to Volume
    # ------------------------------------------------------------------

    def save_json_to_volume(self, records: list[dict], page_num: int, run_id: str) -> str:
        """
        Persist a page of raw records as a JSON file in the Databricks Volume.

        File path: /Volumes/{catalog}/{schema}/{volume}/{run_id}/page_{N:04d}.json
        Returns the file path written.
        """
        dir_path = f"{self.volume_path}/{run_id}"
        os.makedirs(dir_path, exist_ok=True)

        file_path = f"{dir_path}/page_{page_num:04d}.json"
        with open(file_path, "w") as f:
            json.dump(records, f, indent=2, default=str)

        logger.info(f"Saved {len(records)} records → {file_path}")
        return file_path

    # ------------------------------------------------------------------
    # Write flattened records to Bronze Delta table
    # ------------------------------------------------------------------

    def write_bronze(self, records: list[dict]) -> int:
        """
        Flatten and append raw records to the Bronze Delta table.
        Uses append mode — Bronze is an immutable raw landing zone.
        """
        if not records:
            return 0

        flat = [self._flatten(r) for r in records]
        df = self.spark.createDataFrame(flat)
        full_table = f"{self.catalog}.{self.schema}.{self.bronze_table}"

        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(full_table)
        logger.info(f"Appended {len(flat)} rows → {full_table}")
        return len(flat)

    # ------------------------------------------------------------------
    # Flatten nested openFDA event JSON
    # ------------------------------------------------------------------

    def _flatten(self, record: dict) -> dict:
        """
        Extract key scalar fields from the nested openFDA event record.
        Arrays (device, patient, mdr_text) are serialised as JSON strings
        for Bronze — Silver layer will explode them.
        """
        openfda = record.get("openfda") or {}
        device  = (record.get("device") or [{}])[0]
        patient = (record.get("patient") or [{}])[0]

        return {
            # Report identifiers
            "report_number":       record.get("report_number"),
            "mdr_report_key":      record.get("mdr_report_key"),
            "report_source_code":  record.get("report_source_code"),

            # Dates
            "date_received":       record.get("date_received"),
            "date_of_event":       record.get("date_of_event"),
            "date_report":         record.get("date_report"),

            # Event classification
            "event_type":          record.get("event_type"),
            "adverse_event_flag":  record.get("adverse_event_flag"),
            "product_problem_flag":record.get("product_problem_flag"),
            "single_use_flag":     record.get("single_use_flag"),

            # Manufacturer
            "manufacturer_g_name": record.get("manufacturer_g_name"),
            "manufacturer_name":   device.get("manufacturer_d_name"),
            "manufacturer_country":device.get("manufacturer_d_country"),

            # Device
            "brand_name":          device.get("brand_name"),
            "generic_name":        device.get("generic_name"),
            "model_number":        device.get("model_number"),
            "catalog_number":      device.get("catalog_number"),
            "lot_number":          device.get("lot_number"),
            "device_sequence_no":  device.get("device_sequence_no"),
            "device_event_key":    device.get("device_event_key"),
            "implant_flag":        device.get("implant_flag"),

            # Product classification (openFDA enrichment)
            "product_code":        json.dumps(openfda.get("device_class", [])),
            "device_class":        json.dumps(openfda.get("device_class", [])),
            "regulation_number":   json.dumps(openfda.get("regulation_number", [])),
            "medical_specialty":   json.dumps(openfda.get("medical_specialty_description", [])),

            # Patient outcome
            "patient_outcome":     json.dumps(patient.get("sequence_number_outcome", [])),
            "age_of_event":        patient.get("date_of_birth"),

            # Narratives (full arrays as JSON strings for Bronze)
            "mdr_text_raw":        json.dumps(record.get("mdr_text", [])),
            "device_raw":          json.dumps(record.get("device", [])),
            "patient_raw":         json.dumps(record.get("patient", [])),

            # Pipeline metadata
            "_ingested_at":        record.get("_ingested_at"),
            "_manufacturer_filter":record.get("_manufacturer_filter"),
            "_date_from":          record.get("_date_from"),
            "_date_to":            record.get("_date_to"),
        }
