"""
End-to-end orchestration pipeline:
  1. Extract pages from openFDA API
  2. Save raw JSON to Databricks Volume
  3. Write flattened records to Bronze Delta table
"""
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

from .extractor import MAUDEExtractor
from .loader import VolumeLoader

logger = logging.getLogger(__name__)


class MAUDEPipeline:
    def __init__(self, config_path: str | Path, spark: SparkSession):
        with open(config_path) as f:
            self.cfg = yaml.safe_load(f)

        ext_cfg = self.cfg["extraction"]
        db_cfg  = self.cfg["databricks"]
        api_cfg = self.cfg["openfda"]

        self.extractor = MAUDEExtractor(
            manufacturer=ext_cfg["manufacturer"],
            date_from=ext_cfg["date_from"],
            date_to=ext_cfg["date_to"],
            page_limit=api_cfg.get("page_limit", 1000),
        )
        self.loader = VolumeLoader(
            spark=spark,
            catalog=db_cfg["catalog"],
            schema=db_cfg["schema"],
            volume=db_cfg["volume"],
            bronze_table=db_cfg["bronze_table"],
        )
        self.spark = spark

    def run(self) -> dict:
        """
        Execute full extraction run.
        Returns summary dict with total records, pages, volume path, and run ID.
        """
        run_id = f"bd_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        logger.info(f"Starting MAUDE extraction run: {run_id}")

        total_records = 0
        total_pages   = 0
        volume_files  = []

        for page_num, page_records in enumerate(self.extractor.extract(), start=1):
            # 1. Save raw JSON to Databricks Volume
            file_path = self.loader.save_json_to_volume(page_records, page_num, run_id)
            volume_files.append(file_path)

            # 2. Flatten and write to Bronze Delta
            self.loader.write_bronze(page_records)

            total_records += len(page_records)
            total_pages   = page_num

        summary = {
            "run_id":          run_id,
            "manufacturer":    self.extractor.manufacturer,
            "date_from":       self.extractor.date_from,
            "date_to":         self.extractor.date_to,
            "total_pages":     total_pages,
            "total_records":   total_records,
            "volume_path":     f"{self.loader.volume_path}/{run_id}",
            "volume_files":    volume_files,
            "bronze_table":    f"{self.loader.catalog}.{self.loader.schema}.{self.loader.bronze_table}",
            "completed_at":    datetime.now(timezone.utc).isoformat(),
        }

        logger.info(f"Run complete: {total_records:,} records across {total_pages} pages")
        return summary
