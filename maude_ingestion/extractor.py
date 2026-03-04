"""
openFDA MAUDE extractor — paginates through device adverse event reports
for a given manufacturer and date range.

API docs: https://open.fda.gov/apis/device/event/
"""
import logging
import os
from datetime import datetime
from typing import Iterator

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

OPENFDA_URL = "https://api.fda.gov/device/event.json"


class MAUDEExtractor:
    def __init__(
        self,
        manufacturer: str,
        date_from: str,
        date_to: str,
        page_limit: int = 1000,
        api_key: str | None = None,
    ):
        """
        Args:
            manufacturer: Manufacturer name as stored in openFDA (e.g. "becton dickinson")
            date_from:    Start date YYYY-MM-DD
            date_to:      End date   YYYY-MM-DD
            page_limit:   Records per API call (max 1000)
            api_key:      Optional openFDA API key for higher rate limits
        """
        self.manufacturer = manufacturer
        self.date_from = date_from.replace("-", "")   # openFDA uses YYYYMMDD
        self.date_to   = date_to.replace("-", "")
        self.page_limit = min(page_limit, 1000)
        self.api_key = api_key or os.environ.get("OPENFDA_API_KEY")
        self.session = requests.Session()

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def count_total(self) -> int:
        """Return total number of matching records before extraction."""
        resp = self._get(skip=0, limit=1)
        total = resp.get("meta", {}).get("results", {}).get("total", 0)
        logger.info(f"Total records for '{self.manufacturer}' ({self.date_from}–{self.date_to}): {total:,}")
        return total

    def extract(self) -> Iterator[list[dict]]:
        """
        Yield pages of raw event records (dicts) from the openFDA API.
        Each record is the full JSON object as returned by the API.
        """
        total = self.count_total()
        skip = 0
        page = 0

        while skip < total:
            data = self._get(skip=skip, limit=self.page_limit)
            records = data.get("results", [])
            if not records:
                break

            # Stamp extraction metadata onto each record
            ingested_at = datetime.utcnow().isoformat() + "Z"
            for r in records:
                r["_ingested_at"]  = ingested_at
                r["_manufacturer_filter"] = self.manufacturer
                r["_date_from"]    = self.date_from
                r["_date_to"]      = self.date_to

            page += 1
            fetched = skip + len(records)
            logger.info(f"Page {page}: fetched {len(records)} records ({fetched:,}/{total:,})")
            yield records

            skip += self.page_limit

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _build_search(self) -> str:
        # openFDA search syntax — manufacturer name + date range
        # Use device.manufacturer_d_name (not manufacturer_g_name) for device-level manufacturer
        # Note: Use spaces around AND/TO operators, + only within terms
        manufacturer_q = f'device.manufacturer_d_name:{self.manufacturer.replace(" ", "+")}'
        date_q = f"date_received:[{self.date_from} TO {self.date_to}]"
        return f"{manufacturer_q} AND {date_q}"

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=3, max=60),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True,
    )
    def _get(self, skip: int, limit: int) -> dict:
        params: dict = {
            "search": self._build_search(),
            "limit":  limit,
            "skip":   skip,
        }
        if self.api_key:
            params["api_key"] = self.api_key

        resp = self.session.get(OPENFDA_URL, params=params, timeout=60)

        # 404 with no_matches means zero results — not an error
        if resp.status_code == 404:
            body = resp.json()
            if "No matches found" in str(body):
                return {"meta": {"results": {"total": 0}}, "results": []}

        resp.raise_for_status()
        return resp.json()
