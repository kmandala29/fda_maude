# FDA MAUDE — Becton Dickinson Adverse Event Pipeline

Extracts medical device adverse event reports for **Becton Dickinson** from the
[openFDA API](https://open.fda.gov/apis/device/event/) and loads them into
**Databricks** using a Bronze → Silver → Gold medallion architecture.

## What is MAUDE?

The FDA's Manufacturer and User Facility Device Experience (MAUDE) database
contains reports of adverse events involving medical devices submitted by
manufacturers, importers, device user facilities, and voluntary reporters.

## Architecture

```
openFDA REST API
  (device/event endpoint)
        │
        ▼
01_extract_to_volume
  ├── Raw JSON  →  /Volumes/main/fda_maude/raw_json/{run_id}/page_NNNN.json
  └── Flattened →  main.fda_maude.maude_events_bronze  (Delta, append)
        │
        ▼
02_silver_transform
  ├── main.fda_maude.maude_events_silver     (one row per report)
  ├── main.fda_maude.maude_devices_silver    (one row per device)
  └── main.fda_maude.maude_narratives_silver (one row per text block)
        │
        ▼
03_gold_analysis
  ├── main.fda_maude.bd_event_summary        (volume by type/year/month)
  ├── main.fda_maude.bd_product_risk         (adverse event rate by product)
  ├── main.fda_maude.bd_outcome_breakdown    (patient outcomes distribution)
  └── main.fda_maude.bd_narrative_flagged    (keyword-flagged high-risk reports)
```

## Project Structure

```
fda_maude/
├── config.yaml                       # Manufacturer, dates, catalog/schema config
├── requirements.txt
├── maude_ingestion/
│   ├── extractor.py                  # openFDA API client with pagination & retry
│   ├── loader.py                     # Volume JSON writer + Bronze Delta writer
│   └── pipeline.py                   # Orchestration
├── notebooks/
│   ├── 01_extract_to_volume.py       # Extract → Volume + Bronze
│   ├── 02_silver_transform.py        # Bronze → Silver (explode arrays)
│   └── 03_gold_analysis.py           # Silver → Gold (aggregations + keyword scan)
└── tests/
    └── test_extractor.py
```

## Gold Tables

| Table | Description |
|---|---|
| `bd_event_summary` | Report counts by event type, year, month |
| `bd_product_risk` | Adverse event rate + risk tier by brand/generic name |
| `bd_outcome_breakdown` | Patient outcome distribution (Death, Injury, etc.) by year |
| `bd_narrative_flagged` | Reports whose narrative text contains high-risk keywords |

## Setup

### 1. Update `config.yaml`
```yaml
extraction:
  manufacturer: "becton dickinson"
  date_from: "2023-01-01"
  date_to:   "2025-12-31"
databricks:
  catalog: "main"
  schema:  "fda_maude"
  volume:  "raw_json"
```

### 2. (Optional) Get an openFDA API key
Free, higher rate limits: https://open.fda.gov/apis/authentication/

```bash
# Set as Databricks secret
databricks secrets create-scope fda
databricks secrets put-secret fda openfda-api-key
```

Or set `OPENFDA_API_KEY` environment variable.

### 3. Push to Databricks Repos and run notebooks in order

| Step | Notebook |
|------|----------|
| 1 | `01_extract_to_volume` — widgets: `date_from`, `date_to`, `manufacturer` |
| 2 | `02_silver_transform` |
| 3 | `03_gold_analysis` |

### 4. Schedule as a Databricks Workflow

Chain the three notebooks sequentially. Recommended: monthly run aligned with
FDA's monthly MAUDE data update cycle.

## Running Tests Locally

```bash
pip install -r requirements.txt pytest
pytest tests/
```

## Data Source

- **openFDA device/event API**: https://open.fda.gov/apis/device/event/
- **MAUDE database**: https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfmaude/search.cfm
- Coverage: 2009–present, updated monthly
