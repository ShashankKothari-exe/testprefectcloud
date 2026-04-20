# Chapter & section notes flows (portable bundle)

Self-contained copy of the two Prefect flows that download USITC chapter/section notes to bronze and transform HTML to JSON in silver. Paste this entire folder into any repository and run it from there.

## Setup

```bash
cd chapter_section_notes_flows
python3.11 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Configuration

- **`EXECUTION_ENVIRONMENT`**: `local` uses `./bronze` and `./silver` under this folder (created automatically). Any other value uses Azure Blob Storage with the same container and Prefect Secret as the repo’s `flows/etl_flow.py` example.
- **Azure (non-local)**: data is written under container **`ptest`** (bronze HTML and silver JSON use different prefixes: `chapter_section_wco_notes/html/...` vs `chapter_section_wco_notes/json/...`). Credentials come from the Prefect Secret block **`azure-ptest`** (SAS query string, full container URL, or JSON with `account_name` and `sas_token`). Optional overrides: **`CCT_AZURE_STORAGE_ACCOUNT`** (default `jmdtestingkyg`), **`CCT_AZURE_BLOB_CONTAINER`** (default `ptest`), **`CCT_AZURE_SECRET_BLOCK_NAME`** (default `azure-ptest`).
- **`CCT_CONCURRENT_EXECUTION_COUNT`**: optional worker count (default `6`).
- **`CLASSIFICATION_CODES_PROJECT_ROOT`**: optional; set to the absolute path of this folder if local bronze/silver paths resolve incorrectly.
- **Prefect**: configure `PREFECT_API_URL` / auth so `Secret.load("azure-ptest")` works for non-local runs.
- Copy `.env` from the main project or set variables in the environment.

## Run

```bash
python run_pipeline.py
```

Options: `--bronze-only`, `--silver-only`, `-v`.

## Contents

| Path | Role |
|------|------|
| `flows.py` | The two Prefect flows |
| `run_pipeline.py` | CLI |
| `app/lib/notes_utils.py`, `app/tasks/notes/*.py` | Logic (copied from kyg-classification-codes-dataflows) |
| `app/lib/storage_utils.py` | Minimal storage + datasource helpers for these flows only |

When you update behavior in the main repo, refresh the copied modules and re-test this bundle.
