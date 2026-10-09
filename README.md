# TfL Data Pipeline

[![Tests](https://github.com/mahufa/london-transport-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/mahufa/london-transport-pipeline/actions/workflows/tests.yml)

End-to-end data engineering pipeline extracting Transport for London (TfL) data to populate an analytics-ready dimensional Data Warehouse.

## Stack
**Python · Airflow · AWS S3 / MinIO · PostgreSQL · Metabase**

## Data Sources (TfL API)
* `BikePoints`: Docking station availability.
* `Chargers`: EV chargers availability.
* `Roads`: Active road disruptions.

## Architecture
ELT with a **medallion architecture** in PostgreSQL:
Extract (TfL API) → land raw JSON in S3 → load into **bronze** (Postgres `jsonb`) → validate and flatten into **silver** → model **gold** star schema

Each stage is its own Airflow DAG, chained via **Dataset-driven scheduling**. The three extract DAGs (`tfl_bikes`, `tfl_chargers`, `tfl_roads`) run independently on their own schedules; the shared `loader` and `transformer` DAGs each fire once *any* of their upstream datasets is emitted, split the triggering events per source and map over the batches they carry via dynamic task mapping. Dataset events are emitted by the task that produces the data and carry the S3 path (`raw`) or the `batch_id` (`bronze`) in their extra, so each DAG knows exactly which batch to process.

```mermaid
flowchart LR
    API[("TfL API")]

    subgraph EX["Extract DAGs — tfl_bikes · tfl_chargers · tfl_roads"]
        CHECK["check_api sensor"] --> INGEST["ingest_data"]
    end

    subgraph LD["loader DAG (dataset-triggered)"]
        LOAD["validate_and_load\nstream JSON → COPY"]
    end

    subgraph TR["transformer DAG (dataset-triggered)"]
        SILVER["silver\nchecks + flattening"]
        GOLD["gold merge\nsilver → star schema"]
        SILVER --> GOLD
    end

    BRONZE[("bronze.raw_tfl\njsonb")]
    DLQ[("bronze.rejected_records")]
    DW[("gold\nstar schema\ndim_* / fct_*")]

    API --> CHECK
    INGEST -->|"raw JSON (gzip)"| RAWS3[("S3\n*/raw/")]
    RAWS3 -.->|"Dataset trigger (path)"| LOAD
    LOAD --> BRONZE
    LOAD -->|"invalid records"| DLQ
    BRONZE -.->|"Dataset trigger (batch_id)"| SILVER
    SILVER -->|"rejected rows"| DLQ
    GOLD --> DW
```

* **Extract** — one factory-built DAG per source, each polling the TfL API before streaming the response (gzip-compressed when the API supports it) straight into S3 as raw JSON, tagged with a `batch_id`.
* **Load (bronze)** — triggered by the raw datasets. Streams each S3 object through `ijson`, validates every record and `COPY`s it into `bronze.raw_tfl` as `jsonb`, keyed by `(source, batch_id, record_key)`, where `record_key` is the record's natural key from the payload (configured per source in `include/datasets.py`). Memory use stays flat regardless of file size: records are never materialised as a whole, and deduplication runs in SQL (`row_number()` over a temp landing table), not in Python. Records that aren't JSON objects, lack a natural key, contain null bytes or duplicate a key are routed to the dead-letter table `bronze.rejected_records` (`stage = 'ingest'`) instead of failing the batch. Each batch loads in a single transaction with delete-then-insert, so reruns are idempotent. Every S3 object is loaded by its own mapped task instance, which emits the bronze dataset event for its batch on success, so a file that fails to load doesn't hold back the others.
* **Transform (silver → gold)** — triggered by the bronze datasets with the loaded `batch_id`. Silver is a set of views (`db_init/02_silver.sql`) that flatten and type-check the `jsonb` payloads, so no data is copied between bronze and silver. Per source, the `transformer` DAG maps a task group over the batch ids, so every batch runs its own chain: it writes invalid rows to `bronze.rejected_records` (`stage = 'silver'`, `include/sql/silver/reject_*.sql`), fails the batch if more than `MAX_REJECT_RATIO` (10%, `include/dag_config.py`) of the records received were rejected at ingest or in silver, and merges valid rows idempotently into the gold star schema (`include/sql/gold/`; `ON CONFLICT` upserts for dimensions, composite-key dedup for facts). A batch that fails its check doesn't block the other batches of the same run, and gold merges run one batch at a time to avoid concurrent upserts of the same dimension rows.
* **Visualize** — Metabase sits on top of the warehouse for exploring the loaded data.

## Known Limitations
* **Ingest dead-letter records are buffered in memory.** Valid records are streamed to Postgres, but rejected ones are collected in a Python list and written with a single `COPY` at the end of the batch. With TfL volumes this is negligible, but a much larger (and mostly invalid) input would grow the task's memory with the number of rejects. Scaling options: flush the buffer in fixed-size chunks within the same transaction, or spill rejects to a temporary file.

## Testing
```bash
docker build --target test -t tfl-airflow:ci .
docker run --rm -v "$PWD:/opt/airflow/project" -w /opt/airflow/project -e PYTHONPATH=/opt/airflow/project \
  tfl-airflow:ci python -m pytest -p no:cacheprovider tests
```
* **Unit tests** (`tests/`) cover DAG integrity, JSON validation, `COPY` formatting and dataset-event helpers.
* **Integration tests** (`tests/integration/`) load data into a real PostgreSQL and check dead-letter routing, deduplication, rerun idempotency and the silver reject-ratio check. They need a throwaway database, passed as `DW_TEST_DSN` (e.g. `-e DW_TEST_DSN=postgresql://test:test@host.docker.internal:5434/test`), and are skipped without it. The schema is recreated from `db_init/` on every test session, so never point it at the real warehouse.
* **CI** (GitHub Actions) builds the `test` image with layer caching and runs the full suite against a PostgreSQL service container on every push.

## Local Environment
This repository is configured for immediate, local execution. 
It uses MinIO to simulate AWS S3 locally. The `docker-compose` setup automatically provisions the required local buckets, Airflow connections, and the Metabase admin user/data warehouse connection. No cloud credentials or manual infrastructure setup are required to test the pipeline.


## Prerequisites
* **Docker Desktop** (includes Docker Compose; required to run the whole stack) — [Download](https://www.docker.com/products/docker-desktop/)

## Quickstart
Spin up the Airflow image (built from `Dockerfile` on first run), orchestration, PostgreSQL data warehouse, MinIO storage, and Metabase in one command:

```bash
docker compose up --build
```

`--build` guarantees the custom Airflow image is (re)built from the current `Dockerfile`/`requirements.txt` before starting, so the stack always reflects the code in this repo.

* Access Airflow at localhost:8080 (`admin`/`admin`).
* Access Metabase at localhost:3000 (`admin@example.com`/`MetabaseAdmin123`) — the `postgres_dw` connection is provisioned automatically by the `metabase-init` service, so the warehouse is ready to query as soon as the pipeline has loaded data.

The warehouse schema (`db_init/`: schemas, bronze tables, silver views, gold star schema) is applied by PostgreSQL's init scripts, which run **only on an empty volume**. After changing any file in `db_init/`, either reset with `docker compose down -v` or apply the change by hand (e.g. as `CREATE OR REPLACE VIEW ...`) via `docker compose exec postgres_dw psql -U dw_user -d tfl_dw`.

## Configuration:
Airflow connections and variables are managed declaratively as `AIRFLOW_CONN_*` / `AIRFLOW_VAR_*` environment variables under `x-airflow-common` in `docker-compose.yaml`. To run this pipeline against real AWS S3, replace the `s3_conn` connection's values there with your AWS credentials.

Task failures are reported to Microsoft Teams through the `teams` connection. It's not configured by default, so failures are only logged with a warning; to enable alerts, uncomment `AIRFLOW_CONN_TEAMS` in `docker-compose.yaml` and set your incoming webhook URL as its `host`.

Metabase's admin credentials and the name of the `postgres_dw` connection it creates are configured via environment variables on the `metabase-init` service in `docker-compose.yaml`.

## Teardown
Stop the stack while keeping all data (warehouse, MinIO buckets, Metabase setup) for next time:

```bash
docker compose down
```

Stop the stack and wipe all data, resetting everything to a clean slate (next `docker compose up` will re-run migrations, re-provision buckets, and redo the Metabase setup from scratch):

```bash
docker compose down -v
```

## License
MIT