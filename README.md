# TfL Data Pipeline

End-to-end data engineering pipeline extracting Transport for London (TfL) data to populate an analytics-ready dimensional Data Warehouse.

## Stack
**Python · Airflow (Astronomer Runtime, Docker) · AWS S3 / MinIO · PostgreSQL · Pandas**

## Data Sources (TfL API)
* `BikePoints`: Docking station availability.
* `Chargers`: EV chargers availability.
* `Roads`: Active road disruptions.

## Architecture
Extract (TfL API) → land raw JSON in S3 → initially clean with Pandas → stage in S3 → load star schema to Postgres

## Local Environment
This repository is configured for immediate, local execution. 
It uses MinIO to simulate AWS S3 locally. The `docker-compose` setup automatically provisions the required local buckets and Airflow connections. No cloud credentials or manual infrastructure setup are required to test the pipeline.


## Prerequisites
* **Docker Desktop** (Required for the local environment) — [Download](https://www.docker.com/products/docker-desktop/)
* **Astronomer CLI** (Required to run Airflow) — [Install Guide](https://www.astronomer.io/docs/astro/cli/install-cli)

## Quickstart
Spin up the Airflow orchestration, PostgreSQL data warehouse, and MinIO storage:

```bash
astro dev start
```

Access Airflow at localhost:8080 (admin/admin).

## Configuration:
Airflow connections and variables are managed declaratively via `airflow_settings.yaml`. To run this pipeline against AWS S3, simply update the credentials in this file.

## License
MIT