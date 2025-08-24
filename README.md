# TfL  Data Pipeline (WIP)


End-to-end data engineering project using Transport for London (TfL) data to build a small analytics warehouse and dashboard.

## Stack
Python · Airflow (Astronomer Runtime, Docker) · AWS S3 · PostgreSQL (DW) · Pandas · Metabase

## Data Sources (TfL)
- `BikePoint` (bike docking availability)
- `AccidentStats` (historical collisions)
- `Line`/`NetworkStatus` (service disruptions)
- `Road` (road disruptions)

## Architecture
Extract (TfL API) → land raw JSON in S3 → transform with Pandas → load star schema to Postgres → visualize in Metabase.

## Status
**Development**

## S3 / MinIO — main vs dev
- `main` is prod-like: you must provision the S3 **bucket** and Airflow **connection** yourself.

- `dev` is convenience: a local MinIO instance with automatic creation of the bucket and the Airflow connection, no additional setup is required to test locally.

## Quickstart

```bash
    astro dev start
```

## License
MIT
