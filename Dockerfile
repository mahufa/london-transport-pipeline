FROM apache/airflow:2.10.5 AS base
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY requirements-dbt.txt /
ENV DBT_VENV=/opt/airflow/dbt_venv
RUN python -m venv $DBT_VENV && $DBT_VENV/bin/pip install --no-cache-dir -r /requirements-dbt.txt

FROM base AS test
COPY requirements-dev.txt /
RUN pip install --no-cache-dir -r /requirements-dev.txt
