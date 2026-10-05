FROM apache/airflow:2.10.5 AS base
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

FROM base AS test
COPY requirements-dev.txt /
RUN pip install --no-cache-dir -r /requirements-dev.txt
