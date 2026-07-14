# =====================================================================
# MarketPulse Airflow image
# Extends the stock Apache Airflow image with the packages the DAG
# actually needs (dbt-duckdb, great_expectations, openlineage-airflow).
# Using a build-time layer instead of _PIP_ADDITIONAL_REQUIREMENTS keeps
# container startup fast and reproducible.
# =====================================================================
ARG AIRFLOW_BASE_IMAGE_TAG=2.9.2-python3.10
FROM apache/airflow:${AIRFLOW_BASE_IMAGE_TAG}

USER root

# System deps needed by dbt-duckdb / great_expectations wheels and by
# the entrypoint's readiness checks.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY docker/airflow-requirements.txt /opt/airflow/airflow-requirements.txt
COPY docker/airflow-entrypoint.sh /opt/airflow/airflow-entrypoint.sh
RUN chmod +x /opt/airflow/airflow-entrypoint.sh

USER airflow

RUN pip install --no-cache-dir -r /opt/airflow/airflow-requirements.txt

ENTRYPOINT ["/opt/airflow/airflow-entrypoint.sh"]
