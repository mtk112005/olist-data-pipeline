FROM apache/airflow:2.10.5-python3.11

# dbt lives in its own virtualenv so its dependencies never fight Airflow's.
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.12.7 dbt-postgres==1.9.0

# Pipeline code needs pandas/pyarrow/psycopg2 (pinned to Airflow's constraints).
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
    pandas pyarrow psycopg2-binary

ENV DBT_BIN=/home/airflow/dbt-venv/bin/dbt \
    PROJECT_ROOT=/opt/project \
    PYTHONPATH=/opt/project
