import os
from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator)

# AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")


DBT_DIR = os.getenv("DBT_DIR")

with DAG(
    "game_log_dag",
    default_args={"depends_on_past": True},
    start_date=datetime(2023, 11, 7),
    end_date=datetime(2023, 12, 31),
    schedule_interval="@daily",
) as dag:

    # Task 2 : Load Parquet files into an existing BigQuery table
    load_parquet = BigQueryInsertJobOperator(
        task_id='load_parquet',
        gcp_conn_id="google_cloud_connection",
        configuration={
            "load": {
                "sourceUris": ["gs://nba_stats_57100/game_log/2023-11-08/*.parquet"],
                "destinationTable": {
                    "projectId": "wagon-bootcamp-57100",
                    "datasetId": "nba_stats",
                    "tableId": "player_log_test_marius"
                },
                "sourceFormat": "parquet",
                "autodetect": True,
                "writeDisposition": "WRITE_APPEND",
            }
        },
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_DIR}",
    )




    load_parquet >> dbt_run
