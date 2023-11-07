
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'download_game_logs',
    default_args=default_args,
    description='A simple DAG to download game logs from GCS',
    schedule_interval=timedelta(days=1),
)

download_task = PythonOperator(
    task_id='download_game_logs_task',
    python_callable=download_game_logs,
    op_kwargs={'bucket_name': 'your-bucket-name', 'source_blob_prefix': 'game_log/', 'destination_folder': '/local/path/to/download/files'},
    dag=dag,
)

download_task
