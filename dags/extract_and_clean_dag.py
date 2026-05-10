from pathlib import Path
from datetime import datetime

from transform import remove_unnecessary_char, replace_null_values, sort_data_by_date

import pandas as pd

from airflow.sdk import DAG, task
from airflow.sdk.definitions.asset import Asset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

BASE_DIR = Path(
    "/opt/airflow"
).parent.parent  
tiktok_google_play_reviews_filepath_csv = (
    BASE_DIR / "data_files" / "tiktok_google_play_reviews.csv"
) 
tiktok_google_play_reviews = Asset(
    name="cleaned_tiktok_google_play_reviews",
    uri=f"file://{BASE_DIR/"xcom_data_files"/"cleaned_tiktok_google_play_reviews.json"}",
)


with DAG(
    dag_id="Extract_and_clean",
    schedule="@daily",
    start_date=datetime(2026, 4, 25),
    catchup=False,
) as dag:
    wait_for_csv = FileSensor(
        task_id="wait_for_json_file",
        filepath=tiktok_google_play_reviews_filepath_csv,
        fs_conn_id="fs_default",
        poke_interval=60,
        timeout=60 * 5,
        mode="poke",
    )

    @task.branch
    def check_file_contents():
        tiktok_google_play_reviews_csv_df = pd.read_csv(
            tiktok_google_play_reviews_filepath_csv
        )
        df_shape = tiktok_google_play_reviews_csv_df.shape
        rows = df_shape[0]
        if rows != 0:
            return "Extract_Clean.replace_null_values"
        else:
            return "empty_file_log"

    empty_file = BashOperator(
        task_id="empty_file_log",
        bash_command='echo "INFO: tiktok_google_play_reviews.csv is empty: {{ds}}"',
    )

    with TaskGroup("Extract_Clean") as ExtractClean:
        t1 = PythonOperator(
            task_id="replace_null_values", python_callable=replace_null_values
        )
        t2 = PythonOperator(
            task_id="sort_data_by_date", python_callable=sort_data_by_date
        )
        t3 = PythonOperator(
            task_id="remove_unnecessary_char",
            python_callable=remove_unnecessary_char,
            outlets=[tiktok_google_play_reviews],  
        )
        t1 >> t2 >> t3
    wait_for_csv >> check_file_contents() >> [ExtractClean, empty_file]
