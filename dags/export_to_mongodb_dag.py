from datetime import datetime

from load import extract_filepath, load_to_mongodb, tiktok_google_play_reviews


from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="export_to_mongodb",
    schedule=[tiktok_google_play_reviews],
    start_date=datetime(2026, 4, 26),
    catchup=False,
) as export_dag:
    """
    This DAG exports data into a mongodb Database collection,
    it relies on Data aware scheduling in order for it to be triggered,
    hence it is only triggered when  data is updated by upstream tasks in the upstream DAGs.
    """
    t1 = PythonOperator(task_id="extract_filepath", python_callable=extract_filepath)

    t2 = PythonOperator(task_id="load_to_mongodb", python_callable=load_to_mongodb)
    t1 >> t2
