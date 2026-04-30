"""import json
  import os
  from pathlib import Path
  from datetime import datetime 

  from dotenv import load_dotenv
  from pymongo import  UpdateOne
  import pandas as pd

  from airflow.sdk import  DAG, task
  from airflow.sdk.definitions.asset import Asset
  from airflow.providers.standard.operators.python import PythonOperator
  from airflow.providers.standard.operators.bash import BashOperator
  from airflow.providers.standard.sensors.filesystem import FileSensor
  from airflow.providers.mongo.hooks.mongo import MongoHook
  from airflow.utils.task_group import TaskGroup

  BASE_DIR = Path(__file__).parent.parent # This makes sures that during runtime we do not get file not found errors, it is cwd independent for any file used.
  tiktok_google_play_reviews_filepath_csv = BASE_DIR/"data_files"/"tiktok_google_play_reviews.csv"  # PATH TO tiktok_google_play_reviews.csv
  path_to_env = BASE_DIR/".env"  # PATH TO .env file
  load_dotenv(dotenv_path = path_to_env)
  tiktok_google_play_reviews = Asset(
                                      name = "cleaned_tiktok_google_play_reviews",
                                      uri = f"file://{BASE_DIR/"xcom_data_files"/"cleaned_tiktok_google_play_reviews.json"}"
  )



  def replace_null_values(**context):
    
        Replace all "null" values with "-"
    

    replace_nulls_df = pd.read_csv(tiktok_google_play_reviews_filepath_csv,header=0)
    replace_nulls_df.fillna("-",inplace=True)
    replace_nulls_df.to_csv(BASE_DIR/"xcom_data_files"/"removed_nulls.csv",mode='w',index=False)
    removed_nulls_filepath = BASE_DIR/"xcom_data_files"/"removed_nulls.csv"
    ti = context["ti"]
    ti.xcom_push(key="removed_nulls",value =str(removed_nulls_filepath))

  def sort_data_by_date(**context):
    
      Sort data by created_date. 
    
    ti = context["ti"]
    removed_nulls_filepath = ti.xcom_pull(task_ids="Extract_Clean.replace_null_values", key="removed_nulls")
    sort_data_by_date_df = pd.read_csv(removed_nulls_filepath, header=0)
    sort_data_by_date_df.rename(columns={"at":"created_at"}, inplace=True)
    sort_data_by_date_df.sort_values(by="created_at", ascending=True, inplace=True)
    sort_data_by_date_df.to_csv(BASE_DIR/"xcom_data_files"/"sorted_by_date.csv",mode='w',index=False)
    sorted_data_by_date_filepath = BASE_DIR/"xcom_data_files"/"sorted_by_date.csv"
    ti.xcom_push(key="sorted_data_by_date",value=str(sorted_data_by_date_filepath))

  def remove_unnecessary_char(**context):
    
      Remove all unnecessary characters from the content column (e.g. smiley faces, etc.), leaving only text and punctuation marks.
    
    ti = context["ti"]
    sorted_data_by_date_filpath = ti.xcom_pull(task_ids="Extract_Clean.sort_data_by_date",key="sorted_data_by_date")
    remove_unnecessary_char_df = pd.read_csv(sorted_data_by_date_filpath,header=0)
    remove_unnecessary_char_df.replace(r"[^a-zA-Z0-9\s.,!?;:()\'\"-@_]","",regex=True,inplace=True)
    remove_unnecessary_char_df.to_csv(BASE_DIR/"xcom_data_files"/"cleaned_tiktok_google_play_reviews.csv",mode='w',index=False)
    remove_unnecessary_char_df.to_json(BASE_DIR/"xcom_data_files"/"cleaned_tiktok_google_play_reviews.json",mode="w",index=False,orient = "records")

  def extract_filepath(**context):
    
      This Function extracts the file path from the asset event and 
      pushes it to an xcom for downstream tasks to access it.
    
    events = context["triggering_asset_events"] # This returns a dictionary where keys are asset names
    if events:
      file_uri = events[tiktok_google_play_reviews][0].asset.uri
      tiktok_google_play_reviews_filepath_json = file_uri.replace("file://","") # strip the 'file://' to get the actual system path
    ti = context["ti"]
    ti.xcom_push(key="file_path",value=tiktok_google_play_reviews_filepath_json)


  def load_to_mongodb(**context):
    
        This Function pulls the filepath from xcom and 
        extracts data from a json file and loads to a mongodb collection in batches of 1000
    
    load_dotenv(dotenv_path = path_to_env)
    ti = context["ti"]
    tiktok_google_play_reviews_filepath_json = ti.xcom_pull(task_ids="extract_filepath",key="file_path")
    batch_size:int = 1000

    with open(tiktok_google_play_reviews_filepath_json,mode="r") as f:
      data = json.load(f)
    hook = MongoHook(mongo_conn_id = "mongodb_conn")
    client = hook.get_conn()
    db = client["tiktok_google_play"]
    collection = db["tiktok_google_play_reviews"]
    
    for i in range(0, len(data),batch_size):
      batch = data[i:i + batch_size]
      operations = [
            UpdateOne(
              {"reviewId":record["reviewId"]},
              {"$set":record},
              upsert=True
            )
            for record in batch
      ]
    collection.bulk_write(operations,ordered=False)
    client.close()



  with DAG(dag_id = "Extract_and_clean",schedule="@daily",start_date=datetime(2026,4,25),catchup=False) as dag:
      wait_for_csv = FileSensor(
                                task_id="wait_for_json_file",
                                filepath=tiktok_google_play_reviews_filepath_csv,
                                fs_conn_id="fs_default",
                                poke_interval=60,
                                timeout=60*5,
                                mode='poke'

                                )
      @task.branch
      def check_file_contents():
        tiktok_google_play_reviews_csv_df = pd.read_csv(tiktok_google_play_reviews_filepath_csv)
        df_shape = tiktok_google_play_reviews_csv_df.shape
        rows = df_shape[0]
        if rows != 0:
          return "Extract_Clean.replace_null_values"
        else:
          return "empty_file_log"
      
      empty_file = BashOperator(
                                    task_id = "empty_file_log",
                                    bash_command= 'echo "INFO: tiktok_google_play_reviews.csv is empty: {{ds}}"'
      )

      with TaskGroup("Extract_Clean")as ExtractClean:
        t1 = PythonOperator(
                            task_id = "replace_null_values",
                            python_callable=replace_null_values
        )
        t2 = PythonOperator(
                            task_id = "sort_data_by_date",
                            python_callable=sort_data_by_date
        )
        t3 = PythonOperator(
                            task_id = "remove_unnecessary_char",
                            python_callable= remove_unnecessary_char,
                            outlets=[tiktok_google_play_reviews] # emit the asset
        )
        t1>>t2>>t3
      wait_for_csv>>check_file_contents()>>[ExtractClean,empty_file]

  with DAG(dag_id="export_to_mongodb",schedule=[tiktok_google_play_reviews],start_date=datetime(2026,4,26), catchup=False)as export_dag:
      
          This DAG exports data into a mongodb Database collection, 
          it relies on Data aware scheduling in order for it to be triggered, 
          hence it is only triggered when  data is updated by upstream tasks in the upstream DAGs.
      
      t1 = PythonOperator(
                              task_id ="extract_filepath",
                              python_callable=extract_filepath
        )
      
      t2 = PythonOperator(
                              task_id = "load_to_mongodb",
                              python_callable=load_to_mongodb
        )
      t1>>t2
"""



