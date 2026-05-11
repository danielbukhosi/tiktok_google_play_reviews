import json
from pathlib import Path

from pymongo import UpdateOne

from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.sdk.definitions.asset import Asset

BASE_DIR = Path(__file__).parent.parent
tiktok_google_play_reviews_filepath_csv = (
    BASE_DIR / "data_files" / "tiktok_google_play_reviews.csv"
)

tiktok_google_play_reviews = Asset(
    name="cleaned_tiktok_google_play_reviews",
    uri=f"file://{BASE_DIR/"xcom_data_files"/"cleaned_tiktok_google_play_reviews.json"}",
)


def extract_filepath(**context):
    """
    This Function extracts the file path from the asset event and
    pushes it to an xcom for downstream tasks to access it.
    """
    events = context[
        "triggering_asset_events"
    ]  # This returns a dictionary where keys are asset names
    if events:
        file_uri = events[tiktok_google_play_reviews][0].asset.uri
        tiktok_google_play_reviews_filepath_json = file_uri.replace(
            "file://", ""
        )  # strip the 'file://' to get the actual system path
    ti = context["ti"]
    ti.xcom_push(key="file_path", value=tiktok_google_play_reviews_filepath_json)


def load_to_mongodb(**context):
    """
    This Function pulls the filepath from xcom and
    extracts data from a json file and loads to a mongodb collection in batches of 1000
    """

    ti = context["ti"]
    tiktok_google_play_reviews_filepath_json = ti.xcom_pull(
        task_ids="extract_filepath", key="file_path"
    )
    batch_size: int = 1000

    with open(tiktok_google_play_reviews_filepath_json, mode="r") as f:
        data = json.load(f)
    hook = MongoHook(mongo_conn_id="mongodb_conn")
    client = hook.get_conn()
    db = client["tiktok_google_play"]
    collection = db["tiktok_google_play_reviews"]

    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        operations = [
            UpdateOne({"reviewId": record["reviewId"]}, {"$set": record}, upsert=True)
            for record in batch
        ]
        collection.bulk_write(operations, ordered=False)
    client.close()


if __name__ == "__main__":
    pass
