import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
tiktok_google_play_reviews_filepath_csv = (
    BASE_DIR / "data_files" / "tiktok_google_play_reviews.csv"
)


def replace_null_values(**context):
    """
    Replace all "null" values with "-"
    """

    replace_nulls_df = pd.read_csv(tiktok_google_play_reviews_filepath_csv, header=0)
    replace_nulls_df.fillna("-", inplace=True)
    replace_nulls_df.to_csv(
        BASE_DIR / "xcom_data_files" / "removed_nulls.csv", mode="w", index=False
    )
    removed_nulls_filepath = BASE_DIR / "xcom_data_files" / "removed_nulls.csv"
    ti = context["ti"]
    ti.xcom_push(key="removed_nulls", value=str(removed_nulls_filepath))


def sort_data_by_date(**context):
    """
    Sort data by created_date.
    """
    ti = context["ti"]
    removed_nulls_filepath = ti.xcom_pull(
        task_ids="Extract_Clean.replace_null_values", key="removed_nulls"
    )
    sort_data_by_date_df = pd.read_csv(removed_nulls_filepath, header=0)
    sort_data_by_date_df.rename(columns={"at": "created_at"}, inplace=True)
    sort_data_by_date_df.sort_values(by="created_at", ascending=True, inplace=True)
    sort_data_by_date_df.to_csv(
        BASE_DIR / "xcom_data_files" / "sorted_by_date.csv", mode="w", index=False
    )
    sorted_data_by_date_filepath = BASE_DIR / "xcom_data_files" / "sorted_by_date.csv"
    ti.xcom_push(key="sorted_data_by_date", value=str(sorted_data_by_date_filepath))


def remove_unnecessary_char(**context):
    """
    Remove all unnecessary characters from the content column (e.g. smiley faces, etc.), leaving only text and punctuation marks.
    """
    ti = context["ti"]
    sorted_data_by_date_filpath = ti.xcom_pull(
        task_ids="Extract_Clean.sort_data_by_date", key="sorted_data_by_date"
    )
    remove_unnecessary_char_df = pd.read_csv(sorted_data_by_date_filpath, header=0)
    remove_unnecessary_char_df.replace(
        r"[^a-zA-Z0-9\s.,!?;:()\'\"-@_]", "", regex=True, inplace=True
    )
    remove_unnecessary_char_df.to_csv(
        BASE_DIR / "xcom_data_files" / "cleaned_tiktok_google_play_reviews.csv",
        mode="w",
        index=False,
    )
    remove_unnecessary_char_df.to_json(
        BASE_DIR / "xcom_data_files" / "cleaned_tiktok_google_play_reviews.json",
        mode="w",
        index=False,
        orient="records",
    )


if __name__ == "__main__":
    pass
