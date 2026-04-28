# TikTok Google Play Reviews Pipeline
- **An end-to-end data pipeline that extracts, cleans, and loads TikTok Google Play Store reviews into a MongoDB Atlas database using Apache Airflow.**
## Project Structure
- tiktok_google_play_reviews/
- ├── dags/
- │   └── mongodb_dags.py        # Airflow DAG definitions
-  ├── data_files/
-  │   └── tiktok_google_play_reviews.csv   # Source data
-  ├── xcom_data_files/           # Intermediate pipeline files
-  │   ├── removed_nulls.csv
-  │   ├── sorted_by_date.csv
-  │   ├── cleaned_tiktok_google_play_reviews.csv
-  │   └── cleaned_tiktok_google_play_reviews.json
-  └── .env                       # Environment variables
---

## Pipeline Overview

- **The pipeline consists of two Airflow DAGs that work together using Airflow 3's Data-Aware Scheduling (Assets).**

### DAG 1 — `Extract_and_clean`
- Runs on a `@daily` schedule. Detects, validates, and cleans the source CSV file.
- wait_for_csv → check_file_contents → Extract_Clean.replace_null_values
  → Extract_Clean.sort_data_by_date
  → Extract_Clean.remove_unnecessary_char
  ↓ (empty file)
  empty_file_log
  -  | Task | Description |
    |------|-------------|
    | `wait_for_json_file` | FileSensor that waits for the source CSV to exist |
    | `check_file_contents` | Branch task — skips pipeline if file is empty |
    | `Extract_Clean.replace_null_values` | Replaces all null values with `-` |
    | `Extract_Clean.sort_data_by_date` | Renames `at` column to `created_at` and sorts by date |
    | `Extract_Clean.remove_unnecessary_char` | Strips emojis and special characters from review text, emits Asset |
    | `empty_file_log` | Logs a warning if the source file is empty |

### DAG 2 — `export_to_mongodb`
Triggered automatically when the `cleaned_tiktok_google_play_reviews` Asset is updated by DAG 1.
extract_filepath → load_to_mongodb

| Task | Description |
|------|-------------|
| `extract_filepath` | Extracts the JSON file path from the Asset event |
| `load_to_mongodb` | Loads cleaned data into MongoDB Atlas in batches of 1000 using upsert to prevent duplicates |

---

## Technologies

- **Apache Airflow 3** — pipeline orchestration and scheduling
- **Python 3.12** — pipeline logic
- **pandas** — data cleaning and transformation
- **pymongo** — MongoDB client
- **MongoDB Atlas** — cloud database (M0 free tier)
- **python-dotenv** — environment variable management

---

## Setup

### Prerequisites
- Python 3.12
- Apache Airflow 3
- MongoDB Atlas account
- `pip install pandas pymongo python-dotenv certifi apache-airflow-providers-mongo apache-airflow-providers-standard`
### Environment Variables
Create a `.env` file in the project root:
### Airflow Configuration

1. Point Airflow to the project dags folder by updating `airflow.cfg`:
dags_folder = /path/to/tiktok_google_play_reviews/dags

2. Create the filesystem connection for the FileSensor:
```bash
airflow connections add fs_default \
    --conn-type fs \
    --conn-extra '{"path": "/"}'
```

3. Whitelist your IP address in MongoDB Atlas under **Network Access**.

4. Unpause and trigger the pipeline:
```bash
airflow dags unpause Extract_and_clean
airflow dags trigger Extract_and_clean
```

---
## Data Flow
tiktok_google_play_reviews.csv
↓
Replace nulls with "-"
↓
Rename "at" → "created_at", sort by date
↓
Remove emojis and special characters
↓
cleaned_tiktok_google_play_reviews.json  (Asset emitted)
↓
Load to MongoDB Atlas (batched upserts of 1000)
↓
mongodb: tiktok_google_play.tiktok_google_play_reviews

---

## Duplicate Prevention

The `load_to_mongodb` task uses `update_one` with `upsert=True` on the `reviewId` field. This means:
- New records are inserted
- Existing records are updated in place
- No duplicate documents are created on re-runs

---


