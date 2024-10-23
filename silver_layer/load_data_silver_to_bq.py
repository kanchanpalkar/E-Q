import json
from google.cloud import bigquery
from google.cloud import storage
import os

# Setup Google Cloud Storage credentials
os.environ[
    'GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\kagar\pyspark_projects\earthquake_project\extended-creek-431414-c1-bb3a9b85493a.json"


def download_json_from_gcs(bucket_name, blob_name, local_file_name):
    """Download JSON file from Google Cloud Storage to local disk."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(local_file_name)
    print(f"Downloaded {blob_name} from GCS to {local_file_name}.")


def convert_json_to_ndjson(input_file_path, output_file_path):
    """Convert JSON array to newline-delimited JSON."""
    with open(input_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    with open(output_file_path, 'w', encoding='utf-8') as f:
        for item in data:
            f.write(json.dumps(item) + '\n')

    print(f"Converted JSON array to newline-delimited format and saved to {output_file_path}.")


def load_data_to_bigquery_from_local(file_path, table_id):
    """Load data from a local NDJSON file to BigQuery."""
    client = bigquery.Client()

    with open(file_path, "rb") as source_file:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
        )
        load_job = client.load_table_from_file(
            source_file,
            table_id,
            job_config=job_config,
        )

        load_job.result()  # Waits for the job to complete.
        print(f"Loaded {load_job.output_rows} rows into {table_id}.")


# Define your parameters
bucket_name = 'kanchanearthquakeanalysis'  # GCS bucket name
input_file_name = 'Silver/flattened_data_20241022.json'  # Path to the input file in GCS
local_input_file = 'flattened_data_20241022.json'  # Local file name for download
output_file_path = 'flattened_data_ndjson.json'  # Local output file name
table_id = 'extended-creek-431414-c1.earthquake_dataset.earthquake_tb'  # BigQuery table ID

# Download the JSON file from GCS
download_json_from_gcs(bucket_name, input_file_name, local_input_file)

# Convert JSON array to newline-delimited format
convert_json_to_ndjson(local_input_file, output_file_path)

# Load the NDJSON file directly into BigQuery
load_data_to_bigquery_from_local(output_file_path, table_id)

