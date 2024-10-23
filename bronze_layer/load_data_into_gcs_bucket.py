# load it into GCS bucket
from datetime import datetime
from google.cloud import storage

# gs://kanchanearthquakeanalysis/landing/YYYYMMDD/

now = datetime.now()
formatted_date = now.strftime('%Y%m%d')

# Initialize GCS client
client = storage.Client()

# specify your bucket name
bucket_name= 'kanchanearthquakeanalysis'
bucket = client.bucket(bucket_name)

# define the folder name prefix
folder_name = f'raw/landing/{formatted_date}'

# Upload the first JSON file to the specified folder
blob1 = bucket.blob(f'{folder_name}/historical_data.json')
blob1.upload_from_filename('historical_data.json')

# Upload the second JSON file to the specified folder
blob2 = bucket.blob(f'{folder_name}/daily_data.json')
blob2.upload_from_filename('daily_data.json')

print("Files uploaded successfully to folder:", folder_name)