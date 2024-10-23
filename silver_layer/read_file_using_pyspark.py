from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder \
    .appName("earthquake_historical_data_test") \
    .getOrCreate()

now = datetime.now()
formatted_date = now.strftime('%Y%m%d')

bucket_name= 'kanchanearthquakeanalysis'
folder_name = f'raw/landing/{formatted_date}'


gcs_path = r"C:\Users\kagar\pyspark_projects\earthquake_project\bronze_layer\historical_data.json"  # local path

# gcs_path =r"gs://kanchanearthquakeanalysis/raw/landing/20241021/historical_data.json"  # gcs path

# gcs_path=f"gs://{bucket_name}/{folder_name}/historical_data.json"

df = spark.read.json(gcs_path)

df.show(2)
# df.printSchema()
# print(df.count())