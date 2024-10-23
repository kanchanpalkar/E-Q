from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.functions import split
from pyspark.sql.functions import col, explode, split, trim,concat, lit,regexp_extract,from_unixtime,current_timestamp,count,avg,trunc
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery

spark = SparkSession.builder \
    .appName("earthquake_historical_data_test") \
    .getOrCreate()

now = datetime.now()
formatted_date = now.strftime('%Y%m%d')

bucket_name= 'kanchanearthquakeanalysis'
folder_name = f'raw/landing/{formatted_date}'


gcs_path = r"C:\Users\kagar\pyspark_projects\earthquake_project\bronze_layer\historical_data.json"
df = spark.read.json(gcs_path)

# df.show(2)
# df.printSchema()

# Explode the features array
features_df = df.select(explode(col("features")).alias("feature"))

# Select the relevant columns from the exploded DataFrame
transformed_df = features_df.select(
    col("feature.properties.type").alias("type"),
    col("feature.properties.mag").alias("magnitude"),
    col("feature.properties.place").alias("place"),
    col("feature.properties.time").alias("time"),
    col("feature.properties.updated").alias("updated"),
    col("feature.properties.tz").alias("tz"),
    col("feature.properties.url").alias("url"),
    col("feature.properties.detail").alias("detail"),
    col("feature.properties.felt").alias("felt"),
    col("feature.properties.cdi").alias("cdi"),
    col("feature.properties.mmi").alias("mmi"),
    col("feature.properties.alert").alias("alert"),
    col("feature.properties.status").alias("status"),
    col("feature.properties.tsunami").alias("tsunami"),
    col("feature.properties.sig").alias("sig"),
    col("feature.properties.net").alias("net"),
    col("feature.properties.code").alias("code"),
    col("feature.properties.ids").alias("ids"),
    col("feature.properties.sources").alias("sources"),
    col("feature.properties.types").alias("types"),
    col("feature.properties.nst").alias("nst"),
    col("feature.properties.dmin").alias("dmin"),
    col("feature.properties.rms").alias("rms"),
    col("feature.properties.gap").alias("gap"),
    col("feature.properties.magType").alias("magType"),
    col("feature.properties.title").alias("title"),
    col("feature.geometry.type").alias("geometry_type"),
    # Extract longitude, latitude, and depth from coordinates
    col("feature.geometry.coordinates")[0].alias("longitude"),
    col("feature.geometry.coordinates")[1].alias("latitude"),
    col("feature.geometry.coordinates")[2].alias("depth")
)

# print(transformed_df.columns)
# transformed_df.show(1)
# print(transformed_df.count())
# transformed_df.select('time').show()
#-------------------------------------------------------------------------------------------------------------
# save it into silver layer of bucket
# Initialize GCS client
client = storage.Client()

json_data = transformed_df.toJSON().collect()
json_string= "[" + ",".join(json_data) + "]"

# specify your bucket name
bucket_name= 'kanchanearthquakeanalysis'
bucket = client.bucket(bucket_name)

# Store data to GCS
# filename = f"flattened_data_{datetime.now().strftime('%Y%m%d')}.json"
# blob = bucket.blob(f'Silver/{filename}')
# blob.upload_from_string(json_string, content_type='application/json')
# print(f'File uploaded to GCS: {filename}')

#------------------------------------------------------------------------------------------
# Just checking the output of transformed df is it correct or not --- it is correct ? -- correct
# output_path_csv = f"gs://{bucket_name}/{folder_name}/transformed_df_output.csv"  # Replace with your GCS path
# transformed_df.write.mode("overwrite").csv(output_path_csv, header=True)


# adding new column in the DF -- adding a new column on the basis of place
'''
transformed_df.select('place').show(truncate=False)

transformed_df = transformed_df.withColumn(
    "city",
    trim(split(col("place"), ",").getItem(1))  # Get last part after comma
)

# Show the updated DataFrame
transformed_df.select('city').show()


'''
# here i have fetched state name but we need to fetch city and state names  so here we are  -- DONE

transformed_df = transformed_df.withColumn(
    "city",
    trim(split(col("place"), " ").getItem(5))  # Get the last word before the comma
).withColumn(
    "state",
    trim(split(col("place"), ",").getItem(1))  # Get the city name after the comma
)

# transformed_df.select("place", "city", "state").show(truncate=False)


# now changing the time and updated  --- DONE

transformed_df = transformed_df.withColumn("time", from_unixtime(col("time") / 1000)) \
    .withColumn("updated", from_unixtime(col("updated") / 1000))

# transformed_df.select('time','updated').show()


# adding one additional column called timestamp


transformed_df = transformed_df.withColumn("insert_date", current_timestamp())

transformed_df.show(10)
# print(transformed_df.count())

#------------------------------------------------------------------------------------------------
# Q.1 count the number of earthquake by region
result_df = transformed_df.groupBy(col("city"), col("state")) \
                          .agg(count(col("type")).alias("type_count"))

# print(result_df.show())
#----------------------------------------------------------------------------------------------------
# Q.2 find the average magnitude by the region
result_df=transformed_df.groupBy(col('city'),col('state')).agg(avg(col("magnitude")))

# print(result_df.show())
#----------------------------------------------------------------------------------------------------
# Q.3 find how many earthquakes happen on the same day
result_df=transformed_df.groupBy(col('updated')).agg(count(col('type')))
# print(result_df.show())
#---------------------------------------------------------------------------------------------------------
# Q.4 Find how many earthquakes happen on same day and in same region
result_df=transformed_df.groupBy(col('updated'),col('city'),col('state')).agg(count(col('type')))
# print(result_df.show())
#-------------------------------------------------------------------------------------------------------
# Q.5 Find average earthquakes happen on the same day.
from pyspark.sql.functions import col, count, avg, from_unixtime, to_date

# Convert 'time' column (if in epoch milliseconds) to date format and truncate to day level
transformed_df = transformed_df.withColumn('event_date', to_date(from_unixtime(col('time') / 1000)))

# Group by 'event_date' and count the number of earthquakes on each day
daily_earthquake_count = transformed_df.groupBy('event_date') \
                                       .agg(count(col('type')).alias('earthquake_count'))

# Calculate the average number of earthquakes per day
result_df = daily_earthquake_count.agg(avg(col('earthquake_count')).alias('avg_earthquakes_per_day'))

# Show the result
# print(result_df.show())
#------------------------------------------------------------------------------------------------------
# Q.6 Find average earthquakes happen on same day and in same region.
from pyspark.sql.functions import col, count, avg, to_date

# Convert the 'updated' column to just the date (assuming it's a timestamp)
transformed_df = transformed_df.withColumn('event_date', to_date(col('updated')))

# Group by 'event_date' and 'place' (or city, state, etc.), and count the number of earthquakes
daily_region_earthquake_count = transformed_df.groupBy(col('event_date'), col('place')) \
                                              .agg(count(col('type')).alias('earthquake_count'))

# Calculate the average number of earthquakes per region per day
result_df = daily_region_earthquake_count.groupBy(col('place')) \
                                         .agg(avg(col('earthquake_count')).alias('avg_earthquakes_per_day'))

# Show the result
# result_df.show()
#------------------------------------------------------------------------------------------------------------
# Q.7 Find the region name, which had the highest magnitude earthquake last week.
from pyspark.sql.functions import col, max, from_unixtime, to_date
from datetime import datetime, timedelta

# Convert 'time' (or 'updated') column to date if needed
transformed_df = transformed_df.withColumn('event_date', to_date(from_unixtime(col('time') / 1000)))

# Get the current date and the date for one week ago
today = datetime.now()
one_week_ago = today - timedelta(days=7)

# Filter for earthquakes that occurred in the last week
last_week_df = transformed_df.filter((col('event_date') >= one_week_ago.strftime('%Y-%m-%d')) &
                                     (col('event_date') <= today.strftime('%Y-%m-%d')))

# Find the row with the maximum magnitude earthquake
max_magnitude_df = last_week_df.orderBy(col('magnitude').desc()).limit(1)

# Select the region (place) with the highest magnitude
max_magnitude_region = max_magnitude_df.select(col('place'), col('magnitude'))

# Show the result
# max_magnitude_region.show()
#-----------------------------------------------------------------------------------------------------------
# Q.8. Find the region name, which is having magnitudes higher than 5.
result_df=transformed_df.filter(col('magnitude') > 5).select(col('place'),col('magnitude'))
# result_df.show()
#---------------------------------------------------------------------------------------------------------------
# Q.9 Find out the regions which are having the highest frequency and intensity of earthquakes.
from pyspark.sql.functions import col, count, avg, max

# Group by the region (e.g., 'place') and calculate both the frequency (count of earthquakes)
# and the average magnitude (intensity) of earthquakes in each region.
result_df = transformed_df.groupBy(col('place')) \
                          .agg(count(col('type')).alias('earthquake_frequency'),
                               avg(col('magnitude')).alias('avg_magnitude'),
                               max(col('magnitude')).alias('max_magnitude')) \
                          .orderBy(col('earthquake_frequency').desc(), col('avg_magnitude').desc())

# Show the results
# result_df.show()
#----------------------------------------------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("My App") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.25.0") \
    .getOrCreate()

# Initialize the BigQuery client
client = bigquery.Client()

# Verify that the client is working by printing the project ID
print("Client project:", client.project)


# Load data into BQ  -- direct method you can use overwrite, append
table_id = "extended-creek-431414-c1.earthquake_dataset.earthquake_tb"

# Writing the DataFrame to BigQuery
transformed_df.write.format("bigquery") \
    .option("table", table_id) \
    .option("writeMethod", "direct") \
    .save()
#-----------------------------------------------------------------------------------------------------------
# loading data into GCS Silver Layer -- rename the file name after as this one stored in parquet file format

# bucket_name = 'kanchanearthquakeanalysis'
# file_name='flatten_data'
# gcs_bucket_path = f'gs://{bucket_name}/raw/silver/{formatted_date}/{file_name}.parquet'
# file_format = "parquet"  # You can change this to "csv", "json", etc.



# Write the DataFrame as a CSV file to GCS
# transformed_df.write \
#     .mode("overwrite")  \
#     .parquet(gcs_bucket_path)
#
# print("Done")


