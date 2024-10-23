# fetch data from API and load it into GCS bucket into json file format

import requests
import json

# define your API endpoints
historical_api_url_1='https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'
daily_api_url_2 = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson'

# fetch data from first API
response_1 = requests.get(historical_api_url_1)
data_1 = response_1.json()

# print(data_1)

# fetch data from second API
response_2= requests.get(daily_api_url_2)
data_2 = response_2.json()

# print(data_2)

# save file into json format
# with open('historical_data.json','w') as f:
#     json.dump(data_1,f)
#
# with open('daily_data.json','w') as f:
#     json.dump(data_2,f)
#-----------------------------------------------------------------------------------------------------









