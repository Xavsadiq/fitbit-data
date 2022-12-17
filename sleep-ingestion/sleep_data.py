# -*- coding: utf-8 -*-
"""
Created on Fri Dec 16 14:45:29 2022

@author: xav
"""

import requests
import os
import datetime
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# Look for .env.local file
load_dotenv(os.getcwd()+'/.env.local')
access_token = os.environ.get("access_token")

# Auth for google cloud using service account
key_path = os.getcwd()+"/service_account.json"

credentials = service_account.Credentials.from_service_account_file(
key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

# Connect to BigQuery
client = bigquery.Client(credentials=credentials)
table_id = os.environ.get("table_id")

# Define time period for query
day_range_length = 90
start_date = str((datetime.datetime.now() - datetime.timedelta(days=day_range_length)).date())
end_date = str(datetime.datetime.date(datetime.datetime.now()))

# Fitbit implicit grant flow
header = { 'Authorization': 'Bearer {}'.format(access_token) }
response = requests.get("https://api.fitbit.com/1.2/user/-/sleep/date/"+start_date+"/"+end_date+".json", headers=header).json()

data = response['sleep']

result = []

df = pd.json_normalize(data)
df = df.drop(columns=['infoCode', 'type', 'levels.data', 'levels.shortData', 'logType', 'logId', 'isMainSleep'])

    



