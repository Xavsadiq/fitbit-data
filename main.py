import requests
import os
import pandas as pd
import datetime
import functions_framework
import google.cloud.bigquery as bigquery

@functions_framework.http
def main(request):
    # Get fitbit access token // stored in google secret manager

    access_token = os.environ.get('fitbit_access_token')

    # Connect to BigQuery
    client = bigquery.Client()
    table_id = os.environ.get('bq_fitbit_heart_rate_table')

    # Define time period for query
    day_range_length = 90
    start_date = str((datetime.datetime.now() - datetime.timedelta(days=day_range_length)).date())
    end_date = str(datetime.datetime.date(datetime.datetime.now())) 

    # Update your start and end dates here in yyyy-mm-dd format 
    start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    # Fitbit implicit grant flow
    header = { 'Authorization': 'Bearer {}'.format(access_token) }
    response = requests.get("https://api.fitbit.com/1/user/-/activities/heart/date/"+start_date+"/"+end_date+".json", headers=header).json()

    try:
        print(response['activities-heart'])
        df = pd.json_normalize(response['activities-heart'])
        df = pd.concat([df, df.pop('value.heartRateZones').apply(pd.Series)], axis=1)
        df = df.drop(columns=['value.customHeartRateZones'])
        df.reset_index()
        df.columns = ['date', 'resting_heart_rate', 'out_of_range_zone', 'fat_burn_zone', 'cardio_zone', 'peak_zone']
        df = df.fillna(0)
        
    except KeyError as e:
            print(e)

    # Set correct data types for ingestion
    df['date'] = pd.to_datetime(df['date'])
    df['resting_heart_rate'] = df['resting_heart_rate'].astype(str)
    df['out_of_range_zone'] = df['out_of_range_zone'].astype(str)
    df['fat_burn_zone'] = df['fat_burn_zone'].astype(str)
    df['cardio_zone'] = df['cardio_zone'].astype(str)
    df['peak_zone'] = df['peak_zone'].astype(str)

    # Load dataframe to BigQuery
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATETIME),
            bigquery.SchemaField("resting_heart_rate", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("out_of_range_zone", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("fat_burn_zone", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("cardio_zone", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("peak_zone", bigquery.enums.SqlTypeNames.STRING),
        ],
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)  # Make an API request.

    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.

    return (
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
