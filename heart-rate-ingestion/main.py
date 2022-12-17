import requests
import os
import pandas as pd
import datetime
import functions_framework
import google.cloud.bigquery as bigquery

@functions_framework.http
def main(request):

    # Get fitbit access token stored in google secrets
    access_token = os.environ.get('fitbit_access_token')

    # Connect to BigQuery
    client = bigquery.Client()
    table_id = os.environ.get('bq_fitbit_heart_rate_table')

    # Define time period for query
    day_range_length = 1
    start_date = str((datetime.datetime.now() - datetime.timedelta(days=day_range_length)).date())

    # Fitbit implicit grant flow
    header = { 'Authorization': 'Bearer {}'.format(access_token) }
    response = requests.get("https://api.fitbit.com/1/user/-/activities/heart/date/"+start_date+"/1d.json", headers=header).json()

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
        ]
    )

    # read existing table from bq
    df_bq = pd.read_gbq(table_id)

    # check to see if dates already exist in table and insert
    for date in df['date']:
        if date in df_bq['date'].values:
            print(
                "{} already exists in table.".format(date)
                )
        else:
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)  # Make an API request.
            job.result()  # Wait for the job to complete.
            
            print(
                "{} row inserted into table.".format(df.index)
            )

    return ("Job completed successfully.")