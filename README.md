Fitness dashboard project

### Purpose

As i've gotten more into fitness, i've become a bit obsessed with lowering my resting heart rate. When i've taken time off for covid or injuries, i've noticed quite a big rise in my resting heart rate.
So i wanted to look at the relation between the two by plotting a trend graph over time.

### Ingestion

Data is sourced from the Fitbit API and a spreadsheet of workouts. These are ingested via the daily cloud functions in this repo.

### Storage and Transformation

Data is housed in BigQuery, where we then use [dbt to make the transformations](https://github.com/Xavsadiq/fitbit_project_dbt).

### Visualisation

Once the data is transformed and pushed back into Bigquery as 'Analytics', we can then use these production tables to power a [Looker Studio dashboard](https://datastudio.google.com/u/0/reporting/300aa1a3-6867-425e-a42d-72b4d98747d0/page/tEnnC).

![Screenshot](https://github.com/Xavsadiq/personal-portfolio/blob/main/src/health-dashboard.PNG "Portfolio Snapshot")