# Reddit Dashboard

## Project Description

- Develop an end-to-end ETL data dashboard for One Piece Subreddit for each week.
  - [Looker Studio Dashboard](https://lookerstudio.google.com)

## Tools

1. Docker
2. Python
3. Spark
4. Airflow
5. Google Cloud Platform
   - Google Cloud Storage
   - Google BigQuery
   - Google Looker Studio
6. SQL

## Architecture

<!-- ![Architecture Image](img/achitecture_img.png) -->

## How To Run

### Create an API Account for Reddit

1. Go to [Reddit Apps Preferences](https://www.reddit.com/prefs/apps)
2. Create a web application and use the provided key and secret in the subsequent steps.

### Create Google Cloud Project

1. Visit the [Google Cloud Console](https://console.cloud.google.com/) and create a project.
2. Obtain the `project_id` (**not** the project name) to use in the following steps.

### Build the Customized Airflow Image

### Get the GCP Service Account

### Run Airflow DAG

#### Add the Spark Operator in Airflow

#### Run the DAG