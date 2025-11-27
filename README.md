## YouTube Data Engineering Pipeline

A simple end-to-end data pipeline that extracts YouTube video data, processes it using Python, orchestrates the workflow using Airflow, and stores the results in Amazon S3 for further analysis.

This project demonstrates core data engineering skills using **Airflow, Docker, AWS**, and the **YouTube Data API**.

### What the Pipeline Does
1. Extracts YouTube video metadata using the YouTube Data API
2. Enriches the data with statistics such as:
    - views
    - likes
    - comments
    - video duration
3. Saves the extracted data as a CSV inside the Airflow container
4. Uploads the final dataset to Amazon S3 (partitioned by query & date)
5. Uses AWS Glue to transform the raw data into a refined/transformed zone
6. Enables analysis via Athena or downstream tools (e.g. Power BI, QuickSight)

The flow is fully orchestrated using **Apache Airflow (Dockerized).**

## Architecture

          Airflow Scheduler
                 |
        Triggers Daily DAG
                 |
                 v
       YouTube API Extraction
                 |
        Writes CSV to local volume
                 |
                 v
            S3 Upload Task
      s3://bucket/youtube/query=X/date=YYYYMMDD/
                 |
                 v
            S3 Raw Layer
                 |
                 v
            AWS Glue Job
                 |
                 v
         S3 Transformed Layer



## Technologies Used
### Orchestration
* Apache Airflow
### Containerization
* Docker & Docker Compose
### Cloud
* AWS S3
* AWS Glue (ETL job)
* AWS IAM
### APIs / Libraries
* YouTube Data API v3
* Python (Requests, Pandas)
* s3fs / boto3


