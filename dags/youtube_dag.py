import os
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure project root is on path (for pipelines/, etls/, utils/)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.youtube_pipeline import youtube_pipeline
from pipelines.aws_s3_pipeline import upload_s3_pipeline


with DAG(
    dag_id="etl_youtube_pipeline",
    start_date=pendulum.datetime(2023, 10, 22, tz="UTC"),
    schedule="@daily",   # or None if you only want manual for now
    catchup=False,
    default_args={
        "owner": "Femi",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["youtube", "etl", "pipeline"],
) as dag:

    youtube_extraction = PythonOperator(
        task_id="youtube_extraction",
        python_callable=youtube_pipeline,
        op_kwargs={
            "file_name": "youtube_{{ ds_nodash }}",
            "query": "data engineering",   # change to any topic you like
            "max_results": 25,
        },
    )

    s3_upload = PythonOperator(
        task_id="s3_upload",
        python_callable=upload_s3_pipeline,
        op_kwargs={
            "upstream_task_id": "youtube_extraction",
            "s3_prefix": "youtube/query=data_engineering/date={{ ds_nodash }}"
        }
    )

    youtube_extraction >> s3_upload
