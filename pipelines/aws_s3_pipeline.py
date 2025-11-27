from pathlib import Path

from etls.aws_etl import connect_to_s3, create_bucket_if_not_exist, upload_to_s3
from utils.constants import AWS_BUCKET_NAME


def upload_s3_pipeline(ti, upstream_task_id: str, s3_prefix: str = "youtube/raw", **context):
    """
    Generic S3 upload pipeline.

    - Pulls the file path returned by the upstream task via XCom.
    - Uploads it to S3 under <s3_prefix>/<filename>.
    """

    # 1) Get file path from XCom (returned by youtube_pipeline)
    file_path = ti.xcom_pull(task_ids=upstream_task_id, key="return_value")
    if not file_path:
        raise ValueError(f"No file path found in XCom for task_id='{upstream_task_id}'")

    local_name = Path(file_path).name

    # Normalize prefix to avoid '//' in the key
    prefix = s3_prefix.rstrip("/")
    s3_key = f"{prefix}/{local_name}"

    print(f"📁 Uploading local file: {file_path}")
    print(f"🌩️ To S3 bucket: s3://{AWS_BUCKET_NAME}/{s3_key}")

    # 2) Connect to S3
    s3 = connect_to_s3()
    create_bucket_if_not_exist(s3, AWS_BUCKET_NAME)

    # 3) Upload
    upload_to_s3(s3, file_path, AWS_BUCKET_NAME, s3_key)

    print("✅ Upload complete.")
