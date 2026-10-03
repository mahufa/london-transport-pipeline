from typing import IO

from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from include.connections import BUCKET_NAME_VAR, S3_CONN_ID


def get_s3_obj(
    path: str,
    s3_config = None,
):
    s3_client, bucket_name = s3_config or _get_s3_client_and_bucket()

    return s3_client.get_object(
        Bucket=bucket_name,
        Key=path,
    )


def store_stream_in_s3(
    data_stream: IO[bytes],
    path: str,
    s3_config = None
) -> None:
    s3_client, bucket_name = s3_config or _get_s3_client_and_bucket()

    s3_client.upload_fileobj(
        Fileobj=data_stream,
        Bucket=bucket_name,
        Key=path,
    )


def _get_s3_client_and_bucket():
    return (S3Hook(aws_conn_id=S3_CONN_ID).get_conn(),
            Variable.get(BUCKET_NAME_VAR))
