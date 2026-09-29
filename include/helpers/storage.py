from typing import IO

from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def get_s3_obj(path: str):
    hook = _get_s3_hook()
    return hook.get_key(
        key=path,
        bucket_name=Variable.get('BUCKET_NAME'),
    )


# TODO: return stream and check for gzip file in s3
def read_str_from_s3(path: str) -> str:
    hook = _get_s3_hook()
    return hook.read_key(
        key=path,
        bucket_name=Variable.get('BUCKET_NAME'),
    )


def store_stream_in_s3(
    data_stream: IO[bytes],
    path: str
) -> None:
    s3_client = _get_s3_client()

    s3_client.upload_fileobj(
        Fileobj=data_stream,
        Bucket=Variable.get('BUCKET_NAME'),
        Key=path,
    )


def _get_s3_client():
    return _get_s3_hook().get_conn()

def _get_s3_hook() -> S3Hook:
    return S3Hook(aws_conn_id='s3_conn')
