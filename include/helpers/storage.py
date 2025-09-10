from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def read_str_from_s3(path: str) -> str:
    from airflow.models import Variable

    hook = _get_s3_hook()
    return hook.read_key(
        key=path,
        bucket_name=Variable.get('BUCKET_NAME'),
    )


def store_str_in_s3(
    data: str,
    path: str
):
    from airflow.models import Variable

    hook = _get_s3_hook()
    hook.load_string(
        string_data=data,
        key=path,
        bucket_name=Variable.get('BUCKET_NAME'),
        replace=True,
    )


def _get_s3_hook() -> S3Hook:
    return S3Hook(aws_conn_id='s3_conn')
