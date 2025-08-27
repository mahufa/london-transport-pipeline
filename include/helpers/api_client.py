from airflow.providers.http.hooks.http import HttpHook


def get_api_data(
    endpoint: str,
    params: dict = None,
) -> str:
    api = get_api_hook()
    response = api.run(
        endpoint=endpoint,
        data=params,
    )
    return response.json()

def get_api_hook() -> HttpHook:
    return HttpHook(
        method='GET',
        http_conn_id='tfl_api',
    )