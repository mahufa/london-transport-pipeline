from logging import getLogger

from airflow.providers.http.hooks.http import HttpHook


#TODO: change notifications to connect to any service, not just Teams;
#   move payload shape to airflow variables

def notify_teams(context):
    try:
        _post_details_to_teams(context)
    except Exception:
        logger = getLogger(__name__)
        logger.exception("Failed to connect to API")


def _post_details_to_teams(context):
    hook = _get_teams_hook()
    payload = _prepare_payload(context)

    hook.run(
        json=payload,
        headers={'Content-Type': 'application/json'},
    )


def _prepare_payload(context) -> dict:
    message = _prepare_message(context)
    payload = {
        'attachments': [
            {
                'contentType': 'application/vnd.microsoft.card.adaptive',
                'content': {
                    '$schema': 'http://adaptivecards.io/schemas/adaptive-card.json',
                    'type': 'AdaptiveCard',
                    'version': '1.3',
                    'body': [
                        {
                            'type': 'TextBlock',
                            'text': message,
                            'wrap': True
                        }
                    ]
                }
            }
        ]
    }
    return payload


def _prepare_message(context) -> str:
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    exec_date = context['execution_date']

    return f'Task *{task_id}* in DAG *{dag_id}* has failed on *{exec_date}*.'


def _get_teams_hook() -> HttpHook:
    return HttpHook(
        method='POST',
        http_conn_id='teams',
    )