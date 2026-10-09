from airflow.providers.http.hooks.http import HttpHook

from include.connections import TEAMS_CONN_ID


def notify_teams(context):
    from logging import getLogger

    logger = getLogger(__name__)
    if not _is_teams_configured():
        logger.warning("Teams alert skipped: connection '%s' is not configured", TEAMS_CONN_ID)
        return

    try:
        _post_details_to_teams(context)
    except Exception:
        logger.exception("Failed to send Teams alert")


def _is_teams_configured() -> bool:
    from airflow.exceptions import AirflowNotFoundException
    from airflow.hooks.base import BaseHook

    try:
        BaseHook.get_connection(TEAMS_CONN_ID)
    except AirflowNotFoundException:
        return False
    return True


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
    logical_date = context['logical_date']

    return f'Task *{task_id}* in DAG *{dag_id}* has failed on *{logical_date}*.'


def _get_teams_hook() -> HttpHook:
    return HttpHook(
        method='POST',
        http_conn_id=TEAMS_CONN_ID,
    )
