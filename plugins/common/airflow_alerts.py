import os
import json
import pendulum

from fazzfinancial.common.utils.get_gcp_credentials import get_credentials_from_secret_manager
from fazzfinancial.common.utils import override_slack_channel_id

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

tz_jkt = pendulum.timezone("Asia/Jakarta")
tz_sgp = pendulum.timezone("Asia/Singapore")

def notify_base(context, title, color, channel_id="dummy_id"):  
    cred = json.loads(get_credentials_from_secret_manager("SECRET_NAME"))
    slack_token = cred.get("bot_api_token")
    client = WebClient(token=slack_token)

    username = "Airflow"
    channel_id = override_slack_channel_id(channel_id)
    task_id = context.get("task_instance").task_id
    dag_id = context.get("task_instance").dag_id
    log_url = context.get("task_instance").log_url
    exec_date_utc = pendulum.parse(context.get("ts"))
    exec_date_jkt = tz_jkt.convert(exec_date_utc)
    exec_date_sgp = tz_sgp.convert(exec_date_utc)

    fallback_text = f"{title}\nTask: {task_id}\nDag: {dag_id}\nExecution Time: {exec_date_utc}"
    section_text = f"Task: *{task_id}*\nDag: *{dag_id}*\nDag Run: *{exec_date_utc}*\n"
    context_text_jkt = f"Dag Run (JKT): *{exec_date_jkt}*\n"
    context_text_sgp = f"Dag Run (SGP): *{exec_date_sgp}*\n"

    header_block = {
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": title,
        },
    }
    divider_block = {"type": "divider"}
    section_block = {"type": "section", "text": {"type": "mrkdwn", "text": section_text}}
    context_block = {
        "type": "context",
        "elements": [
            {"type": "mrkdwn", "text": context_text_jkt},
            {"type": "mrkdwn", "text": context_text_sgp},
        ],
    }
    actions_block = {
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": "View Log",
                },
                "url": log_url,
                "value": "details",
            }
        ],
    }
    blocks = [header_block, divider_block, section_block, context_block, actions_block]
    attachments = [{"color": color, "fallback": fallback_text, "blocks": blocks}]

    print("Sending First message")
    try:
        response = client.chat_postMessage(
            text="",
            attachments=attachments,
            channel=channel_id,
            username=username,
        )
        original_timestamp = response["ts"]
        print(f"First message sent successfully. Original timestamp: {original_timestamp}")
        return original_timestamp
    except SlackApiError as e:
        # You will get a SlackApiError if "ok" is False
        print(f"Error sending first message: {e.response['error']}")
        raise


def threaded_message(context, original_ts):
    cred = json.loads(get_credentials_from_secret_manager("SECRET_NAME"))
    slack_token = cred.get("bot_api_token")
    client = WebClient(token=slack_token)

    dag = context["dag"]
    channel_id = override_slack_channel_id("dummy_id")  
    airflow_environment = os.getenv("AIRFLOW_ENVIRONMENT")
    test_environment = airflow_environment in ["local", "testing", "development"]
    if test_environment:
        if "P0" in dag.tags:
            mention_text = "Mention dummy oncallbot call data-oncall"
        else:
            mention_text = "Mention dummy data-eng-support"
    else:
        if "P0" in dag.tags:
            mention_text = (
                "<@U028Q3JF1RN> call <!subteam^S04D9FDS6GK>"  # @oncallbot call @data-oncall
            )
        else:
            mention_text = "Mention <!subteam^S029J2XUX6C>"  # @data-eng-support

    print("Sending second message")
    try:
        followup = client.chat_postMessage(
            channel=channel_id,
            text=mention_text,
            thread_ts=original_ts,
        )
        print("Second message sent successfully.")
    except SlackApiError as e:
        print(f"Error sending follow-up message: {e.response['error']}")



def notify_fail(context):
    original_ts = notify_base(context, "Task Failed", "#CF202A")
    threaded_message(context, original_ts)
