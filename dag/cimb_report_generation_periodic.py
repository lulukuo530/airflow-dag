# https://5431fcb4ba994c87a144b01e19070fae-dot-asia-southeast1.composer.googleusercontent.com/dags/pusdafil_pushmail_daily/graph

import os
import pendulum
from datetime import datetime
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator

from fazzfinancial.common.callbacks.airflow_alerts import notify_fail
from fazzfinancial.common.utils import override_filepath
from fazzfinancial.tasks.cimb_report_generation.utils import (
    generate_periodic_cimb_report,
    send_periodic_report,
)


GCP_PROJECT_ID = "d291209"
gcp_conn_id = "composer_worker"
local_tz = pendulum.timezone("Asia/Singapore")


default_args = {
    "owner": "louise.kuo",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 1, tzinfo=local_tz),
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "cimb_report_generation_periodic",
    description="Daily send reports via email",
    schedule_interval="0 9 3 * *",
    template_searchpath=[override_filepath("plugins/fazzfinancial")],
    tags=["cimb", "periodic"],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    on_failure_callback=notify_fail,
    default_args=default_args,
) as dag:
    initial_node = EmptyOperator(task_id="initial_node")

    generate_periodic_cimb_report = PythonOperator(
        task_id="generate_periodic_cimb_report",
        python_callable=generate_periodic_cimb_report,
        op_kwargs={"report_date": pendulum.now(local_tz).date() - pendulum.duration(days=1)},
    )

    # send_email = EmptyOperator(task_id="send_daily_report")
    send_email = PythonOperator(
        task_id="send_periodic_report",
        python_callable=send_periodic_report,
        op_kwargs={
            "task_id": "generate_periodic_cimb_report",
        },
    )

    (initial_node >> generate_periodic_cimb_report >> send_email)
