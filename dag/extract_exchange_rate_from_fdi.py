from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

from fazzfinancial.common.callbacks.airflow_alerts import notify_fail
from fazzfinancial.common.utils import override_filepath, override_environment
from fazzfinancial.tasks.extract_exchange_rate.utils import (
    set_defaul_var,
    write_blob_in_json,
    delete_blob,
    list_blobs,
    check_workday_files,
    insert_to_bq_table,
)


GCP_CONN_ID = "composer_worker"
GCP_PROJECT_ID = override_environment(value_prod="d291209", value_dev="payfazz-data-development")

# If we need to add new currencies in the future, only need to modify this list
CURRENCY_LIST = ["EUR", "IDR", "MYR", "SGD", "TWD", "USD", "VND", "GBP", "HKD"]
BUCKET_NAME = "fazz-data-fdi"

with DAG(
    "extract_exchange_rate_from_fdi",
    description="Transform Data from GCS provided by FDI team",
    schedule_interval="0 1 * * *",
    start_date=datetime(2023, 8, 1),
    default_args={
        "depends_on_past": False,
        "on_failure_callback": notify_fail,
        "owner": "louise.kuo",
        "retry_delay": timedelta(minutes=1),
        "retries": 1,
    },
    catchup=False,
    is_paused_upon_creation=True,
    tags=["extract", "bq", "fdi"],
    template_searchpath=[override_filepath("plugins/fazzfinancial/tasks/extract_exchange_rate")],
) as dag:
    initial_node = EmptyOperator(task_id="tasks_start")
    end_node = EmptyOperator(task_id="tasks_end")

    check_workday_files = PythonOperator(
        task_id="check_workday_files",
        python_callable=check_workday_files,
        op_kwargs={"bucket_name": BUCKET_NAME},
    )

    check_bucket_contents = PythonOperator(
        task_id="check_bucket_contents",
        python_callable=list_blobs,
        op_kwargs={"bucket_name": BUCKET_NAME, "check_num": len(CURRENCY_LIST)},
    )

    transfer_tz_in_daily_table = BigQueryExecuteQueryOperator(
        task_id="tz_transform_in_daily_table",
        gcp_conn_id=GCP_CONN_ID,
        sql="sql/daily_exchange_rate.sql",
        use_legacy_sql=False,
        params={
            "GCP_PROJECT_ID": GCP_PROJECT_ID,
        },
        dag=dag,
    )

    calculate_monthly_table = BigQueryExecuteQueryOperator(
        task_id="calculate_monthly_table",
        gcp_conn_id=GCP_CONN_ID,
        sql="sql/monthly_avg.sql",
        use_legacy_sql=False,
        params={
            "GCP_PROJECT_ID": GCP_PROJECT_ID,
        },
        dag=dag,
    )

    for currency in CURRENCY_LIST:
        set_var = PythonOperator(
            task_id=f"set_defaul_var_{currency}",
            python_callable=set_defaul_var,
            op_kwargs={"bucket_name": BUCKET_NAME, "currency": currency},
        )

        copy_to_new_bucket = PythonOperator(
            # [notes]: if old_blob_name does not exist, this node will fail, causing this pipeline break.
            # This is expected since if no blob exist, it shouldn't continue uploading objects.
            task_id=f"copy_{currency}_to_new_bucket",
            python_callable=write_blob_in_json,
            op_kwargs={
                "currency": currency,
            },
        )

        delete_old_data = PythonOperator(
            task_id=f"delete_old_data_{currency}",
            python_callable=delete_blob,
            op_kwargs={
                "currency": currency,
            },
        )

        insert_to_bq = PythonOperator(
            task_id=f"insert_{currency}_to_bq_table",
            python_callable=insert_to_bq_table,
            op_kwargs={
                "currency_list": CURRENCY_LIST,
                "currency": currency,
            },
        )

        (
            initial_node
            >> check_workday_files
            >> set_var
            >> copy_to_new_bucket
            >> delete_old_data
            >> insert_to_bq
            >> check_bucket_contents
            >> transfer_tz_in_daily_table
            >> calculate_monthly_table
            >> end_node
        )
