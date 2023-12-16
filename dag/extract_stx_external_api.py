"""
Copy the format in defi_metrics
Need to extract four different APIs: https://www.notion.so/fazzfinancialgroup/95942abf63214aa486ea92d450837ca4?v=6d15e2beeaf14dc39db74fe30104b921
"""
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.macros import timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator

from fazzfinancial.common.callbacks.airflow_alerts import notify_fail
from fazzfinancial.common.utils import override_filepath
from fazzfinancial.common.utils import override_environment
from fazzfinancial.tasks.extract_stx_external_api.utils import (
    get_binance_data,
    get_crypto_com_data,
    get_coingecko_com_data,
    get_bittrex_global,
    get_cex_io_data,
)

gcp_conn_id = "composer_worker"
location = "asia-southeast2"
GCP_PROJECT_ID = override_environment(value_prod="d291209", value_dev="payfazz-data-development")

default_args = {
    "owner": "louise.kuo",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 1),
    "catchup": False,
    "retries": 12,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "extract_stx_external_api",
    description="STX External API",
    schedule_interval="10 * * * *",
    template_searchpath=[override_filepath("plugins/fazzfinancial")],
    tags=["metrics"],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    on_failure_callback=notify_fail,
    default_args=default_args,
) as dag:
    initial_node = DummyOperator(task_id="initial_node")

    api_info_dict = {
        "binance_id": {  # Part 1: Binance ID, DE-2441
            "secret_name": "airflow__connections__extract_stx_external_api__binance_id",
        },
        "crypto.com": {  # Part 2: Crypto.com, DE-3865
            "secret_name": "airflow__connections__extract_stx_external_api__crypto_com",
        },
        "coingecko.com": {  # Part 3: coingecko.com, DE-3775
            "ids": [
                "binance-usd",
                "defi-04ab07ad-43a9-4d63-a379-2c6a2499f748",
                "ethereum",
                "hedera-hashgraph",
                "matic-network",
                "straitsx-indonesia-rupiah",
                "tether",
                "usd-coin",
                "weth",
                "wrapped-bitcoin",
                "xsgd",
                "zilliqa",
            ],
            "vs_currencies": "sgd,usd",
        },
        "bittrex_global": {  # Part 4: bittrex.com, DE-4101
            "secret_name": "airflow__connections__extract_stx_external_api__bittrex_global",
            "measure_name_prefix": "bittrex_global",
        },
        "bittrex_global_otc": {  # Part 6: bittrex.com, DE-4245
            "secret_name": "airflow__connections__extract_stx_external_api__bittrex_global_otc",
            "measure_name_prefix": "bittrex_global_otc",
        },
        "cex_io": {  # Part 5: cex.io, DE-2443
            "secret_name": "airflow__connections__extract_stx_external_api__cex_io"
        },
    }

    for api_info in api_info_dict:
        if api_info == "binance_id":
            data = api_info_dict[api_info]

            insert_data_to_staging = PythonOperator(
                task_id=f"stage_{api_info}",
                python_callable=get_binance_data,
                op_kwargs={
                    "application": "CeFi_metrics",
                    "secret_name": data["secret_name"],
                    "process_id": "{{ task_instance_key_str }}",
                },
            )
        elif api_info == "crypto.com":  # PART 2
            data = api_info_dict[api_info]
            insert_data_to_staging = PythonOperator(
                task_id=f"stage_{api_info}",
                python_callable=get_crypto_com_data,
                op_kwargs={
                    "application": "CeFi_metrics",
                    "secret_name": data["secret_name"],
                    "process_id": "{{ task_instance_key_str }}",
                },
            )
        elif api_info == "coingecko.com":  # PART 3
            data = api_info_dict[api_info]
            insert_data_to_staging = PythonOperator(
                task_id=f"stage_{api_info}",
                python_callable=get_coingecko_com_data,
                op_kwargs={
                    "application": "crypto_metrics",
                    "ids": data["ids"],
                    "vs_currencies": data["vs_currencies"],
                    "process_id": "{{ task_instance_key_str }}",
                },
            )
        elif api_info in ["bittrex_global", "bittrex_global_otc"]:  # PART 4 & 6
            data = api_info_dict[api_info]
            insert_data_to_staging = PythonOperator(
                task_id=f"stage_{api_info}",
                python_callable=get_bittrex_global,
                op_kwargs={
                    "application": "CeFi_metrics",
                    "secret_name": data["secret_name"],
                    "measure_name_prefix": data["measure_name_prefix"],
                    "process_id": "{{ task_instance_key_str }}",
                },
            )

        elif api_info == "cex_io":  # PART 5
            data = api_info_dict[api_info]
            insert_data_to_staging = PythonOperator(
                task_id=f"stage_{api_info}",
                python_callable=get_cex_io_data,
                op_kwargs={
                    "application": "CeFi_metrics",
                    "secret_name": data["secret_name"],
                    "process_id": "{{ task_instance_key_str }}",
                },
            )

        remove_task_id = BigQueryOperator(
            task_id=f"remove_staged_{api_info}",
            gcp_conn_id=gcp_conn_id,
            sql="common/sql/bigquery/dml/remove_previous_staged_data.sql",
            use_legacy_sql=False,
            params={
                "stage_task_id": insert_data_to_staging.task_id,
                "GCP_PROJECT_ID": GCP_PROJECT_ID,
            },
        )

        validate_rows = BigQueryValueCheckOperator(
            task_id=f"validate_{api_info}_rows",
            gcp_conn_id=gcp_conn_id,
            sql="common/sql/bigquery/dml/validate_dimensions.sql",
            pass_value=0,
            use_legacy_sql=False,
            location=location,
            params={
                "stage_task_id": insert_data_to_staging.task_id,
                "GCP_PROJECT_ID": GCP_PROJECT_ID,
            },
        )

        commit_metrics = BigQueryOperator(
            task_id=f"commit_{api_info}",
            gcp_conn_id=gcp_conn_id,
            sql="common/sql/bigquery/dml/upsert_metrics.sql",
            use_legacy_sql=False,
            params={
                "stage_task_id": insert_data_to_staging.task_id,
                "GCP_PROJECT_ID": GCP_PROJECT_ID,
            },
        )

        (
            initial_node
            >> remove_task_id
            >> insert_data_to_staging
            >> validate_rows
            >> commit_metrics
        )
