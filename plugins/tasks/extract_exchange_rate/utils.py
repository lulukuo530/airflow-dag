import json
import logging
import pandas as pd
import pprint as pp
import pytz
from datetime import datetime
import re

import airflow.macros as macros
from google.cloud import storage
from fazzfinancial.common.utils import override_environment


def set_defaul_var(**kwargs):
    # gs://fazz-data-fdi/extracts/currency-exchange/dt=YYYY-MM-DD/YYYYMMDD-filename.json
    bucket_name = kwargs["bucket_name"]
    currency = kwargs["currency"]
    ddate = kwargs["data_interval_start"].strftime("%Y-%m-%d")
    execution_date = macros.ds_add(ddate, 1)  # YYYY-MM-DD
    ymd_format = macros.ds_format(execution_date, "%Y-%m-%d", "%Y%m%d")  # YYYYMMDD

    old_blob_name = f"{ymd_format}-CurrencyExchangeRates-{currency}.json"
    target_blob_name = f"extracts/currency-exchange/dt={execution_date}/{old_blob_name}"
    currency_info_dict = {
        "currency": currency,
        "execution_date": str(execution_date),
        "bucket_name": bucket_name,
        "old_blob_name": f"{ymd_format}-CurrencyExchangeRates-{currency}.json",
        "target_blob_name": f"extracts/currency-exchange/dt={execution_date}/{old_blob_name}",
    }

    print("Source blob name:", old_blob_name)
    print("Target path:", target_blob_name)

    # [notes]: Need to use this to set default var because we're getting execution_date {{ds}} from airflow template
    # If we don't pass the xcome here, it will be hard to backfill data
    kwargs["ti"].xcom_push(key=f"default_vars_{currency}", value=currency_info_dict)


def read_blob(bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    if blob.exists():
        with blob.open("r") as f:
            data = f.read()
    else:
        logging.warning(f"Blob {blob_name} does not exist.")

    return data


def write_blob_in_json(**kwargs):
    """
    bucket_name       is fazz-data-fdi
    old_blob_name     is the source bucket file path
    target_blob_name  is the destination bucket file path
    """
    ti = kwargs["ti"]
    currency = kwargs["currency"]
    default_vars = ti.xcom_pull(
        task_ids=f"set_defaul_var_{currency}", key=f"default_vars_{currency}"
    )

    execution_date = default_vars["execution_date"]
    bucket_name = default_vars["bucket_name"]
    old_blob_name = default_vars["old_blob_name"]
    target_blob_name = default_vars["target_blob_name"]

    data = read_blob(bucket_name, old_blob_name)
    logging.info(f"Reading data from {old_blob_name} completed.")

    # proposed json file path
    # gs://fazz-data-fdi/extracts/currency-exchange/dt=YYYY-MM-DD/YYYYMMDD-filename-EUR.json
    prefix = "extracts/currency-exchange"
    new_folder_path = f"{prefix}/dt={execution_date}/"

    # This is to create an empty folder
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(new_folder_path)
    blob.upload_from_string("")
    logging.info(f"Created folder {new_folder_path}")

    # Copy data to new folder
    logging.info(f"Uploading data to {new_folder_path}")
    blob_n = bucket.blob(target_blob_name)
    blob_n.upload_from_string(data, content_type="application/json")
    logging.info("Upload completed.")


def delete_blob(**kwargs):
    ti = kwargs["ti"]
    currency = kwargs["currency"]
    default_vars = ti.xcom_pull(
        task_ids=f"set_defaul_var_{currency}", key=f"default_vars_{currency}"
    )

    bucket_name = default_vars["bucket_name"]
    blob_name = default_vars["old_blob_name"]

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Check if the blob exists before attempting to delete
    if blob.exists():
        # Optional: set a generation-match precondition to avoid potential race conditions
        # and data corruptions. The request to delete is aborted if the object's
        # generation number does not match your precondition.
        blob.reload()  # Fetch blob metadata to use in generation_match_precondition.
        generation_match_precondition = blob.generation
        blob.delete(if_generation_match=generation_match_precondition)
        logging.info(f"Blob {blob_name} deleted.")
    else:
        logging.warning(f"Blob {blob_name} does not exist.")


def list_blobs(bucket_name, check_num, **kwargs):
    storage_client = storage.Client()
    # Note: Client.list_blobs requires at least package version 1.17.0.

    ddate = kwargs["data_interval_start"].strftime("%Y-%m-%d")
    execution_date = macros.ds_add(ddate, 1)  # YYYY-MM-DD
    blobs = storage_client.list_blobs(
        bucket_name, prefix=f"extracts/currency-exchange/dt={execution_date}"
    )
    filter_txt = "CurrencyExchangeRates"
    cnt = 0
    for blob in blobs:
        if filter_txt in blob.name:
            print(blob.name)
            cnt += 1
    if cnt != check_num:
        raise ValueError(
            f"The number of files are not correct. Should be {check_num}. Only have {cnt} now."
        )


def check_workday_files(bucket_name, **kwargs):
    storage_client = storage.Client()
    # Note: Client.list_blobs requires at least package version 1.17.0.

    ddate = kwargs["data_interval_start"].strftime("%Y-%m-%d")
    execution_date = macros.ds_add(ddate, 1)  # YYYY-MM-DD
    ymd_format = macros.ds_format(execution_date, "%Y-%m-%d", "%Y%m%d")  # YYYYMMDD
    blobs = storage_client.list_blobs(bucket_name)
    pattern = "(\d*)-"

    idx = 0
    logging.info(f"Today's date: {ymd_format}")
    for blob in blobs:
        if "extracts/" not in blob.name:
            search_result = re.search(pattern, blob.name)
            if search_result.group(1) == ymd_format:
                print(blob.name)
            else:
                logging.warning(f"Wrong date for filename: {blob.name}.")
                idx += 1
    if idx > 0:
        raise ValueError("Workday sent the files in wrong format.")


def insert_to_bq_table(**kwargs):
    ti = kwargs["ti"]
    currency = kwargs["currency"]
    default_vars = ti.xcom_pull(
        task_ids=f"set_defaul_var_{currency}", key=f"default_vars_{currency}"
    )

    source_currency = currency
    bucket_name = default_vars["bucket_name"]
    target_blob_name = default_vars["target_blob_name"]
    bucket_name = default_vars["bucket_name"]

    currency_list = kwargs["currency_list"]  # this is taken from original CURRENCY_LIST

    data = json.loads(read_blob(bucket_name, target_blob_name))
    logging.info(f"Loading data from {target_blob_name} completed.")

    extract_timestamp = data.get("timestamp")
    extract_date = datetime.fromtimestamp(extract_timestamp).date()

    currency_data = []
    for target_curency in currency_list:
        if source_currency != target_curency:  # exculde things like EUREUR
            target_currency_list = {}
            target_currency_list["trx_date"] = extract_date
            target_currency_list["source_currency"] = source_currency
            target_currency_list["target_currency"] = target_curency
            target_currency_list["exchange_rate"] = data.get("quotes").get(
                f"{source_currency}{target_curency}"
            )
            target_currency_list["etl_load_at_wib"] = datetime.now(pytz.timezone("Asia/Jakarta"))
            target_currency_list["etl_load_at_utc"] = datetime.now(pytz.timezone("UTC"))
            currency_data.append(target_currency_list)

    pp.pprint(currency_data)
    logging.info(f"Exchange rate for source currency in {source_currency} completed.")
    insert_data = pd.DataFrame.from_records(currency_data)
    insert_data = insert_data.convert_dtypes()
    insert_data["trx_date"] = pd.to_datetime(insert_data["trx_date"])

    schema_1d = [
        {"name": "trx_date", "type": "DATE"},
        {"name": "source_currency", "type": "STRING"},
        {"name": "target_currency", "type": "STRING"},
        {"name": "exchange_rate", "type": "FLOAT64"},
        {"name": "etl_load_at_wib", "type": "DATETIME"},
        {"name": "etl_load_at_utc", "type": "DATETIME"},
    ]
    logging.info("Starting inserting data...")
    insert_data.to_gbq(
        destination_table="reference_datalake.dim_exchange_rate_daily",
        project_id=override_environment(value_prod="d291209", value_dev="payfazz-data-development"),
        table_schema=schema_1d,
        chunksize=365,
        location="asia-southeast2",
        if_exists="append",
    )
    logging.info("Inserting data completed.")
