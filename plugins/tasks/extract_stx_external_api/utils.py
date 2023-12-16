import logging
import hashlib
import hmac
import json
import math
import os
import pandas as pd
import pendulum
import requests
import time
import urllib

from pprint import pprint
from fazzfinancial.common.utils.get_gcp_credentials import get_credentials_from_secret_manager
from fazzfinancial.common.utils.straitsx_metrics_common_func import (
    create_df,
    insert_df_to_staging_metrics,
)

gcp_conn_id = "composer_worker"
airflow_environment = os.getenv("AIRFLOW_ENVIRONMENT")


def get_binance_user_asset(api_key, api_secret):
    base_url = "https://api.binance.com"
    endpoint = "/sapi/v3/asset/getUserAsset"

    # Prepare the request parameters
    params = {"timestamp": int(time.time() * 1000)}

    # Generate the signature for the request
    query_string = urllib.parse.urlencode(params)
    signature = hmac.new(
        api_secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    params["signature"] = signature

    # Set the headers
    headers = {"content-type": "application/json", "X-MBX-APIKEY": api_key}

    # Send the request
    response = requests.post(base_url + endpoint, params=params, headers=headers)
    response.raise_for_status()

    return response.json()


def get_binance_data(**kwargs):
    secret_name = kwargs["secret_name"]
    application = kwargs["application"]
    process_id = kwargs["process_id"]

    cred = json.loads(get_credentials_from_secret_manager(secret_name))
    api_key = cred["api-key"]
    api_secret = cred["secret-key"]
    data = get_binance_user_asset(api_key, api_secret)

    data_list = []
    for item in data:
        processed_at = pd.to_datetime(pendulum.now().to_datetime_string())
        measure_name = f"binance_id_{item['asset']}"
        dimensions = json.dumps(item)

        data_list.append(
            [
                processed_at,
                processed_at,
                application,
                measure_name,
                dimensions,
                0,
                None,
                process_id,
                processed_at,
            ]
        )

    df = create_df(data_list)
    insert_df_to_staging_metrics(df, process_id)
    logging.info("Insert data to stg table completed.")


def get_crypto_com_data(**kwargs):
    application = kwargs["application"]
    secret_name = kwargs["secret_name"]
    process_id = kwargs["process_id"]

    credentials = json.loads(get_credentials_from_secret_manager(secret_name))
    api_key = credentials["api-key"]
    secret_key = credentials["secret-key"]
    # Start generating sig
    req = {
        "id": math.ceil(time.time()),
        "method": "private/user-balance",
        "api_key": api_key,
        "params": {},
        "nonce": int(time.time() * 1000),
    }

    # First ensure the params are alphabetically sorted by key
    param_str = ""

    MAX_LEVEL = 3

    def params_to_str(obj, level):
        if level >= MAX_LEVEL:
            return str(obj)

        return_str = ""
        for key in sorted(obj):
            return_str += key
            if obj[key] is None:
                return_str += "null"
            elif isinstance(obj[key], list):
                for subObj in obj[key]:
                    return_str += params_to_str(subObj, ++level)
            else:
                return_str += str(obj[key])
        return return_str

    if "params" in req:
        param_str = params_to_str(req["params"], 0)

    payload_str = req["method"] + str(req["id"]) + req["api_key"] + param_str + str(req["nonce"])

    req["sig"] = hmac.new(
        bytes(str(secret_key), "utf-8"), msg=bytes(payload_str, "utf-8"), digestmod=hashlib.sha256
    ).hexdigest()
    # End generating sig
    logging.info("Requst body:")
    pprint(req)

    headers = {"content-type": "application/json"}
    response = requests.post(
        "https://api.crypto.com/exchange/v1/private/user-balance", json=req, headers=headers
    )
    logging.info("Response:")
    pprint(response.json())
    data = response.json()["result"]["data"][0]["position_balances"]

    data_list = []
    for item in data:
        processed_at = pd.to_datetime(pendulum.now().to_datetime_string())
        measure_name = f"crypto.com_{item['instrument_name']}_quantity"
        dimensions = json.dumps(item)

        data_list.append(
            [
                processed_at,
                processed_at,
                application,
                measure_name,
                dimensions,
                str(item["quantity"]),
                None,
                process_id,
                processed_at,
            ]
        )
    df = create_df(data_list)
    insert_df_to_staging_metrics(df, process_id)


def get_coingecko_com_data(**kwargs):
    application = kwargs["application"]
    ids = ",".join(kwargs["ids"])
    vs_currencies = kwargs["vs_currencies"]
    process_id = kwargs["process_id"]

    request = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": ids, "vs_currencies": vs_currencies}
    headers = {"accept": "application/json"}
    logging.info("Requsting: %s", request)
    logging.info("Params:")
    pprint(params)
    session = requests.session()
    response = session.get(request, params=params, headers=headers)
    logging.info("********** Original Response:")
    pprint(response.json())

    data_list = []
    data = response.json()
    processed_at = pd.to_datetime(pendulum.now().to_datetime_string())
    measure_name = "coingecko"
    # start format
    dimensions = []
    for key in data:
        data[key]["asset"] = key
        dimensions.append(data[key])
    # end format
    logging.info("********** New format for bigquery:")
    pprint(dimensions)
    dimensions = str(dimensions)

    data_list.append(
        [
            processed_at,
            processed_at,
            application,
            measure_name,
            dimensions,
            str(0),
            None,
            process_id,
            processed_at,
        ]
    )

    df = create_df(data_list)
    insert_df_to_staging_metrics(df, process_id)


def bittrex_auth(uri, method, payload, apiKey, apiSecret):
    """Ref: https://algotrading101.com/learn/bittrex-api-guide/"""
    timestamp = str(round(time.time() * 1000))
    contentHash = hashlib.sha512(payload).hexdigest()

    array = [timestamp, uri, method, contentHash]
    s = ""
    preSign = s.join(str(v) for v in array)
    signature = hmac.new(apiSecret.encode(), preSign.encode(), hashlib.sha512).hexdigest()

    headers = {
        "Accept": "application/json",
        "Api-Key": apiKey,
        "Api-Timestamp": timestamp,
        "Api-Content-Hash": contentHash,
        "Api-Signature": signature,
        "Content-Type": "application/json",
    }

    return headers


def get_bittrex_global(**kwargs):
    application = kwargs["application"]
    secret_name = kwargs["secret_name"]
    measure_name_prefix = kwargs["measure_name_prefix"]
    process_id = kwargs["process_id"]

    credentials = json.loads(get_credentials_from_secret_manager(secret_name))
    api_key = credentials["api-key"]
    secret_key = credentials["secret-key"]

    uri = "https://api.bittrex.com/v3/balances"
    payload = ""

    response = requests.get(
        uri,
        data=payload,
        headers=bittrex_auth(uri, "GET", payload.encode(), api_key, secret_key),
    )
    logging.info("Response:")
    data = response.json()
    pprint(data)

    data_list = []
    for item in data:
        processed_at = pd.to_datetime(pendulum.now().to_datetime_string())
        measure_name = f"{measure_name_prefix}_{item['currencySymbol']}"
        dimensions = json.dumps(item)

        data_list.append(
            [
                processed_at,
                processed_at,
                application,
                measure_name,
                dimensions,
                str(0),
                None,
                process_id,
                processed_at,
            ]
        )

    df = create_df(data_list)
    insert_df_to_staging_metrics(df, process_id)


def get_cex_io_account_balance(api_key, api_secret, user_id):
    base_url = "https://cex.io/api/balance/"

    timestamp = int(time.time() * 1000)
    string = "{}{}{}".format(timestamp, user_id, api_key)
    signature = hmac.new(api_secret.encode(), string.encode(), hashlib.sha256).hexdigest()

    params = {
        "key": api_key,
        "signature": signature,
        "nonce": timestamp,
    }

    headers = {"Content-Type": "application/json"}

    response = requests.post(base_url, data=json.dumps(params), headers=headers)
    response.raise_for_status()
    return response.json()


def get_cex_io_data(**kwargs):
    secret_name = kwargs["secret_name"]
    application = kwargs["application"]
    process_id = kwargs["process_id"]

    cred = json.loads(get_credentials_from_secret_manager(secret_name))
    api_key = cred["api-key"]
    api_secret = cred["secret-key"]
    user_id = cred["user_id"]
    data = get_cex_io_account_balance(api_key, api_secret, user_id)
    logging.info("extracting api completed...")

    data_list = []
    for name in data:
        if name != "timestamp" and name != "username":
            if float(data.get(name)["available"]) + float(data.get(name)["orders"]) > 0:
                processed_at = pd.to_datetime(pendulum.now().to_datetime_string())
                measure_name = f"cex_io_{name}"
                dimensions = json.dumps(data.get(name))
                print(measure_name, ":", data.get(name))

                data_list.append(
                    [
                        processed_at,
                        processed_at,
                        application,
                        measure_name,
                        dimensions,
                        0,
                        None,
                        process_id,
                        processed_at,
                    ]
                )

    df = create_df(data_list)
    insert_df_to_staging_metrics(df, process_id)
    logging.info("Insert data to stg table completed.")
