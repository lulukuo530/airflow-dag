import os
import requests
import datetime as dt
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz
import json
import pandas as pd
import pandas_gbq
import logging

target_key = "a$%^24G$jBU0f09sfm$$"  # This is a fake key. Need to get the right key from the API or Secret Manager
destination_table = "reference_datalake.dim_exchange_rate_daily_archive"


def get_data_from_api(start_date, end_date, target_key=target_key):
    ##-- 0. Extract Data from API --##
    ## https://apilayer.com/marketplace/currency_data-api
    ## Because API layer can only extract maximum 365 days data, need to separate into two years
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    currency = "IDR,SGD,USD"
    url = f"https://api.apilayer.com/currency_data/timeframe?start_date={start_date_str}&end_date={end_date_str}&currencies={currency}"

    payload = {}

    headers = {"apikey": target_key}  # monthly quota 100
    response = requests.request("GET", url, headers=headers, data=payload)
    status_code = response.status_code
    result = response.text
    result_json = json.loads(result)
    return result_json


def sort_df(day_df, source_df, target_df, currency):
    target_df = pd.DataFrame([day_df, source_df]).transpose()
    target_df.columns = ["trx_date", "exchange_rate"]
    target_df["currency"] = currency
    target_df = target_df[["trx_date", "currency", "exchange_rate"]]
    return target_df


def json_clean_up(result_json):
    # -- generate date sequence
    day_list = []
    for day in result_json.get("quotes"):
        day_list.append(day)

    # -- get currency from IDR & SGD
    idr_list = []
    idr_idx = []
    sgd_list = []
    sgd_idx = []
    for k in result_json.get("quotes").keys():
        idr_list.append(result_json.get("quotes").get(k).get("USDIDR"))
        idr_idx.append("IDR")
        sgd_list.append(result_json.get("quotes").get(k).get("USDSGD"))
        sgd_idx.append("SGD")

    # -- create df: trx_date, currency, exchange_rate
    idr_df_1d = pd.DataFrame()
    idr_df_1d = sort_df(day_list, idr_list, idr_df_1d, "IDR")

    sgd_df_1d = pd.DataFrame()
    sgd_df_1d = sort_df(day_list, sgd_list, sgd_df_1d, "SGD")

    # -- combine daily tables
    daily_df = pd.concat([idr_df_1d, sgd_df_1d]).sort_values("trx_date")
    daily_df = daily_df.reset_index(drop=True)
    daily_df = daily_df.convert_dtypes()
    return daily_df


def ingest_to_bq(raw_1d, destination_table=destination_table):
    raw_1d["etl_load_at_wib"] = datetime.now(pytz.timezone("Asia/Jakarta"))
    raw_1d["etl_load_at_utc"] = datetime.now(pytz.timezone("UTC"))

    # -- insert data into BQ. PLEASE DO NOT CHANGE ORIGINAL TABLE DATA TYPE. OTHERWISE THE SCHEMA WILL FAIL
    schema_1d = [
        {"name": "trx_date", "type": "STRING"},
        {"name": "currency", "type": "STRING"},
        {"name": "exchange_rate", "type": "FLOAT64"},
        {"name": "etl_load_at_wib", "type": "DATETIME"},
        {"name": "etl_load_at_utc", "type": "DATETIME"},
    ]
    logging.info("Starting insert daily table...")
    raw_1d.to_gbq(
        destination_table=destination_table,
        project_id="d291209",
        table_schema=schema_1d,
        chunksize=365,
        location="asia-southeast2",
        if_exists="append",
    )


# -- MANUALLY BACKFILL EXCHANGE RATE TABLE --#
# maximum can only extract 365 days from the API
json_2016 = get_data_from_api(dt.date(2016, 1, 1), dt.date(2016, 12, 31))
json_2017 = get_data_from_api(dt.date(2017, 1, 1), dt.date(2017, 12, 31))
json_2018 = get_data_from_api(dt.date(2018, 1, 1), dt.date(2018, 12, 31))
json_2019 = get_data_from_api(dt.date(2019, 1, 1), dt.date(2019, 12, 31))
json_2020 = get_data_from_api(dt.date(2020, 1, 1), dt.date(2020, 12, 31))
json_2021 = get_data_from_api(dt.date(2021, 1, 1), dt.date(2021, 12, 31))
json_2022 = get_data_from_api(dt.date(2022, 1, 1), dt.date(2022, 12, 31))
json_2023 = get_data_from_api(dt.date(2023, 1, 1), dt.date(2023, 5, 9))

data_list = [json_2016, json_2017, json_2018, json_2019, json_2020, json_2021, json_2022, json_2023]

for data in data_list:
    ingest_data = json_clean_up(data)
    ingest_to_bq(ingest_data)
    print("Finish ingesting data in ", ingest_data["trx_date"][0])

# -- Run this in BQ after the backfill is complete
"""
CREATE OR REPLACE TABLE d291209.reference_datalake.dim_exchange_rate_daily_archive
OPTIONS (description = "Please DO NOT DELETE this table. This is the archive data for reference_datalake.dim_exchange_rate_daily.")
AS 
SELECT 
  trx_date
  , currency
  , exchange_rate 
  , datetime(timestamp(etl_load_at_utc), 'Asia/Jakarta') AS etl_load_at_wib
  , etl_load_at_utc
FROM d291209.reference_datalake.dim_exchange_rate_daily_archive
ORDER BY 1,2 
"""
