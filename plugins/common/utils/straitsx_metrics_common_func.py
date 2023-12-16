import logging
import numpy
import os
import pandas as pd

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from decimal import Decimal, ROUND_HALF_UP
from google.cloud import bigquery

gcp_conn_id = "composer_worker"
airflow_environment = os.getenv("AIRFLOW_ENVIRONMENT")


def create_df(data_list) -> pd.DataFrame:
    """
    Create the dataframe with the format of d291209.xfers_datamart.metrics
    """
    df = pd.DataFrame(
        data_list,
        columns=[
            "period_start",
            "period_end",
            "application",
            "measure_name",
            "dimensions",
            "measure_value",
            "measure_count",
            "process_id",
            "processed_at",
        ],
    )
    return df


def insert_df_to_staging_metrics(df, process_id):
    """
    Detect the env and insert the data into xfers_datamart_stg in different projects, depending on the env.
    """
    if airflow_environment in ["local", "testing", "development"]:
        projet_id = "payfazz-data-development"
    else:
        projet_id = "d291209"

    dataset = "xfers_datamart_stg"
    table = "metrics"
    logging.info("Result will insert into %s.%s.%s", projet_id, dataset, table)

    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
    credentials = bq_hook.get_credentials()

    client = bigquery.Client(credentials=credentials)
    table_info = client.get_table(f"{projet_id}.{dataset}.{table}")
    generated_schema = [{"name": i.name, "type": i.field_type} for i in table_info.schema]

    destination_table = f"{dataset}.{table}"
    df["measure_value"] = df["measure_value"].apply(
        lambda x: Decimal(x).quantize(Decimal(".000000001"), rounding=ROUND_HALF_UP)
    )  # The line is meant to ensure that the value won't be too long. https://github.com/payfazz/data-composer/pull/93
    df["measure_count"] = numpy.nan
    df.to_gbq(
        project_id=projet_id,
        destination_table=destination_table,
        if_exists="append",
        table_schema=generated_schema,
        credentials=credentials,
    )
    logging.info(
        "Result inserted to %s.%s.%s, process_id is '%s'.", projet_id, dataset, table, process_id
    )
