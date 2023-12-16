
CREATE OR REPLACE TABLE `{{ params.GCP_PROJECT_ID }}.reference_datalake.dim_exchange_rate_monthly`
  (
    trx_month DATE OPTIONS(description = "the first day of each month")
    , source_currency STRING OPTIONS(description = "source currency. Only contains USD before 2023-10-10")
    , target_currency STRING OPTIONS(description = "target currency. Only contains IDR and SGD before 2023-10-10")
    , avg_exchange_rate FLOAT64 OPTIONS(description = "average value of exchange rate in each month")
    , etl_load_at_wib DATETIME OPTIONS(description = "data load time at wib timezone")
    , etl_load_at_utc DATETIME OPTIONS(description = "data load time at utc timezone")
  )
PARTITION BY trx_month 
OPTIONS (
  description = "Calculate the monthly average of exchange rate from reference_datalake.dim_exchange_rate_daily"
) AS 
  SELECT 
    date_trunc(trx_date, MONTH) AS trx_month 
    , source_currency 
    , target_currency 
    , sum(exchange_rate) / count(distinct trx_date) AS avg_exchange_rate
    , current_datetime('Asia/Jakarta') AS etl_load_at_wib
    , current_datetime() AS etl_load_at_utc
  FROM `{{ params.GCP_PROJECT_ID }}.reference_datalake.dim_exchange_rate_daily`
  GROUP BY 1,2,3 
