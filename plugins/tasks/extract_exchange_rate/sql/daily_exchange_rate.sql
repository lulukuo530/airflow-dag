CREATE OR REPLACE TABLE `{{ params.GCP_PROJECT_ID }}.reference_datalake.dim_exchange_rate_daily`
  (
    trx_date DATE OPTIONS(description = "the cutoff date of the exchange rate value")
    , source_currency STRING OPTIONS(description = "source currency. Only contains USD before 2023-10-10")
    , target_currency STRING OPTIONS(description = "target currency. Only contains IDR and SGD before 2023-10-10")
    , exchange_rate FLOAT64 OPTIONS(description = "exchange rate sent by Workday. Soure website: https://currencylayer.com/")
    , etl_load_at_wib DATETIME OPTIONS(description = "data load time at wib timezone")
    , etl_load_at_utc DATETIME OPTIONS(description = "data load time at utc timezone")
  )
PARTITION BY trx_date 
OPTIONS (
  description = "The table update frequency is daily at 8am JKT time. The data is sent via Workday. Starts from 2023-10-10, we added source currencies other than USD."
) AS 
  SELECT * except (rnk)
  FROM 
    (-- using rank to keep the latest record. So backfill data will not create dupliactes.
    SELECT 
      trx_date
      , source_currency 
      , target_currency
      , exchange_rate  -- this will be in scientific notation. Need to use this for user to convert: format('%.8f', exchange_rate)
      , datetime(timestamp(etl_load_at_utc), 'Asia/Jakarta') AS etl_load_at_wib
      , etl_load_at_utc
      , row_number() over(partition by trx_date, source_currency || target_currency order by etl_load_at_utc desc) AS rnk 
    FROM `{{ params.GCP_PROJECT_ID }}.reference_datalake.dim_exchange_rate_daily`
    )
  WHERE rnk = 1 
