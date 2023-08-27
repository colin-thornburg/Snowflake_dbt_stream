{{
config(
    materialized = 'dynamic_table',
    snowflake_warehouse = 'VHOL_CDC_WH',
    target_lag = '20 days',
    on_configuration_change = 'apply',
)
}}

SELECT * EXCLUDE (score,action) from (
  SELECT
    RECORD_CONTENT:transaction:primaryKey_tokenized::varchar as orderid_tokenized,
    RECORD_CONTENT:transaction:record_after:orderid_encrypted::varchar as orderid_encrypted,
    TO_TIMESTAMP_NTZ(RECORD_CONTENT:transaction:committed_at::number/1000) as lastUpdated,
    RECORD_CONTENT:transaction:action::varchar as action,
    RECORD_CONTENT:transaction:record_after:client::varchar as client,
    RECORD_CONTENT:transaction:record_after:ticker::varchar as ticker,
    RECORD_CONTENT:transaction:record_after:LongOrShort::varchar as position,
    RECORD_CONTENT:transaction:record_after:Price::number(38,3) as price,
    RECORD_CONTENT:transaction:record_after:Quantity::number(38,3) as quantity,
    RANK() OVER (
        partition by orderid_tokenized order by RECORD_CONTENT:transaction:committed_at::number desc) as score
  FROM {{ source('stream_source', 'CDC_STREAMING_TABLE') }}
    WHERE
        RECORD_CONTENT:transaction:schema::varchar='PROD' AND RECORD_CONTENT:transaction:table::varchar='LIMIT_ORDERS'
)
WHERE score = 1 and action != 'DELETE'

--538,512