WITH

source as (
    SELECT * FROM {{ source('northwind', 'orders_status') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source