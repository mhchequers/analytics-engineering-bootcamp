WITH

source as (
    SELECT * FROM {{ source('northwind', 'purchase_orders') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source