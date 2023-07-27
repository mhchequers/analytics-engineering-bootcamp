WITH

source as (
    SELECT * FROM {{ source('northwind', 'order_details') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source