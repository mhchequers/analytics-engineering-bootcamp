WITH

source as (
    SELECT * FROM {{ source('northwind', 'inventory_transaction_types') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source