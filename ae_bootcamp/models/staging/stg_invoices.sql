WITH

source as (
    SELECT * FROM {{ source('northwind', 'invoices') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source