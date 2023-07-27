WITH

source as (
    SELECT * FROM {{ source('northwind', 'customer') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source