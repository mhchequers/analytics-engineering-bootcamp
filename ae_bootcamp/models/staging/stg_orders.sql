WITH

source as (
    SELECT * FROM {{ source('northwind', 'orders') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source