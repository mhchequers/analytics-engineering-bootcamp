WITH

source as (
    SELECT * FROM {{ source('northwind', 'suppliers') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source