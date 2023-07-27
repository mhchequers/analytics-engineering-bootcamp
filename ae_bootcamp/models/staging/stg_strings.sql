WITH

source as (
    SELECT * FROM {{ source('northwind', 'strings') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source