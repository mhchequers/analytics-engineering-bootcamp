WITH

source as (
    SELECT * FROM {{ source('northwind', 'privileges') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM source