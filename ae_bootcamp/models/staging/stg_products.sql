WITH

source AS (
    SELECT * 
    FROM {{ source('northwind', 'products') }}
),

transformed AS (
    SELECT 
        CAST(supplier_ids AS INTEGER) AS supplier_id,
        * EXCEPT(supplier_ids)
    FROM source 
    WHERE supplier_ids NOT LIKE '%;%'
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS ingestion_timestamp
FROM transformed