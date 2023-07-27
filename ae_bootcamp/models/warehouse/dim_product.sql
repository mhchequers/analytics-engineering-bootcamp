WITH 

products AS (
    SELECT
        p.id AS product_id,				
        p.product_code,				
        p.product_name,				
        p.description,	
        s.company AS supplier_company,			
        p.standard_cost,				
        p.list_price,				
        p.reorder_level,				
        p.target_level,			
        p.quantity_per_unit,				
        p.discontinued,				
        p.minimum_reorder_quantity,				
        p.category,			
        p.attachments,				
        p.ingestion_timestamp,
        CURRENT_TIMESTAMP() AS insertion_timestamp
    FROM {{ ref('stg_products') }} p
    LEFT JOIN {{ ref('stg_suppliers') }} s ON s.id = p.supplier_id
),

unique_products AS (
    SELECT *
    FROM products
    QUALIFY row_number() OVER (PARTITION BY product_id) = 1
)

SELECT *
FROM unique_products