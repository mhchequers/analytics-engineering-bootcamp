{{ config(
    partition_by={
      "field": "order_date",
      "data_type": "date"
    }
)}}

WITH 

orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
),

order_details AS (
    SELECT *
    FROM {{ ref('stg_order_details') }}
),

joined AS (
    SELECT 
        od.order_id,
        od.product_id,
        o.customer_id,
        o.employee_id,
        o.shipper_id,
        od.quantity,
        od.unit_price,
        od.discount,
        od.status_id,
        od.date_allocated,
        od.purchase_order_id,
        od.inventory_id,
        DATE(o.order_date) AS order_date,
        o.shipped_date,
        o.paid_date,
        CURRENT_TIMESTAMP() AS insertion_timestamp,
    FROM orders o
    LEFT JOIN order_details od ON od.order_id = o.id
    WHERE od.order_id IS NOT NULL
),

unique_joined AS (
    SELECT *
    FROM joined 
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY    customer_id, 
                            employee_id, 
                            order_id, 
                            product_id, 
                            shipper_id, 
                            purchase_order_id, 
                            shipper_id, 
                            order_date
        ) = 1
)


SELECT *
FROM unique_joined