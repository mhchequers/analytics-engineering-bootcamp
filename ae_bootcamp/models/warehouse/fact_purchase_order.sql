{{ config(
    partition_by={
      "field": "creation_date",
      "data_type": "date"
    }
)}}

WITH 

purchase_orders AS (
    SELECT *
    FROM {{ ref('stg_purchase_orders') }}
),

purchase_order_details AS (
    SELECT *
    FROM {{ ref('stg_purchase_order_details') }}
),

products AS (
    SELECT *
    FROM {{ ref('stg_products') }}
),

order_details AS (
    SELECT *
    FROM {{ ref('stg_order_details') }}
),

orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
),

employees AS (
    SELECT *
    FROM {{ ref('stg_employees') }}
),

customer AS (
    SELECT *
    FROM {{ ref('stg_customer') }}
),

joined AS(
    select
        c.id AS customer_id,
        e.id AS employee_id,
        pod.purchase_order_id,
        pod.product_id,
        pod.quantity,
        pod.unit_cost,
        pod.date_received,
        pod.posted_to_inventory,
        pod.inventory_id,
        po.supplier_id,
        po.created_by,
        po.submitted_date,
        DATE(po.creation_date) AS creation_date,
        po.status_id,
        po.expected_date,
        po.shipping_fee,
        po.taxes,
        po.payment_date,
        po.payment_amount,
        po.payment_method,
        po.notes,
        po.approved_by,
        po.approved_date,
        po.submitted_by,
        CURRENT_TIMESTAMP() AS insertion_timestamp
    FROM purchase_orders po
    LEFT JOIN purchase_order_details pod ON pod.purchASe_order_id = po.id
    LEFT JOIN products p ON p.id = pod.product_id
    LEFT JOIN order_details od ON od.product_id = p.id
    LEFT JOIN orders o ON o.id = od.order_id
    LEFT JOIN employees e ON e.id = po.created_by
    LEFT JOIN customer c ON c.id = o.customer_id
    WHERE o.customer_id IS NOT NULL
),

unique_joined AS (
    SELECT *
    FROM joined 
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY    customer_id, 
                            employee_id, 
                            purchase_order_id, 
                            product_id, 
                            inventory_id, 
                            supplier_id, 
                            creation_date
        ) = 1
)

SELECT * 
FROM unique_joined