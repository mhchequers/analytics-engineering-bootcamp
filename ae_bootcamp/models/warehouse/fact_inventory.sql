{{ config(
    partition_by={
      "field": "transaction_created_date",
      "data_type": "date"
    }
)}}

WITH 

unique_inventory AS (
    select
        id AS inventory_id,
        transaction_type,
        DATE(transaction_created_date) AS transaction_created_date,
        transaction_modified_date,
        product_id,
        quantity,
        purchase_order_id,
        customer_order_id,
        comments,
        CURRENT_TIMESTAMP() AS insertion_timestamp,
    FROM {{ ref('stg_inventory_transactions') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY inventory_id) = 1
)

SELECT *
FROM unique_inventory