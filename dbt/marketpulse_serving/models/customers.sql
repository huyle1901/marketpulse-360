{{ config(order_by='customer_id') }}
select * from {{ source('pinned_gold', 'raw_customers') }}
