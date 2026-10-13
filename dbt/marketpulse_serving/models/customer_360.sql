{{ config(order_by='profile_key') }}
select * from {{ source('pinned_gold', 'raw_customer_360') }}
