{{ config(order_by='(event_date, product_id, device_type, currency)') }}
select * from {{ source('pinned_gold', 'raw_commerce_daily') }}
