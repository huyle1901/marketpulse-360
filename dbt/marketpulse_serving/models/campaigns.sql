{{ config(order_by='campaign_id') }}
select * from {{ source('pinned_gold', 'raw_campaigns') }}
