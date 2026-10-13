{{ config(order_by='(model, event_date, campaign_id)') }}
select * from {{ source('pinned_gold', 'raw_attribution_daily') }}
