{{ config(order_by='(event_date, campaign_id)') }}
select * from {{ source('pinned_gold', 'raw_campaign_performance_daily') }}
