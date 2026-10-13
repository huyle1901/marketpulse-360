{{ config(order_by='(event_date, campaign_id)') }}
select p.*,
       if(p.campaign_id = '__unattributed__', 'Unattributed', c.name) as campaign_name,
       c.status as campaign_status,
       toDecimal128(p.clicks, 6) / nullIf(toDecimal128(p.impressions, 6), 0) as ctr,
       toDecimal128(p.spend_minor, 6) / nullIf(toDecimal128(p.clicks, 6), 0) as cpc_minor
from {{ ref('campaign_performance_daily') }} p
left join {{ ref('campaigns') }} c on p.campaign_id = c.campaign_id
