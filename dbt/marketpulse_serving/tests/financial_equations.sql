select 'customer' as relation, profile_key as key
from {{ ref('customer_360') }}
where toInt128(gross_revenue_minor) - refund_amount_minor != net_revenue_minor
union all
select 'attribution', campaign_id from {{ ref('attribution_daily') }}
where toInt128(allocated_gross_minor) - allocated_refund_minor != allocated_net_minor
union all
select 'performance', campaign_id from {{ ref('campaign_performance_daily') }}
where toInt128(gross_revenue_minor) - refund_amount_minor != net_revenue_minor
{% for prefix in ['first_touch', 'last_touch', 'linear'] %}
   or toInt128({{ prefix }}_gross_minor) - {{ prefix }}_refund_minor != {{ prefix }}_net_minor
{% endfor %}
