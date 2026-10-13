{% for model, field in [('funnel_daily','campaign_id'),('attribution_daily','campaign_id'),('campaign_performance_daily','campaign_id'),('customer_360','acquisition_campaign_id')] %}
select '{{ model }}' as relation, {{ field }} as campaign_id from {{ ref(model) }}
where {{ field }} is not null and {{ field }} != '__unattributed__'
  and {{ field }} not in (select campaign_id from {{ ref('campaigns') }})
{% if not loop.last %}union all{% endif %}
{% endfor %}
