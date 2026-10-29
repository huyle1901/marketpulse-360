-- Product/device commerce must reconcile exactly with certified campaign finance and stay in known domains.
with commerce as (
    select currency, sum(toInt128(gross_revenue_minor)) as gross, sum(toInt128(refund_amount_minor)) as refund,
           sum(toInt128(net_revenue_minor)) as net
    from {{ ref('commerce_daily') }} group by currency
), performance as (
    select currency, sum(toInt128(gross_revenue_minor)) as gross, sum(toInt128(refund_amount_minor)) as refund,
           sum(toInt128(net_revenue_minor)) as net
    from {{ ref('campaign_performance_daily') }} group by currency
)
select 'totals' as check_name, coalesce(c.currency, p.currency) as key
from commerce c full outer join performance p on c.currency = p.currency
where coalesce(c.gross, 0) != coalesce(p.gross, 0) or coalesce(c.refund, 0) != coalesce(p.refund, 0)
   or coalesce(c.net, 0) != coalesce(p.net, 0)
union all
select 'equation', product_id from {{ ref('commerce_daily') }}
where toInt128(gross_revenue_minor) - refund_amount_minor != net_revenue_minor
union all
select 'domain', product_id from {{ ref('commerce_daily') }}
where device_type not in ('desktop', 'mobile', 'tablet', 'unknown')
   or category not in ('electronics', 'home', 'beauty', 'fashion', 'sports')
   or orders < 0 or units < 0 or refunds < 0
