with totals as (
  select sum(toInt128(gross_revenue_minor)) as gross,
         sum(toInt128(refund_amount_minor)) as refunds,
         sum(toInt128(net_revenue_minor)) as net
  from {{ ref('customer_360') }}
), allocations as (
  select model, sum(toInt128(allocated_gross_minor)) as gross,
         sum(toInt128(allocated_refund_minor)) as refunds,
         sum(toInt128(allocated_net_minor)) as net
  from {{ ref('attribution_daily') }} group by model
)
select a.model from allocations a cross join totals t
where a.gross != t.gross or a.refunds != t.refunds or a.net != t.net
union all
select expected from (select arrayJoin(['first_touch','last_touch','linear']) as expected)
where expected not in (select model from allocations)
  and (select count() from {{ ref('customer_360') }} where order_count > 0) > 0
