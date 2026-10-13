select * from {{ ref('funnel_daily') }}
where sessions < 0 or page_view_sessions < 0 or view_item_sessions < 0
   or add_to_cart_sessions < 0 or checkout_sessions < 0 or purchase_sessions < 0
   or page_view_sessions > sessions or view_item_sessions > sessions
   or add_to_cart_sessions > sessions or checkout_sessions > sessions or purchase_sessions > sessions
