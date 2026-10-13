select * from {{ ref('customer_360') }} where not consent_analytics
