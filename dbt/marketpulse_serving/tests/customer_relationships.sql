select * from {{ ref('customer_360') }}
where (profile_type = 'customer' and (customer_id is null or customer_id not in (select customer_id from {{ ref('customers') }})))
   or (profile_type != 'customer' and customer_id is not null)
   or (profile_type = 'anonymous' and anonymous_id is null)
