{{ config(order_by='(cohort_month, activity_month, profile_type)') }}
select * from {{ source('pinned_gold', 'raw_cohort_retention') }}
