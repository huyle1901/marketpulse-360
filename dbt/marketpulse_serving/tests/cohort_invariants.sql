select * from {{ ref('cohort_retention') }}
where cohort_size <= 0 or active_profiles < 0 or active_profiles > cohort_size
   or month_number < 0 or month_number != dateDiff('month',cohort_month,activity_month)
   or retained_rate * 1000000 != intDiv(toInt128(active_profiles) * 1000000 + intDiv(cohort_size,2),cohort_size)
