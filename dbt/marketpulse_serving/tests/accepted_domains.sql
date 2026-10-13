select 'campaigns' as relation, platform as invalid_value from {{ ref('campaigns') }}
where platform not in ('google','meta','tiktok')
union all
select 'performance', platform from {{ ref('campaign_performance_daily') }}
where platform is not null and platform not in ('google','meta','tiktok')
union all
select 'cohort',profile_type from {{ ref('cohort_retention') }}
where profile_type not in ('customer','anonymous','session')
