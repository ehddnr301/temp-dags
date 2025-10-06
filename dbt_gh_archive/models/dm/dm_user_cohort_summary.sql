{{ config(
  materialized='table',
  on_table_exists='replace',
  properties={
    "location": "'s3://gh-archive-delta/dm/dm_user_cohort_summary'",
    "partitioned_by": "ARRAY['cohort_month']"
  }
) }}


-- DM: 월별 코호트 요약 테이블
-- 목적: 코호트별 비교 및 신규 유입 품질 분석
-- 소스: dm_user_retention
-- Materialization: table (전체 재계산)

with retention_base as (
  select
    cohort_date,
    organization,
    repo_name,
    days_since_cohort,
    cohort_size,
    cohort_activity_type,
    retention_rate_any,
    retention_rate_light,
    retention_rate_medium,
    retention_rate_heavy
  from {{ ref('dm_user_retention') }}
)

-- 월별 코호트 정의
, cohort_monthly as (
  select
    date_format(cohort_date, '%Y-%m') as cohort_month,
    organization,
    repo_name,
    cohort_activity_type,
    cohort_date,
    cohort_size
  from retention_base
  where days_since_cohort = 1  -- 코호트 크기를 한 번만 가져오기 위해 임의의 시점 선택
  group by cohort_date, organization, repo_name, cohort_activity_type, cohort_size
)

-- Day 1/7/30 Retention 추출
, retention_pivot as (
  select
    date_format(cohort_date, '%Y-%m') as cohort_month,
    organization,
    repo_name,
    cohort_activity_type,
    cohort_date,
    max(case when days_since_cohort = 1 then retention_rate_any end) as day1_retention,
    max(case when days_since_cohort = 7 then retention_rate_any end) as day7_retention,
    max(case when days_since_cohort = 14 then retention_rate_any end) as day14_retention,
    max(case when days_since_cohort = 30 then retention_rate_any end) as day30_retention,
    max(case when days_since_cohort = 60 then retention_rate_any end) as day60_retention,
    max(case when days_since_cohort = 90 then retention_rate_any end) as day90_retention
  from retention_base
  group by cohort_date, organization, repo_name, cohort_activity_type
)

-- 사용자별 활동 통계 (전체 기간) - user 레벨
, user_activity_stats_raw as (
  select
    organization,
    repo_name,
    user_login,
    count(distinct base_date) as active_days_count,
    sum(event_count) as total_events_count,
    min(base_date) as first_activity_date,
    max(base_date) as last_activity_date,
    date_diff('day', min(base_date), max(base_date)) as lifetime_days
  from {{ ref('dw_activity_daily') }}
  where organization is not null
    and repo_name is not null
    and user_login is not null
  group by organization, repo_name, user_login
)

-- 월별 집계로 변환 (cohort_month, org, repo 레벨)
, user_activity_stats as (
  select
    date_format(first_activity_date, '%Y-%m') as cohort_month,
    organization,
    repo_name,
    avg(active_days_count) as avg_active_days_per_user,
    avg(total_events_count) as avg_events_per_user,
    avg(lifetime_days) as avg_lifetime_days
  from user_activity_stats_raw
  group by date_format(first_activity_date, '%Y-%m'), organization, repo_name
)

-- 월별 코호트 집계
, final as (
  select
    c.cohort_month,
    c.organization,
    c.repo_name,
    
    -- 코호트 기본 정보
    sum(c.cohort_size) as cohort_size,
    sum(case when c.cohort_activity_type = 'Light' then c.cohort_size else 0 end) as light_users_count,
    sum(case when c.cohort_activity_type = 'Medium' then c.cohort_size else 0 end) as medium_users_count,
    sum(case when c.cohort_activity_type = 'Heavy' then c.cohort_size else 0 end) as heavy_users_count,
    
    -- Retention 요약 (가중 평균)
    case 
      when sum(c.cohort_size) > 0 
      then sum(coalesce(r.day1_retention, 0) * c.cohort_size) / sum(c.cohort_size)
      else null
    end as day1_retention,
    case 
      when sum(c.cohort_size) > 0 
      then sum(coalesce(r.day7_retention, 0) * c.cohort_size) / sum(c.cohort_size)
      else null
    end as day7_retention,
    case 
      when sum(c.cohort_size) > 0 
      then sum(coalesce(r.day14_retention, 0) * c.cohort_size) / sum(c.cohort_size)
      else null
    end as day14_retention,
    case 
      when sum(c.cohort_size) > 0 
      then sum(coalesce(r.day30_retention, 0) * c.cohort_size) / sum(c.cohort_size)
      else null
    end as day30_retention,
    case 
      when sum(c.cohort_size) > 0 
      then sum(coalesce(r.day60_retention, 0) * c.cohort_size) / sum(c.cohort_size)
      else null
    end as day60_retention,
    case 
      when sum(c.cohort_size) > 0 
      then sum(coalesce(r.day90_retention, 0) * c.cohort_size) / sum(c.cohort_size)
      else null
    end as day90_retention,
    
    -- 활동 지표 (평균) - 이미 월별로 집계된 평균값
    max(s.avg_events_per_user) as avg_events_per_user,
    max(s.avg_active_days_per_user) as avg_active_days_per_user,
    max(s.avg_lifetime_days) as avg_lifetime_days
    
  from cohort_monthly c
  left join retention_pivot r
    on c.cohort_month = r.cohort_month
    and c.organization = r.organization
    and c.repo_name = r.repo_name
    and c.cohort_activity_type = r.cohort_activity_type
    and c.cohort_date = r.cohort_date
  left join user_activity_stats s
    on c.cohort_month = s.cohort_month
    and c.organization = s.organization
    and c.repo_name = s.repo_name
  group by c.cohort_month, c.organization, c.repo_name
)

select
  cohort_month,
  organization,
  repo_name,
  cohort_size,
  light_users_count,
  medium_users_count,
  heavy_users_count,
  day1_retention,
  day7_retention,
  day14_retention,
  day30_retention,
  day60_retention,
  day90_retention,
  avg_events_per_user,
  avg_active_days_per_user,
  avg_lifetime_days
from final
where cohort_size > 0
order by cohort_month desc, organization, repo_name



