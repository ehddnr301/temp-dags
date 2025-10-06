{{ config(
  materialized='table',
  on_table_exists='replace',
  properties={
    "location": "'s3://gh-archive-delta/dm/dm_user_retention'",
    "partitioned_by": "ARRAY['cohort_date']"
  }
) }}

-- DM: 사용자 코호트 Retention 분석
-- 목적: 신규 유입 사용자의 시간 경과별 재방문율 추적
-- 코호트: first_seen_date 기준으로 그룹화
-- Retention 측정 시점: Day 1, 7, 14, 30, 60, 90
-- Materialization: table (전체 재계산 - 코호트 간 의존성 때문)

with daily_base as (
  select
    base_date,
    organization,
    repo_name,
    user_login,
    event_type,
    event_count
  from {{ ref('dw_activity_daily') }}
  where organization is not null
    and repo_name is not null
    and user_login is not null
)

-- 사용자별 활동 타입 분류
, user_activity_type as (
  select
    base_date,
    organization,
    repo_name,
    user_login,
    -- Light Active 판단
    max(case 
      when event_type in ('WatchEvent', 'ForkEvent') then 1 
      else 0 
    end) as is_light_active,
    -- Medium Active 판단
    max(case 
      when event_type in ('PushEvent', 'CreateEvent', 'DeleteEvent') then 1 
      else 0 
    end) as is_medium_active,
    -- Heavy Active 판단
    max(case 
      when event_type in (
        'PullRequestEvent',
        'PullRequestReviewEvent', 
        'PullRequestReviewCommentEvent',
        'IssuesEvent',
        'IssueCommentEvent'
      ) then 1 
      else 0 
    end) as is_heavy_active
  from daily_base
  group by 1, 2, 3, 4
)

-- 사용자의 first_seen_date (코호트 기준일)
, user_first_seen as (
  select
    organization,
    repo_name,
    user_login,
    min(base_date) as cohort_date
  from user_activity_type
  group by 1, 2, 3
)

-- 첫 활동 타입 결정 (우선순위: Heavy > Medium > Light)
, user_cohort as (
  select
    f.organization,
    f.repo_name,
    f.user_login,
    f.cohort_date,
    case
      when max(case when a.is_heavy_active = 1 then 1 else 0 end) = 1 
        then 'Heavy'
      when max(case when a.is_medium_active = 1 then 1 else 0 end) = 1 
        then 'Medium'
      when max(case when a.is_light_active = 1 then 1 else 0 end) = 1 
        then 'Light'
      else 'Unknown'
    end as cohort_activity_type
  from user_first_seen f
  left join user_activity_type a
    on f.organization = a.organization
    and f.repo_name = a.repo_name
    and f.user_login = a.user_login
    and f.cohort_date = a.base_date  -- 첫날의 활동만
  group by 1, 2, 3, 4
)

-- Retention 측정 시점 정의 (Day 1, 7, 14, 30, 60, 90)
, retention_days as (
  select day_offset
  from (
    values (1), (7), (14), (30), (60), (90)
  ) as t(day_offset)
)

-- 코호트별 크기 계산
, cohort_size as (
  select
    cohort_date,
    organization,
    repo_name,
    cohort_activity_type,
    count(distinct user_login) as cohort_size
  from user_cohort
  group by 1, 2, 3, 4
)

-- 각 코호트에 대해 N일 후 활동한 사용자 찾기
, retention_activity as (
  select
    c.cohort_date,
    c.organization,
    c.repo_name,
    c.user_login,
    c.cohort_activity_type,
    r.day_offset as days_since_cohort,
    date_add('day', r.day_offset, c.cohort_date) as retention_check_date,
    -- N일 후 활동 여부 확인
    max(case when a.base_date = date_add('day', r.day_offset, c.cohort_date) then 1 else 0 end) as is_retained_any,
    max(case when a.base_date = date_add('day', r.day_offset, c.cohort_date) and a.is_light_active = 1 then 1 else 0 end) as is_retained_light,
    max(case when a.base_date = date_add('day', r.day_offset, c.cohort_date) and a.is_medium_active = 1 then 1 else 0 end) as is_retained_medium,
    max(case when a.base_date = date_add('day', r.day_offset, c.cohort_date) and a.is_heavy_active = 1 then 1 else 0 end) as is_retained_heavy
  from user_cohort c
  cross join retention_days r
  left join user_activity_type a
    on c.organization = a.organization
    and c.repo_name = a.repo_name
    and c.user_login = a.user_login
    and a.base_date = date_add('day', r.day_offset, c.cohort_date)
  group by 1, 2, 3, 4, 5, 6, 7
)

-- 최종 집계
, final as (
  select
    ra.cohort_date,
    ra.organization,
    ra.repo_name,
    ra.days_since_cohort,
    ra.retention_check_date,
    cs.cohort_size,
    cs.cohort_activity_type,
    -- Retained users (N일 후에도 활동한 사용자 수)
    count(distinct case when ra.is_retained_any = 1 then ra.user_login end) as retained_users_any,
    count(distinct case when ra.is_retained_light = 1 then ra.user_login end) as retained_users_light,
    count(distinct case when ra.is_retained_medium = 1 then ra.user_login end) as retained_users_medium,
    count(distinct case when ra.is_retained_heavy = 1 then ra.user_login end) as retained_users_heavy,
    -- Retention Rate (비율)
    case 
      when cs.cohort_size > 0 
      then cast(count(distinct case when ra.is_retained_any = 1 then ra.user_login end) as double) / cast(cs.cohort_size as double)
      else 0.0
    end as retention_rate_any,
    case 
      when cs.cohort_size > 0 
      then cast(count(distinct case when ra.is_retained_light = 1 then ra.user_login end) as double) / cast(cs.cohort_size as double)
      else 0.0
    end as retention_rate_light,
    case 
      when cs.cohort_size > 0 
      then cast(count(distinct case when ra.is_retained_medium = 1 then ra.user_login end) as double) / cast(cs.cohort_size as double)
      else 0.0
    end as retention_rate_medium,
    case 
      when cs.cohort_size > 0 
      then cast(count(distinct case when ra.is_retained_heavy = 1 then ra.user_login end) as double) / cast(cs.cohort_size as double)
      else 0.0
    end as retention_rate_heavy
  from retention_activity ra
  left join cohort_size cs
    on ra.cohort_date = cs.cohort_date
    and ra.organization = cs.organization
    and ra.repo_name = cs.repo_name
    and ra.cohort_activity_type = cs.cohort_activity_type
  group by 1, 2, 3, 4, 5, 6, 7
)

select
  cohort_date,
  organization,
  repo_name,
  days_since_cohort,
  retention_check_date,
  cohort_size,
  cohort_activity_type,
  retained_users_any,
  retained_users_light,
  retained_users_medium,
  retained_users_heavy,
  retention_rate_any,
  retention_rate_light,
  retention_rate_medium,
  retention_rate_heavy
from final
where cohort_size > 0  -- 코호트가 존재하는 경우만
order by cohort_date desc, organization, repo_name, days_since_cohort



