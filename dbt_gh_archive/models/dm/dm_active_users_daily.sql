{{ config(
  materialized='table',
  on_table_exists='replace',
  properties={
    "location": "'s3://gh-archive-delta/dm/dm_active_users_daily'",
    "partitioned_by": "ARRAY['base_date']"
  }
) }}

-- DM: 일별 활성 사용자 수 (DAU/WAU/MAU)
-- 목적: Active User 타입별 일별/주별/월별 활성 사용자 추적
-- Active User 정의:
--   - Light Active: WatchEvent, ForkEvent (관심 표현)
--   - Medium Active: PushEvent, CreateEvent, DeleteEvent (코드 기여)
--   - Heavy Active: PullRequestEvent, PullRequestReviewEvent, PullRequestReviewCommentEvent, IssuesEvent, IssueCommentEvent (협업)
-- Materialization: table (전체 재계산)

with daily_base as (
  select
    base_date,
    organization,
    repo_name,
    user_login,
    event_type,
    event_count
  from {{ ref('dw_activity_daily') }}
  where organization in (
    'CausalInferenceLab',
    'Pseudo-Lab'
  ) or repo_name in (
    'superset'
  )
)

-- 사용자별 일자별 활동 타입 분류
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
    end) as is_heavy_active,
    sum(event_count) as total_events
  from daily_base
  group by 1, 2, 3, 4
)

-- 신규/복귀 사용자 메타 정보
, user_metadata as (
  select
    organization,
    repo_name,
    user_login,
    base_date,
    row_number() over (partition by organization, repo_name, user_login order by base_date) as rn,
    lag(base_date) over (partition by organization, repo_name, user_login order by base_date) as prev_activity_date
  from user_activity_type
)

-- 날짜 스파인 (활동이 있는 모든 날짜/조직/레포 조합)
, date_spine as (
  select distinct 
    base_date,
    organization,
    repo_name
  from user_activity_type
)

-- 최종 집계: 각 날짜별로 과거 데이터 참조
, final as (
  select
    d.base_date,
    d.organization,
    d.repo_name,
    
    -- DAU (Daily Active Users) - 해당 날짜에 활동한 사용자
    count(distinct case 
      when u.base_date = d.base_date
        and (u.is_light_active = 1 or u.is_medium_active = 1 or u.is_heavy_active = 1)
      then u.user_login 
    end) as dau_any,
    count(distinct case 
      when u.base_date = d.base_date and u.is_light_active = 1
      then u.user_login 
    end) as dau_light,
    count(distinct case 
      when u.base_date = d.base_date and u.is_medium_active = 1
      then u.user_login 
    end) as dau_medium,
    count(distinct case 
      when u.base_date = d.base_date and u.is_heavy_active = 1
      then u.user_login 
    end) as dau_heavy,
    
    -- WAU (Weekly Active Users) - 과거 7일간 활동한 unique users
    count(distinct case 
      when u.base_date >= date_add('day', -6, d.base_date) 
        and u.base_date <= d.base_date
        and (u.is_light_active = 1 or u.is_medium_active = 1 or u.is_heavy_active = 1)
      then u.user_login 
    end) as wau_any,
    count(distinct case 
      when u.base_date >= date_add('day', -6, d.base_date) 
        and u.base_date <= d.base_date
        and u.is_light_active = 1
      then u.user_login 
    end) as wau_light,
    count(distinct case 
      when u.base_date >= date_add('day', -6, d.base_date) 
        and u.base_date <= d.base_date
        and u.is_medium_active = 1
      then u.user_login 
    end) as wau_medium,
    count(distinct case 
      when u.base_date >= date_add('day', -6, d.base_date) 
        and u.base_date <= d.base_date
        and u.is_heavy_active = 1
      then u.user_login 
    end) as wau_heavy,
    
    -- MAU (Monthly Active Users) - 과거 30일간 활동한 unique users
    count(distinct case 
      when u.base_date >= date_add('day', -29, d.base_date) 
        and u.base_date <= d.base_date
        and (u.is_light_active = 1 or u.is_medium_active = 1 or u.is_heavy_active = 1)
      then u.user_login 
    end) as mau_any,
    count(distinct case 
      when u.base_date >= date_add('day', -29, d.base_date) 
        and u.base_date <= d.base_date
        and u.is_light_active = 1
      then u.user_login 
    end) as mau_light,
    count(distinct case 
      when u.base_date >= date_add('day', -29, d.base_date) 
        and u.base_date <= d.base_date
        and u.is_medium_active = 1
      then u.user_login 
    end) as mau_medium,
    count(distinct case 
      when u.base_date >= date_add('day', -29, d.base_date) 
        and u.base_date <= d.base_date
        and u.is_heavy_active = 1
      then u.user_login 
    end) as mau_heavy,
    
    -- 신규 사용자 (해당 날짜에 처음 활동한 사용자)
    count(distinct case 
      when m.rn = 1 and m.base_date = d.base_date
      then m.user_login 
    end) as new_users_count,
    
    -- 복귀 사용자 (7일 이상 비활동 후 다시 활동)
    count(distinct case 
      when m.base_date = d.base_date
        and m.prev_activity_date is not null
        and date_diff('day', m.prev_activity_date, m.base_date) >= 7
      then m.user_login 
    end) as returning_users_count
    
  from date_spine d
  inner join user_activity_type u
    on d.organization = u.organization
    and d.repo_name = u.repo_name
    and u.base_date >= date_add('day', -29, d.base_date)  -- MAU 범위만 조인
    and u.base_date <= d.base_date
  left join user_metadata m
    on d.organization = m.organization
    and d.repo_name = m.repo_name
    and d.base_date = m.base_date
  group by 1, 2, 3
)

select
  base_date,
  organization,
  repo_name,
  dau_any,
  dau_light,
  dau_medium,
  dau_heavy,
  wau_any,
  wau_light,
  wau_medium,
  wau_heavy,
  mau_any,
  mau_light,
  mau_medium,
  mau_heavy,
  -- Stickiness (참여도): DAU/MAU 비율
  case 
    when mau_any > 0 then cast(dau_any as double) / cast(mau_any as double)
    else 0.0
  end as stickiness_ratio,
  new_users_count,
  returning_users_count
from final
order by base_date desc, organization, repo_name

