{{ config(
  materialized='incremental',
  incremental_strategy='append',
  on_schema_change='ignore',
  on_table_exists='replace',
  pre_hook=[
    "{{ delete_by_base_date() }}"
  ],
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
-- Incremental 전략:
--   - 증분 모드: 오늘 날짜만 계산하되, WAU/MAU는 과거 데이터 참조
--   - Full refresh: 전체 재계산

{% if is_incremental() %}
  {% set target_date = load_base_date_kst() %}
{% endif %}

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
{% if is_incremental() %}
    -- WAU/MAU 계산을 위해 현재 날짜 기준 과거 30일 데이터 필요
    and base_date >= date_add('day', -30, {{ target_date }})
{% endif %}
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

-- 사용자의 first_seen_date 계산 (신규 사용자 판단용)
, user_first_seen as (
  select
    organization,
    repo_name,
    user_login,
    min(base_date) as first_seen_date
  from user_activity_type
  group by 1, 2, 3
)

-- 최근 7일/30일 활동 계산을 위한 윈도우 함수
, user_rolling_activity as (
  select
    u.base_date,
    u.organization,
    u.repo_name,
    u.user_login,
    u.is_light_active,
    u.is_medium_active,
    u.is_heavy_active,
    f.first_seen_date,
    -- 최근 7일간 활동 여부 (자신 포함)
    max(u.is_light_active) over (
      partition by u.organization, u.repo_name, u.user_login
      order by u.base_date
      rows between 6 preceding and current row
    ) as is_light_active_7d,
    max(u.is_medium_active) over (
      partition by u.organization, u.repo_name, u.user_login
      order by u.base_date
      rows between 6 preceding and current row
    ) as is_medium_active_7d,
    max(u.is_heavy_active) over (
      partition by u.organization, u.repo_name, u.user_login
      order by u.base_date
      rows between 6 preceding and current row
    ) as is_heavy_active_7d,
    -- 최근 30일간 활동 여부 (자신 포함)
    max(u.is_light_active) over (
      partition by u.organization, u.repo_name, u.user_login
      order by u.base_date
      rows between 29 preceding and current row
    ) as is_light_active_30d,
    max(u.is_medium_active) over (
      partition by u.organization, u.repo_name, u.user_login
      order by u.base_date
      rows between 29 preceding and current row
    ) as is_medium_active_30d,
    max(u.is_heavy_active) over (
      partition by u.organization, u.repo_name, u.user_login
      order by u.base_date
      rows between 29 preceding and current row
    ) as is_heavy_active_30d,
    -- 7일 이전 마지막 활동일
    max(case when u.base_date < date_add('day', -7, current_date) then u.base_date end) over (
      partition by u.organization, u.repo_name, u.user_login
      order by u.base_date
      rows between unbounded preceding and 8 preceding
    ) as last_activity_before_7d
  from user_activity_type u
  left join user_first_seen f
    on u.organization = f.organization
    and u.repo_name = f.repo_name
    and u.user_login = f.user_login
)

-- 날짜별 집계 (타겟 날짜만 또는 전체)
, date_spine as (
  select distinct base_date, organization, repo_name
  from user_activity_type
{% if is_incremental() %}
  where base_date = {{ target_date }}
{% endif %}
)

-- 최종 집계
, final as (
  select
    d.base_date,
    d.organization,
    d.repo_name,
    
    -- DAU (Daily Active Users)
    count(distinct case when (r.is_light_active = 1 or r.is_medium_active = 1 or r.is_heavy_active = 1) then r.user_login end) as dau_any,
    count(distinct case when r.is_light_active = 1 then r.user_login end) as dau_light,
    count(distinct case when r.is_medium_active = 1 then r.user_login end) as dau_medium,
    count(distinct case when r.is_heavy_active = 1 then r.user_login end) as dau_heavy,
    
    -- WAU (Weekly Active Users - 최근 7일)
    count(distinct case when (r.is_light_active_7d = 1 or r.is_medium_active_7d = 1 or r.is_heavy_active_7d = 1) then r.user_login end) as wau_any,
    count(distinct case when r.is_light_active_7d = 1 then r.user_login end) as wau_light,
    count(distinct case when r.is_medium_active_7d = 1 then r.user_login end) as wau_medium,
    count(distinct case when r.is_heavy_active_7d = 1 then r.user_login end) as wau_heavy,
    
    -- MAU (Monthly Active Users - 최근 30일)
    count(distinct case when (r.is_light_active_30d = 1 or r.is_medium_active_30d = 1 or r.is_heavy_active_30d = 1) then r.user_login end) as mau_any,
    count(distinct case when r.is_light_active_30d = 1 then r.user_login end) as mau_light,
    count(distinct case when r.is_medium_active_30d = 1 then r.user_login end) as mau_medium,
    count(distinct case when r.is_heavy_active_30d = 1 then r.user_login end) as mau_heavy,
    
    -- 신규 사용자 (해당 일자에 처음 본 사용자)
    count(distinct case when r.first_seen_date = d.base_date then r.user_login end) as new_users_count,
    
    -- 복귀 사용자 (7일 이상 비활동 후 오늘 활동)
    count(distinct case 
      when (r.is_light_active = 1 or r.is_medium_active = 1 or r.is_heavy_active = 1)
        and r.last_activity_before_7d is not null
        and date_diff('day', r.last_activity_before_7d, d.base_date) >= 7
      then r.user_login 
    end) as returning_users_count
    
  from date_spine d
  left join user_rolling_activity r
    on d.base_date = r.base_date
    and d.organization = r.organization
    and d.repo_name = r.repo_name
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
where dau_any > 0 or wau_any > 0 or mau_any > 0  -- 활동이 있는 날짜만
{% if is_incremental() %}
-- incremental 모드: 타겟 날짜만 적재
  and base_date = {{ target_date }}
{% endif %}
order by base_date desc, organization, repo_name

