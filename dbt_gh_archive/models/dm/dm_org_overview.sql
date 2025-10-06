{{ config(
  materialized='table',
  on_table_exists='replace',
  properties={
    "location": "'s3://gh-archive-delta/dm/dm_org_overview'",
    "partitioned_by": "ARRAY['activity_date']"
  }
) }}



-- DM: 조직 전체 현황 요약
-- 목적: 여러 레포를 통합한 조직 단위 KPI 제공
-- 집계 수준: 일/주/월별
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
  where organization is not null
)

-- 일별 집계 (day)
, daily_agg as (
  select
    base_date as activity_date,
    'day' as aggregation_level,
    organization,
    
    -- 저장소 현황
    count(distinct repo_name) as total_repos_count,
    count(distinct repo_name) as active_repos_count,  -- 일별은 활동이 있는 레포만 카운트
    
    -- 구성원 현황
    count(distinct user_login) as total_contributors_count,
    count(distinct user_login) as active_contributors_count,  -- 일별 활성 기여자
    
    -- 활동 현황
    sum(case when event_type = 'PushEvent' then event_count else 0 end) as total_commits_count,
    sum(case when event_type = 'PullRequestEvent' then event_count else 0 end) as total_pr_count,
    sum(case when event_type = 'IssuesEvent' then event_count else 0 end) as total_issues_count,
    sum(case when event_type in ('PullRequestReviewEvent', 'PullRequestReviewCommentEvent') then event_count else 0 end) as total_reviews_count,
    sum(case when event_type = 'WatchEvent' then event_count else 0 end) as total_stars_count,
    sum(case when event_type = 'ForkEvent' then event_count else 0 end) as total_forks_count,
    sum(event_count) as total_events_count
    
  from daily_base
  group by 1, 2, 3
)

-- 주별 집계 (week)
, weekly_agg as (
  select
    date_trunc('week', base_date) as activity_date,
    'week' as aggregation_level,
    organization,
    
    -- 저장소 현황
    count(distinct repo_name) as total_repos_count,
    count(distinct repo_name) as active_repos_count,
    
    -- 구성원 현황
    count(distinct user_login) as total_contributors_count,
    count(distinct user_login) as active_contributors_count,
    
    -- 활동 현황
    sum(case when event_type = 'PushEvent' then event_count else 0 end) as total_commits_count,
    sum(case when event_type = 'PullRequestEvent' then event_count else 0 end) as total_pr_count,
    sum(case when event_type = 'IssuesEvent' then event_count else 0 end) as total_issues_count,
    sum(case when event_type in ('PullRequestReviewEvent', 'PullRequestReviewCommentEvent') then event_count else 0 end) as total_reviews_count,
    sum(case when event_type = 'WatchEvent' then event_count else 0 end) as total_stars_count,
    sum(case when event_type = 'ForkEvent' then event_count else 0 end) as total_forks_count,
    sum(event_count) as total_events_count
    
  from daily_base
  group by 1, 2, 3
)

-- 월별 집계 (month)
, monthly_agg as (
  select
    date_trunc('month', base_date) as activity_date,
    'month' as aggregation_level,
    organization,
    
    -- 저장소 현황
    count(distinct repo_name) as total_repos_count,
    count(distinct repo_name) as active_repos_count,
    
    -- 구성원 현황
    count(distinct user_login) as total_contributors_count,
    count(distinct user_login) as active_contributors_count,
    
    -- 활동 현황
    sum(case when event_type = 'PushEvent' then event_count else 0 end) as total_commits_count,
    sum(case when event_type = 'PullRequestEvent' then event_count else 0 end) as total_pr_count,
    sum(case when event_type = 'IssuesEvent' then event_count else 0 end) as total_issues_count,
    sum(case when event_type in ('PullRequestReviewEvent', 'PullRequestReviewCommentEvent') then event_count else 0 end) as total_reviews_count,
    sum(case when event_type = 'WatchEvent' then event_count else 0 end) as total_stars_count,
    sum(case when event_type = 'ForkEvent' then event_count else 0 end) as total_forks_count,
    sum(event_count) as total_events_count
    
  from daily_base
  group by 1, 2, 3
)

-- 신규 기여자 계산 (일별)
, new_contributors_daily as (
  select
    base_date as activity_date,
    organization,
    user_login,
    row_number() over (partition by organization, user_login order by base_date) as rn
  from daily_base
  group by 1, 2, 3
)

, new_contributors_count_daily as (
  select
    activity_date,
    organization,
    count(distinct user_login) as new_contributors_count
  from new_contributors_daily
  where rn = 1
  group by 1, 2
)

-- 신규 기여자 계산 (주별)
, new_contributors_weekly as (
  select
    date_trunc('week', activity_date) as activity_week,
    organization,
    user_login,
    min(activity_date) as first_activity_date
  from new_contributors_daily
  where rn = 1
  group by 1, 2, 3
)

, new_contributors_count_weekly as (
  select
    activity_week as activity_date,
    organization,
    count(distinct user_login) as new_contributors_count
  from new_contributors_weekly
  where date_trunc('week', first_activity_date) = activity_week
  group by 1, 2
)

-- 신규 기여자 계산 (월별)
, new_contributors_monthly as (
  select
    date_trunc('month', activity_date) as activity_month,
    organization,
    user_login,
    min(activity_date) as first_activity_date
  from new_contributors_daily
  where rn = 1
  group by 1, 2, 3
)

, new_contributors_count_monthly as (
  select
    activity_month as activity_date,
    organization,
    count(distinct user_login) as new_contributors_count
  from new_contributors_monthly
  where date_trunc('month', first_activity_date) = activity_month
  group by 1, 2
)

-- Union all aggregation levels
, combined as (
  select
    d.activity_date,
    d.aggregation_level,
    d.organization,
    d.total_repos_count,
    d.active_repos_count,
    d.total_contributors_count,
    coalesce(n.new_contributors_count, 0) as new_contributors_count,
    d.active_contributors_count,
    d.total_commits_count,
    d.total_pr_count,
    d.total_issues_count,
    d.total_reviews_count,
    d.total_stars_count,
    d.total_forks_count,
    d.total_events_count
  from daily_agg d
  left join new_contributors_count_daily n
    on d.activity_date = n.activity_date
    and d.organization = n.organization
  
  union all
  
  select
    w.activity_date,
    w.aggregation_level,
    w.organization,
    w.total_repos_count,
    w.active_repos_count,
    w.total_contributors_count,
    coalesce(n.new_contributors_count, 0) as new_contributors_count,
    w.active_contributors_count,
    w.total_commits_count,
    w.total_pr_count,
    w.total_issues_count,
    w.total_reviews_count,
    w.total_stars_count,
    w.total_forks_count,
    w.total_events_count
  from weekly_agg w
  left join new_contributors_count_weekly n
    on w.activity_date = n.activity_date
    and w.organization = n.organization
  
  union all
  
  select
    m.activity_date,
    m.aggregation_level,
    m.organization,
    m.total_repos_count,
    m.active_repos_count,
    m.total_contributors_count,
    coalesce(n.new_contributors_count, 0) as new_contributors_count,
    m.active_contributors_count,
    m.total_commits_count,
    m.total_pr_count,
    m.total_issues_count,
    m.total_reviews_count,
    m.total_stars_count,
    m.total_forks_count,
    m.total_events_count
  from monthly_agg m
  left join new_contributors_count_monthly n
    on m.activity_date = n.activity_date
    and m.organization = n.organization
)

select
  cast(activity_date as date) as activity_date,
  aggregation_level,
  organization,
  total_repos_count,
  active_repos_count,
  total_contributors_count,
  new_contributors_count,
  active_contributors_count,
  total_commits_count,
  total_pr_count,
  total_issues_count,
  total_reviews_count,
  total_stars_count,
  total_forks_count,
  total_events_count
from combined
where total_events_count > 0  -- 활동이 있는 경우만
order by organization, aggregation_level, activity_date desc



