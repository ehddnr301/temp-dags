{{ config(
  materialized='table',
  on_table_exists='replace',
  properties={
    "location": "'s3://gh-archive-delta/dm/dm_user_activity'",
    "partitioned_by": "ARRAY['activity_date']"
  }
) }}

-- DM: 사용자별 활동 현황
-- 목적: 개별 사용자의 활동 패턴과 기여도를 추적
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
  where user_login is not null
)

-- 일별 집계 (day)
, daily_agg as (
  select
    base_date as activity_date,
    'day' as aggregation_level,
    user_login,
    organization,
    repo_name,
    
    -- 활동 카운트
    sum(event_count) as total_events_count,
    sum(case when event_type = 'PushEvent' then event_count else 0 end) as commits_count,
    sum(case when event_type = 'PullRequestEvent' then event_count else 0 end) as pr_count,
    sum(case when event_type = 'PullRequestReviewEvent' then event_count else 0 end) as pr_review_count,
    sum(case when event_type = 'PullRequestReviewCommentEvent' then event_count else 0 end) as pr_review_comment_count,
    sum(case when event_type = 'IssuesEvent' then event_count else 0 end) as issue_count,
    sum(case when event_type = 'IssueCommentEvent' then event_count else 0 end) as issue_comment_count,
    sum(case when event_type = 'WatchEvent' then event_count else 0 end) as watch_count,
    sum(case when event_type = 'ForkEvent' then event_count else 0 end) as fork_count,
    sum(case when event_type = 'CreateEvent' then event_count else 0 end) as create_count,
    sum(case when event_type = 'DeleteEvent' then event_count else 0 end) as delete_count,
    sum(case when event_type = 'GollumEvent' then event_count else 0 end) as wiki_count,
    sum(case when event_type = 'MemberEvent' then event_count else 0 end) as member_count,
    
    -- 활동 패턴 (일별은 1일)
    1 as active_days_count,
    base_date as first_activity_date,
    base_date as last_activity_date
    
  from daily_base
  group by 1, 2, 3, 4, 5
)

-- 주별 집계 (week)
, weekly_agg as (
  select
    date_trunc('week', base_date) as activity_date,
    'week' as aggregation_level,
    user_login,
    organization,
    repo_name,
    
    -- 활동 카운트
    sum(event_count) as total_events_count,
    sum(case when event_type = 'PushEvent' then event_count else 0 end) as commits_count,
    sum(case when event_type = 'PullRequestEvent' then event_count else 0 end) as pr_count,
    sum(case when event_type = 'PullRequestReviewEvent' then event_count else 0 end) as pr_review_count,
    sum(case when event_type = 'PullRequestReviewCommentEvent' then event_count else 0 end) as pr_review_comment_count,
    sum(case when event_type = 'IssuesEvent' then event_count else 0 end) as issue_count,
    sum(case when event_type = 'IssueCommentEvent' then event_count else 0 end) as issue_comment_count,
    sum(case when event_type = 'WatchEvent' then event_count else 0 end) as watch_count,
    sum(case when event_type = 'ForkEvent' then event_count else 0 end) as fork_count,
    sum(case when event_type = 'CreateEvent' then event_count else 0 end) as create_count,
    sum(case when event_type = 'DeleteEvent' then event_count else 0 end) as delete_count,
    sum(case when event_type = 'GollumEvent' then event_count else 0 end) as wiki_count,
    sum(case when event_type = 'MemberEvent' then event_count else 0 end) as member_count,
    
    -- 활동 패턴
    count(distinct base_date) as active_days_count,
    min(base_date) as first_activity_date,
    max(base_date) as last_activity_date
    
  from daily_base
  group by 1, 2, 3, 4, 5
)

-- 월별 집계 (month)
, monthly_agg as (
  select
    date_trunc('month', base_date) as activity_date,
    'month' as aggregation_level,
    user_login,
    organization,
    repo_name,
    
    -- 활동 카운트
    sum(event_count) as total_events_count,
    sum(case when event_type = 'PushEvent' then event_count else 0 end) as commits_count,
    sum(case when event_type = 'PullRequestEvent' then event_count else 0 end) as pr_count,
    sum(case when event_type = 'PullRequestReviewEvent' then event_count else 0 end) as pr_review_count,
    sum(case when event_type = 'PullRequestReviewCommentEvent' then event_count else 0 end) as pr_review_comment_count,
    sum(case when event_type = 'IssuesEvent' then event_count else 0 end) as issue_count,
    sum(case when event_type = 'IssueCommentEvent' then event_count else 0 end) as issue_comment_count,
    sum(case when event_type = 'WatchEvent' then event_count else 0 end) as watch_count,
    sum(case when event_type = 'ForkEvent' then event_count else 0 end) as fork_count,
    sum(case when event_type = 'CreateEvent' then event_count else 0 end) as create_count,
    sum(case when event_type = 'DeleteEvent' then event_count else 0 end) as delete_count,
    sum(case when event_type = 'GollumEvent' then event_count else 0 end) as wiki_count,
    sum(case when event_type = 'MemberEvent' then event_count else 0 end) as member_count,
    
    -- 활동 패턴
    count(distinct base_date) as active_days_count,
    min(base_date) as first_activity_date,
    max(base_date) as last_activity_date
    
  from daily_base
  group by 1, 2, 3, 4, 5
)

-- Union all aggregation levels
, combined as (
  select * from daily_agg
  union all
  select * from weekly_agg
  union all
  select * from monthly_agg
)

select
  cast(activity_date as date) as activity_date,
  aggregation_level,
  user_login,
  organization,
  repo_name,
  total_events_count,
  commits_count,
  pr_count,
  pr_review_count,
  pr_review_comment_count,
  issue_count,
  issue_comment_count,
  watch_count,
  fork_count,
  create_count,
  delete_count,
  wiki_count,
  member_count,
  active_days_count,
  first_activity_date,
  last_activity_date
from combined
where total_events_count > 0  -- 활동이 있는 경우만
order by user_login, organization, repo_name, aggregation_level, activity_date desc



