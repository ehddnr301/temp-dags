{{ config(
  materialized='table',
  on_table_exists='replace',
  properties={
    "location": "'s3://gh-archive-delta/dm/dm_repo_contributors'",
    "partitioned_by": "ARRAY['period_start_date']"
  }
) }}

-- DM: 레포지토리별 기여자 현황 및 랭킹
-- 목적: 레포별 기여자 목록, 기여도, 활동 패턴 제공
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
    and organization is not null
    and repo_name is not null
)

-- 일별 집계 (day)
, daily_agg as (
  select
    base_date as period_start_date,
    base_date as period_end_date,
    'day' as aggregation_level,
    organization,
    repo_name,
    user_login,
    
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
    
    -- 기여도 점수 (가중치 적용)
    sum(
      case event_type
        when 'PullRequestEvent' then event_count * 10         -- PR 생성/병합: 가장 높은 가치
        when 'PullRequestReviewEvent' then event_count * 8     -- PR 리뷰: 높은 협업 가치
        when 'PushEvent' then event_count * 5                  -- 커밋: 코드 기여
        when 'IssuesEvent' then event_count * 5                -- 이슈 생성/관리
        when 'PullRequestReviewCommentEvent' then event_count * 3  -- 리뷰 코멘트
        when 'IssueCommentEvent' then event_count * 2          -- 이슈 코멘트
        when 'CreateEvent' then event_count * 2                -- 브랜치/태그 생성
        when 'WatchEvent' then event_count * 1                 -- Star
        when 'ForkEvent' then event_count * 1                  -- Fork
        else event_count * 1
      end
    ) as contribution_score,
    
    -- 활동 패턴 (일별은 1일)
    1 as active_days_count,
    base_date as first_activity_date,
    base_date as last_activity_date
    
  from daily_base
  group by 1, 2, 3, 4, 5, 6
)

-- 주별 집계 (week)
, weekly_agg as (
  select
    date_trunc('week', base_date) as period_start_date,
    date_trunc('week', base_date) + interval '6' day as period_end_date,
    'week' as aggregation_level,
    organization,
    repo_name,
    user_login,
    
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
    
    -- 기여도 점수 (가중치 적용)
    sum(
      case event_type
        when 'PullRequestEvent' then event_count * 10
        when 'PullRequestReviewEvent' then event_count * 8
        when 'PushEvent' then event_count * 5
        when 'IssuesEvent' then event_count * 5
        when 'PullRequestReviewCommentEvent' then event_count * 3
        when 'IssueCommentEvent' then event_count * 2
        when 'CreateEvent' then event_count * 2
        when 'WatchEvent' then event_count * 1
        when 'ForkEvent' then event_count * 1
        else event_count * 1
      end
    ) as contribution_score,
    
    -- 활동 패턴
    count(distinct base_date) as active_days_count,
    min(base_date) as first_activity_date,
    max(base_date) as last_activity_date
    
  from daily_base
  group by 1, 2, 3, 4, 5, 6
)

-- 월별 집계 (month)
, monthly_agg as (
  select
    date_trunc('month', base_date) as period_start_date,
    last_day_of_month(date_trunc('month', base_date)) as period_end_date,
    'month' as aggregation_level,
    organization,
    repo_name,
    user_login,
    
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
    
    -- 기여도 점수 (가중치 적용)
    sum(
      case event_type
        when 'PullRequestEvent' then event_count * 10
        when 'PullRequestReviewEvent' then event_count * 8
        when 'PushEvent' then event_count * 5
        when 'IssuesEvent' then event_count * 5
        when 'PullRequestReviewCommentEvent' then event_count * 3
        when 'IssueCommentEvent' then event_count * 2
        when 'CreateEvent' then event_count * 2
        when 'WatchEvent' then event_count * 1
        when 'ForkEvent' then event_count * 1
        else event_count * 1
      end
    ) as contribution_score,
    
    -- 활동 패턴
    count(distinct base_date) as active_days_count,
    min(base_date) as first_activity_date,
    max(base_date) as last_activity_date
    
  from daily_base
  group by 1, 2, 3, 4, 5, 6
)

-- Union all aggregation levels
, combined as (
  select * from daily_agg
  union all
  select * from weekly_agg
  union all
  select * from monthly_agg
)

-- 레포지토리 내 랭킹 계산
, with_rank as (
  select
    cast(period_start_date as date) as period_start_date,
    cast(period_end_date as date) as period_end_date,
    aggregation_level,
    organization,
    repo_name,
    user_login,
    total_events_count,
    commits_count,
    pr_count,
    pr_review_count,
    pr_review_comment_count,
    issue_count,
    issue_comment_count,
    watch_count,
    fork_count,
    contribution_score,
    active_days_count,
    first_activity_date,
    last_activity_date,
    
    -- 기여도 기준 레포 내 랭킹
    row_number() over (
      partition by period_start_date, aggregation_level, organization, repo_name 
      order by contribution_score desc, total_events_count desc
    ) as rank_in_repo
  from combined
  where total_events_count > 0  -- 활동이 있는 경우만
)

select
  period_start_date,
  period_end_date,
  aggregation_level,
  organization,
  repo_name,
  user_login,
  total_events_count,
  commits_count,
  pr_count,
  pr_review_count,
  pr_review_comment_count,
  issue_count,
  issue_comment_count,
  watch_count,
  fork_count,
  contribution_score,
  rank_in_repo,
  active_days_count,
  first_activity_date,
  last_activity_date
from with_rank
order by organization, repo_name, period_start_date desc, rank_in_repo asc



