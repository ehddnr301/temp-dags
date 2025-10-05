{{ config(materialized='table') }}

-- 사용자 DIM: 첫/마지막 활동일, 이벤트 사용 여부, 빌더 여부 포함
with activity as (
  select
    user_login,
    min(base_date) as first_seen_date,
    max(base_date) as last_seen_date,
    -- 이벤트 경험 여부 플래그
    (max(case when event_type = 'CreateEvent' then 1 else 0 end) > 0) as has_create_event,
    (max(case when event_type = 'MemberEvent' then 1 else 0 end) > 0) as has_member_event,
    (max(case when event_type = 'PullRequestEvent' then 1 else 0 end) > 0) as has_pull_request_event,
    (max(case when event_type = 'PullRequestReviewCommentEvent' then 1 else 0 end) > 0) as has_pr_review_comment_event,
    (max(case when event_type = 'PushEvent' then 1 else 0 end) > 0) as has_push_event,
    (max(case when event_type = 'IssuesEvent' then 1 else 0 end) > 0) as has_issues_event,
    (max(case when event_type = 'WatchEvent' then 1 else 0 end) > 0) as has_watch_event
  from {{ ref('dw_activity_daily') }}
  where user_login is not null
  group by user_login
)
,
builder as (
  select
    user_login,
    min(base_date) as builder_first_date,
    (max(case when is_repo_creation then 1 else 0 end) > 0) as is_builder
  from {{ ref('dl_create_events') }}
  where user_login is not null
    and is_repo_creation = true
  group by 1
)

,
first_seen_ctx as (
  select user_login, organization as first_seen_organization, repo_name as first_seen_repo
  from (
    select
      d.user_login,
      d.organization,
      d.repo_name,
      d.event_count,
      row_number() over (
        partition by d.user_login
        order by d.event_count desc, d.organization asc, d.repo_name asc
      ) as rn
    from {{ ref('dw_activity_daily') }} d
    join activity a
      on d.user_login = a.user_login
     and d.base_date  = a.first_seen_date
  ) s
  where rn = 1
)

,
last_seen_ctx as (
  select user_login, organization as last_seen_organization, repo_name as last_seen_repo
  from (
    select
      d.user_login,
      d.organization,
      d.repo_name,
      d.event_count,
      row_number() over (
        partition by d.user_login
        order by d.event_count desc, d.organization asc, d.repo_name asc
      ) as rn
    from {{ ref('dw_activity_daily') }} d
    join activity a
      on d.user_login = a.user_login
     and d.base_date  = a.last_seen_date
  ) s
  where rn = 1
)

select
  A.user_login,
  A.first_seen_date,
  A.last_seen_date,
  F.first_seen_organization,
  F.first_seen_repo,
  L.last_seen_organization,
  L.last_seen_repo,
  A.has_create_event,
  A.has_member_event,
  A.has_pull_request_event,
  A.has_pr_review_comment_event,
  A.has_push_event,
  A.has_issues_event,
  A.has_watch_event,
  coalesce(B.is_builder, false) as is_builder,
  B.builder_first_date
from activity AS A
left join builder AS B 
ON A.user_login = B.user_login
left join first_seen_ctx F
  on A.user_login = F.user_login
left join last_seen_ctx L
  on A.user_login = L.user_login


