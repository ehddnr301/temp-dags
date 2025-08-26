{{ config(
  materialized='incremental',
  incremental_strategy='append',
  on_schema_change='ignore',
  pre_hook=[
    "{{ delete_by_base_date() }}"
  ]
) }}

-- DW: 모든 GitHub 활동을 단일 피드로 통합
-- 표준 스키마
--  - activity_id: string (유니크)
--  - source_event_type: string (PushEvent, IssuesEvent, ...)
--  - organization: string
--  - repo_name: string
--  - actor_login: string
--  - activity_text: string (한국어 요약)
--  - ts_kst: timestamp
--  - base_date: date (KST)

with

push as (
  select
    'PushEvent' as source_event_type,
    case when commit_sha is not null then concat(event_id, ':', commit_sha) else event_id end as activity_id,
    organization,
    repo_name,
    user_login as actor_login,
    concat(user_login, '이 ', coalesce(repo_name, ''), ':', coalesce(ref_name, ''),
           '에 ', substr(coalesce(commit_sha, ''), 1, 7), ' 푸시: ', substr(coalesce(commit_message, ''), 1, 120)) as activity_text,
    ts_kst,
    base_date
  from {{ ref('dl_push_events') }}
  where base_date = {{ load_base_date_kst() }}
)

, issues as (
  select
    'IssuesEvent' as source_event_type,
    event_id as activity_id,
    organization,
    repo_name,
    user_login as actor_login,
    case issue_action
      when 'opened' then concat(user_login, '이 이슈 #', cast(issue_number as varchar), ' 생성: ', substr(coalesce(issue_title, ''), 1, 120))
      when 'closed' then concat(user_login, '이 이슈 #', cast(issue_number as varchar), ' 종료')
      when 'reopened' then concat(user_login, '이 이슈 #', cast(issue_number as varchar), ' 재오픈')
      else concat(user_login, '이 이슈 #', cast(issue_number as varchar), ' ', coalesce(issue_action, '업데이트'))
    end as activity_text,
    ts_kst,
    base_date
  from {{ ref('dl_issues_events') }}
  where base_date = {{ load_base_date_kst() }}
)

, issue_comment as (
  select
    'IssueCommentEvent' as source_event_type,
    case when comment_id is not null then concat(event_id, ':', cast(comment_id as varchar)) else event_id end as activity_id,
    organization,
    repo_name,
    user_login as actor_login,
    concat(user_login, '이 이슈 #', cast(issue_number as varchar), '에 댓글: ', substr(coalesce(comment_body, ''), 1, 120)) as activity_text,
    ts_kst,
    base_date
  from {{ ref('dl_issue_comment_events') }}
  where base_date = {{ load_base_date_kst() }}
)

, pr as (
  select
    'PullRequestEvent' as source_event_type,
    event_id as activity_id,
    organization,
    repo_name,
    user_login as actor_login,
    case
      when pr_action = 'opened' then concat(user_login, '이 PR #', cast(pr_number as varchar), ' 생성: ', substr(coalesce(pr_title, ''), 1, 120))
      when pr_action = 'closed' and coalesce(is_merged, false) = true then concat(user_login, '이 PR #', cast(pr_number as varchar), ' 병합')
      when pr_action = 'closed' then concat(user_login, '이 PR #', cast(pr_number as varchar), ' 종료')
      when pr_action = 'synchronize' then concat(user_login, '이 PR #', cast(pr_number as varchar), ' 업데이트')
      else concat(user_login, '이 PR #', cast(pr_number as varchar), ' ', coalesce(pr_action, '업데이트'))
    end as activity_text,
    ts_kst,
    base_date
  from {{ ref('dl_pull_request_events') }}
  where base_date = {{ load_base_date_kst() }}
)

, pr_review as (
  select
    'PullRequestReviewEvent' as source_event_type,
    case when review_submitted_at_utc is not null then concat(event_id, ':', cast(pr_number as varchar), ':', coalesce(review_state, '')) else event_id end as activity_id,
    organization,
    repo_name,
    user_login as actor_login,
    concat(user_login, '이 PR #', cast(pr_number as varchar), ' 리뷰 제출: ', coalesce(review_state, '')) as activity_text,
    ts_kst,
    base_date
  from {{ ref('dl_pull_request_review_events') }}
  where base_date = {{ load_base_date_kst() }}
)

, pr_review_comment as (
  select
    'PullRequestReviewCommentEvent' as source_event_type,
    case when review_comment_id is not null then concat(event_id, ':', cast(review_comment_id as varchar)) else event_id end as activity_id,
    organization,
    repo_name,
    user_login as actor_login,
    concat(user_login, '이 PR #', cast(pr_number as varchar), '에 리뷰 댓글: ', substr(coalesce(file_path, ''), 1, 80)) as activity_text,
    ts_kst,
    base_date
  from {{ ref('dl_pull_request_review_comment_events') }}
  where base_date = {{ load_base_date_kst() }}
)

, watch as (
  select
    'WatchEvent' as source_event_type,
    event_id as activity_id,
    organization,
    repo_name,
    user_login as actor_login,
    concat(user_login, '이 ', coalesce(repo_name, ''), '에 Star 추가') as activity_text,
    ts_kst,
    base_date
  from {{ ref('dl_watch_events') }}
  where base_date = {{ load_base_date_kst() }}
)

, fork as (
  select
    'ForkEvent' as source_event_type,
    event_id as activity_id,
    organization,
    repo_name,
    user_login as actor_login,
    concat(user_login, '이 ', coalesce(repo_name, ''), '를 ', coalesce(forkee_full_name, ''), '로 포크') as activity_text,
    ts_kst,
    base_date
  from {{ ref('dl_fork_events') }}
  where base_date = {{ load_base_date_kst() }}
)

, member as (
  select
    'MemberEvent' as source_event_type,
    event_id as activity_id,
    organization,
    repo_name,
    user_login as actor_login,
    case
      when is_member_added then concat(user_login, '이 멤버 ', coalesce(member_login, ''), ' 추가')
      when is_member_removed then concat(user_login, '이 멤버 ', coalesce(member_login, ''), ' 제거')
      else concat(user_login, '이 멤버 ', coalesce(member_login, ''), ' 변경')
    end as activity_text,
    ts_kst,
    base_date
  from {{ ref('dl_member_events') }}
  where base_date = {{ load_base_date_kst() }}
)

, create_evt as (
  select
    'CreateEvent' as source_event_type,
    event_id as activity_id,
    organization,
    repo_name,
    user_login as actor_login,
    case
      when is_repo_creation then concat(user_login, '이 저장소 생성')
      when is_branch_creation then concat(user_login, '이 브랜치 ', coalesce(created_ref_name, ''), ' 생성')
      when is_tag_creation then concat(user_login, '이 태그 ', coalesce(created_ref_name, ''), ' 생성')
      else concat(user_login, '이 항목 ', coalesce(created_ref_name, ''), ' 생성')
    end as activity_text,
    ts_kst,
    base_date
  from {{ ref('dl_create_events') }}
  where base_date = {{ load_base_date_kst() }}
)

, delete_evt as (
  select
    'DeleteEvent' as source_event_type,
    event_id as activity_id,
    organization,
    repo_name,
    user_login as actor_login,
    case
      when is_branch_deletion then concat(user_login, '이 브랜치 ', coalesce(deleted_ref_name, ''), ' 삭제')
      when is_tag_deletion then concat(user_login, '이 태그 ', coalesce(deleted_ref_name, ''), ' 삭제')
      else concat(user_login, '이 ', coalesce(deleted_ref_type, ''), ' ', coalesce(deleted_ref_name, ''), ' 삭제')
    end as activity_text,
    ts_kst,
    base_date
  from {{ ref('dl_delete_events') }}
  where base_date = {{ load_base_date_kst() }}
)

, gollum as (
  select
    'GollumEvent' as source_event_type,
    case when page_sha is not null then concat(event_id, ':', page_sha) else event_id end as activity_id,
    organization,
    repo_name,
    user_login as actor_login,
    concat(user_login, '이 위키 ', coalesce(page_title, ''), ' ', coalesce(page_action, '업데이트')) as activity_text,
    ts_kst,
    base_date
  from {{ ref('dl_gollum_events') }}
  where base_date = {{ load_base_date_kst() }}
)

select * from (
  select * from push
  union all select * from issues
  union all select * from issue_comment
  union all select * from pr
  union all select * from pr_review
  union all select * from pr_review_comment
  union all select * from watch
  union all select * from fork
  union all select * from member
  union all select * from create_evt
  union all select * from delete_evt
  union all select * from gollum
)
as all_activities
{% if is_incremental() %}
where not exists (
  select 1 from {{ this }} t
  where t.activity_id = all_activities.activity_id
)
{% endif %}


