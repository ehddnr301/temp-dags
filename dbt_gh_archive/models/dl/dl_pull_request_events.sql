{{ config(
  materialized='incremental',
  incremental_strategy='append',
  on_schema_change='ignore',
  on_table_exists='replace',
  pre_hook=[
    "{{ delete_by_base_date() }}"
  ],
  properties={
    "location": "'s3://gh-archive-delta/dl/dl_pull_request_events'",
    "partitioned_by": "ARRAY['base_date']"
  }
) }}

-- DL: PullRequestEvent 원천 필터 뷰

with src as (
  select *
  from {{ source('delta_default', 'gh_archive_filtered') }}
  where cast(type as varchar) = 'PullRequestEvent'
{% if is_incremental() %}
    and cast(dt_kst as date) = {{ load_base_date_kst() }}
{% endif %}
)

 , final as (
  select
    cast(id as varchar)                                  as event_id,
    try(json_extract_scalar(repo, '$.name'))             as repo_name,
    coalesce(
      try(json_extract_scalar(org, '$.login')),
      case
        when try(json_extract_scalar(repo, '$.name')) is not null
          and length(try(json_extract_scalar(repo, '$.name'))) > 0
          and strpos(try(json_extract_scalar(repo, '$.name')), '/') > 0
        then split(try(json_extract_scalar(repo, '$.name')), '/')[1]
      end,
      organization
    )                                                    as organization,
    try(json_extract_scalar(actor, '$.login'))           as user_login,

    -- action
    json_extract_scalar(payload, '$.action')             as pr_action,

    -- pr meta
    try(cast(json_extract_scalar(payload, '$.pull_request.number') as integer)) as pr_number,
    json_extract_scalar(payload, '$.pull_request.title')           as pr_title,
    json_extract_scalar(payload, '$.pull_request.html_url')        as pr_html_url,
    json_extract_scalar(payload, '$.pull_request.state')           as pr_state,
    try(cast(json_extract_scalar(payload, '$.pull_request.draft') as boolean))  as is_draft,
    json_extract_scalar(payload, '$.pull_request.body')            as pr_body,

    json_extract_scalar(payload, '$.pull_request.user.login')      as pr_author_login,
    try(cast(json_extract_scalar(payload, '$.pull_request.user.id') as bigint)) as pr_author_id,

    json_extract_scalar(payload, '$.pull_request.head.ref')        as head_ref,
    json_extract_scalar(payload, '$.pull_request.base.ref')        as base_ref,
    json_extract_scalar(payload, '$.pull_request.head.sha')        as head_sha,
    json_extract_scalar(payload, '$.pull_request.base.sha')        as base_sha,

    try(cast(json_extract_scalar(payload, '$.pull_request.commits')       as integer)) as commits_count,
    try(cast(json_extract_scalar(payload, '$.pull_request.additions')     as integer)) as additions,
    try(cast(json_extract_scalar(payload, '$.pull_request.deletions')     as integer)) as deletions,
    try(cast(json_extract_scalar(payload, '$.pull_request.changed_files') as integer)) as changed_files,
    try(cast(json_extract_scalar(payload, '$.pull_request.comments')      as integer)) as issue_comments_count,
    try(cast(json_extract_scalar(payload, '$.pull_request.review_comments') as integer)) as review_comments_count,

    try(cast(json_extract_scalar(payload, '$.pull_request.merged') as boolean)) as is_merged,
    json_extract_scalar(payload, '$.pull_request.merge_commit_sha')        as merge_commit_sha,

    cast(ts_kst as timestamp(3) with time zone)          as ts_kst,
    cast(dt_kst as date)                                 as base_date
  from src
 
)

select *
from final
 


