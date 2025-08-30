{{ config(
  materialized='incremental',
  incremental_strategy='append',
  on_schema_change='ignore',
  on_table_exists='replace',
  pre_hook=[
    "{{ delete_by_base_date() }}"
  ],
  properties={
    "location": "'s3://gh-archive-delta/dl/dl_pull_request_review_comment_events'",
    "partitioned_by": "ARRAY['base_date']"
  }
) }}

-- DL: PullRequestReviewCommentEvent 원천 필터 뷰

with src as (
  select *
  from {{ source('delta_default', 'gh_archive_filtered') }}
  where cast(type as varchar) = 'PullRequestReviewCommentEvent'
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

    json_extract_scalar(payload, '$.action')             as pr_review_comment_action,
    try(cast(json_extract_scalar(payload, '$.comment.id') as bigint))        as review_comment_id,
    json_extract_scalar(payload, '$.comment.html_url')                       as review_comment_html_url,
    json_extract_scalar(payload, '$.comment.body')                           as review_comment_body,
    json_extract_scalar(payload, '$.comment.diff_hunk')                      as diff_hunk,
    json_extract_scalar(payload, '$.comment.path')                           as file_path,
    json_extract_scalar(payload, '$.comment.commit_id')                      as commit_id,
    try(cast(json_extract_scalar(payload, '$.comment.pull_request_review_id') as bigint)) as review_id,

    json_extract_scalar(payload, '$.comment.user.login')      as commenter_login,
    try(cast(json_extract_scalar(payload, '$.comment.user.id') as bigint))   as commenter_id,

    try(cast(json_extract_scalar(payload, '$.pull_request.number') as integer)) as pr_number,
    json_extract_scalar(payload, '$.pull_request.title')      as pr_title,
    json_extract_scalar(payload, '$.pull_request.html_url')   as pr_html_url,
    json_extract_scalar(payload, '$.pull_request.state')      as pr_state,

    cast(ts_kst as timestamp(3) with time zone)          as ts_kst,
    cast(dt_kst as date)                                 as base_date
  from src
 
)

select *
from final


