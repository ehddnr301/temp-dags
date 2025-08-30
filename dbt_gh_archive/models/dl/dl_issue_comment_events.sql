{{ config(
  materialized='incremental',
  incremental_strategy='append',
  on_schema_change='ignore',
  on_table_exists='replace',
  pre_hook=[
    "{{ delete_by_base_date() }}"
  ],
  properties={
    "location": "'s3://gh-archive-delta/dl/dl_issue_comment_events'",
    "partitioned_by": "ARRAY['base_date']"
  }
) }}

-- DL: IssueCommentEvent 원천 필터 뷰

with src as (
  select *
  from {{ source('delta_default', 'gh_archive_filtered') }}
  where cast(type as varchar) = 'IssueCommentEvent'
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

    json_extract_scalar(payload, '$.action')             as ic_action,

    try(cast(json_extract_scalar(payload, '$.issue.number') as integer)) as issue_number,
    json_extract_scalar(payload, '$.issue.title')         as issue_title,
    json_extract_scalar(payload, '$.issue.state')         as issue_state,
    json_extract_scalar(payload, '$.issue.html_url')      as issue_html_url,

    json_extract_scalar(payload, '$.issue.user.login')    as issue_author_login,
    try(cast(json_extract_scalar(payload, '$.issue.user.id') as bigint)) as issue_author_id,

    try(cast(json_extract_scalar(payload, '$.comment.id') as bigint))    as comment_id,
    json_extract_scalar(payload, '$.comment.html_url')    as comment_html_url,
    json_extract_scalar(payload, '$.comment.body')        as comment_body,

    json_extract_scalar(payload, '$.comment.user.login')  as commenter_login,
    try(cast(json_extract_scalar(payload, '$.comment.user.id') as bigint)) as commenter_id,

    cast(ts_kst as timestamp(3) with time zone)          as ts_kst,
    cast(dt_kst as date)                                 as base_date
  from src
 
)

select *
from final
 


