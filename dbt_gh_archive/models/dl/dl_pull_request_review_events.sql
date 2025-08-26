{{ config(
  materialized='incremental',
  incremental_strategy='append',
  on_schema_change='ignore',
  pre_hook=[
    "{{ delete_by_base_date() }}"
  ]
) }}

-- DL: PullRequestReviewEvent 원천 필터 뷰

with src as (
  select *
  from {{ source('delta_default', 'gh_archive_filtered') }}
  where cast(type as varchar) = 'PullRequestReviewEvent'
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

    json_extract_scalar(payload, '$.review.user.login')  as reviewer_login,
    try(cast(json_extract_scalar(payload, '$.review.user.id') as bigint)) as reviewer_id,
    json_extract_scalar(payload, '$.review.state')       as review_state,
    json_extract_scalar(payload, '$.review.body')        as review_body,
    json_extract_scalar(payload, '$.review.html_url')    as review_html_url,
    json_extract_scalar(payload, '$.review.commit_id')   as commit_sha,

    try(json_extract_scalar(payload, '$.review.submitted_at')) as review_submitted_at_utc,
    try(json_extract_scalar(payload, '$.review.submitted_at')) as review_submitted_at_kst,

    try(cast(json_extract_scalar(payload, '$.pull_request.number') as integer)) as pr_number,
    json_extract_scalar(payload, '$.pull_request.title')      as pr_title,
    json_extract_scalar(payload, '$.pull_request.html_url')   as pr_html_url,
    json_extract_scalar(payload, '$.pull_request.user.login') as pr_author_login,

    cast(ts_kst as timestamp(3) with time zone)          as ts_kst,
    cast(dt_kst as date)                                 as base_date
  from src
  where cast(dt_kst as date) = {{ load_base_date_kst() }}
)

select *
from final
{% if is_incremental() %}
where not exists (
  select 1 from {{ this }} t
  where t.event_id = final.event_id
)
{% endif %}


