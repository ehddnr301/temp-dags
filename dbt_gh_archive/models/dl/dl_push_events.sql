{{ config(
  materialized='incremental',
  incremental_strategy='append',
  on_schema_change='ignore',
  pre_hook=[
    "{{ delete_by_base_date() }}"
  ]
) }}

-- DL: PushEvent (커밋 단위로 펼친 뷰)
--  - payload.ref/commits를 기준으로 커밋 레벨 레코드
--  - 대시보드 최소 컬럼 + 푸시/커밋 메타 노출

with src as (
  select *
  from {{ source('delta_default', 'gh_archive_filtered') }}
  where cast(type as varchar) = 'PushEvent'
)

, commits as (
  select
    src.*,
    u.commit as commit
  from src
  cross join unnest(cast(json_extract(payload, '$.commits') as array(json))) as u(commit)
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

    -- push meta
    try(cast(json_extract_scalar(payload, '$.repository_id') as bigint))  as repository_id,
    try(cast(json_extract_scalar(payload, '$.push_id') as bigint))        as push_id,
    try(cast(json_extract_scalar(payload, '$.size') as integer))          as size,
    try(cast(json_extract_scalar(payload, '$.distinct_size') as integer)) as distinct_size,
    json_extract_scalar(payload, '$.ref')                                 as ref_full,
    regexp_replace(json_extract_scalar(payload, '$.ref'), '^(refs/heads/|refs/tags/)', '') as ref_name,
    case when json_extract_scalar(payload, '$.ref') like 'refs/heads/%' then true else false end as is_branch_ref,
    case when json_extract_scalar(payload, '$.ref') like 'refs/tags/%'  then true else false end as is_tag_ref,
    json_extract_scalar(payload, '$.head')                                as head_sha,
    json_extract_scalar(payload, '$.before')                              as before_sha,

    -- commit meta (expanded)
    json_extract_scalar(commit, '$.sha')                                  as commit_sha,
    json_extract_scalar(commit, '$.author.name')                          as commit_author_name,
    json_extract_scalar(commit, '$.author.email')                         as commit_author_email,
    json_extract_scalar(commit, '$.message')                              as commit_message,
    try(cast(json_extract_scalar(commit, '$.distinct') as boolean))       as commit_distinct,
    json_extract_scalar(commit, '$.url')                                  as commit_url,

    cast(ts_kst as timestamp(3) with time zone)          as ts_kst,
    cast(dt_kst as date)                                 as base_date
  from commits
  where cast(dt_kst as date) = {{ load_base_date_kst() }}
)

select *
from final
{% if is_incremental() %}
where not exists (
  select 1 from {{ this }} t
  where t.event_id = final.event_id
    and coalesce(t.commit_sha, '') = coalesce(final.commit_sha, '')
)
{% endif %}


