{{ config(
  materialized='incremental',
  incremental_strategy='append',
  on_schema_change='ignore',
  pre_hook=[
    "{{ delete_by_base_date() }}"
  ]
) }}

-- DL: GollumEvent 원천 필터 뷰 (payload.pages 배열을 행으로 확장)

with src as (
  select *
  from {{ source('delta_default', 'gh_archive_filtered') }}
  where cast(type as varchar) = 'GollumEvent'
)

, pages as (
  select
    src.*,
    u.page as page
  from src
  cross join unnest(cast(json_extract(payload, '$.pages') as array(json))) as u(page)
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

    json_extract_scalar(page, '$.page_name')             as page_name,
    json_extract_scalar(page, '$.title')                 as page_title,
    json_extract_scalar(page, '$.summary')               as page_summary,
    json_extract_scalar(page, '$.action')                as page_action,
    json_extract_scalar(page, '$.sha')                   as page_sha,
    json_extract_scalar(page, '$.html_url')              as page_html_url,

    case json_extract_scalar(page, '$.action') when 'created' then true else false end as is_page_created,
    case json_extract_scalar(page, '$.action') when 'edited'  then true else false end as is_page_edited,
    case json_extract_scalar(page, '$.action') when 'deleted' then true else false end as is_page_deleted,

    ts_kst,
    cast(dt_kst as date)                                 as base_date
  from pages
  where cast(dt_kst as date) = {{ load_base_date_kst() }}
)

select *
from final
{% if is_incremental() %}
where not exists (
  select 1 from {{ this }} t
  where t.event_id = final.event_id
    and coalesce(t.page_sha, '') = coalesce(final.page_sha, '')
)
{% endif %}


