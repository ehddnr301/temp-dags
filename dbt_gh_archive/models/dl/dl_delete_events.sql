{{ config(
  materialized='incremental',
  incremental_strategy='append',
  on_schema_change='ignore',
  pre_hook=[
    "{{ delete_by_base_date() }}"
  ]
) }}

-- DL: DeleteEvent 원천 필터 뷰

with src as (
  select *
  from {{ source('delta_default', 'gh_archive_filtered') }}
  where cast(type as varchar) = 'DeleteEvent'
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

    json_extract_scalar(payload, '$.ref_type')           as deleted_ref_type,
    json_extract_scalar(payload, '$.ref')                as deleted_ref_name,
    json_extract_scalar(payload, '$.pusher_type')        as pusher_type,

    case json_extract_scalar(payload, '$.ref_type') when 'branch' then true else false end as is_branch_deletion,
    case json_extract_scalar(payload, '$.ref_type') when 'tag'    then true else false end as is_tag_deletion,

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


