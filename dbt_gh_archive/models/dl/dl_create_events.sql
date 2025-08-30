{{ config(
  materialized='incremental',
  incremental_strategy='append',
  on_schema_change='ignore',
  on_table_exists='replace',
  pre_hook=[
    "{{ delete_by_base_date() }}"
  ],
  properties={
    "location": "'s3://gh-archive-delta/dl/dl_create_events'",
    "partitioned_by": "ARRAY['base_date']"
  }
) }}

-- DL: CreateEvent 원천 필터 뷰

with src as (
  select *
  from {{ source('delta_default', 'gh_archive_filtered') }}
  where cast(type as varchar) = 'CreateEvent'
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

    json_extract_scalar(payload, '$.ref_type')           as created_ref_type,
    json_extract_scalar(payload, '$.ref')                as created_ref_name,
    json_extract_scalar(payload, '$.master_branch')      as default_branch,
    json_extract_scalar(payload, '$.description')        as repo_description,
    json_extract_scalar(payload, '$.pusher_type')        as pusher_type,

    case json_extract_scalar(payload, '$.ref_type') when 'repository' then true else false end as is_repo_creation,
    case json_extract_scalar(payload, '$.ref_type') when 'branch'     then true else false end as is_branch_creation,
    case json_extract_scalar(payload, '$.ref_type') when 'tag'        then true else false end as is_tag_creation,

    cast(ts_kst as timestamp(3) with time zone)          as ts_kst,
    cast(dt_kst as date)                                 as base_date
  from src
)

select *
from final
 


