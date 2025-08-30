{{ config(
  materialized='incremental',
  incremental_strategy='append',
  on_schema_change='ignore',
  on_table_exists='replace',
  pre_hook=[
    "{{ delete_by_base_date() }}"
  ],
  properties={
    "location": "'s3://gh-archive-delta/dl/dl_watch_events'",
    "partitioned_by": "ARRAY['base_date']"
  }
) }}

-- DL: WatchEvent 원천 필터 뷰 (공통 DL 패턴 적용)

with src as (
  select *
  from {{ source('delta_default', 'gh_archive_filtered') }}
  where cast(type as varchar) = 'WatchEvent'
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
    cast(ts_kst as timestamp(3) with time zone)          as ts_kst,
    cast(dt_kst as date)                                 as base_date
  from src
 
)

select *
from final
 


