{{ config(
  materialized='incremental',
  incremental_strategy='append',
  on_schema_change='ignore',
  on_table_exists='replace',
  pre_hook=[
    "{{ delete_by_base_date() }}"
  ],
  properties={
    "location": "'s3://gh-archive-delta/dl/dl_member_events'",
    "partitioned_by": "ARRAY['base_date']"
  }
) }}

-- DL: MemberEvent 원천 필터 뷰

with src as (
  select *
  from {{ source('delta_default', 'gh_archive_filtered') }}
  where cast(type as varchar) = 'MemberEvent'
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

    json_extract_scalar(payload, '$.member.login')       as member_login,
    try(cast(json_extract_scalar(payload, '$.member.id') as bigint)) as member_id,
    json_extract_scalar(payload, '$.member.html_url')    as member_html_url,
    json_extract_scalar(payload, '$.action')             as member_action,
    case json_extract_scalar(payload, '$.action') when 'added' then true else false end   as is_member_added,
    case json_extract_scalar(payload, '$.action') when 'removed' then true else false end as is_member_removed,

    cast(ts_kst as timestamp(3) with time zone)          as ts_kst,
    cast(dt_kst as date)                                 as base_date
  from src
 
)

select *
from final
 


