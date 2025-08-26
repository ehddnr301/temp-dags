{{ config(
  materialized='incremental',
  incremental_strategy='append',
  on_schema_change='ignore',
  pre_hook=[
    "{{ delete_by_base_date() }}"
  ]
) }}

-- DL: ForkEvent 원천 필터 뷰

with src as (
  select *
  from {{ source('delta_default', 'gh_archive_filtered') }}
  where cast(type as varchar) = 'ForkEvent'
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

    -- forkee meta
    try(cast(json_extract_scalar(payload, '$.forkee.id') as bigint))       as forkee_id,
    json_extract_scalar(payload, '$.forkee.full_name')                     as forkee_full_name,
    json_extract_scalar(payload, '$.forkee.html_url')                      as forkee_html_url,
    json_extract_scalar(payload, '$.forkee.owner.login')                   as forkee_owner_login,
    try(cast(json_extract_scalar(payload, '$.forkee.owner.id') as bigint)) as forkee_owner_id,
    json_extract_scalar(payload, '$.forkee.description')                   as forkee_description,
    try(cast(json_extract_scalar(payload, '$.forkee.private') as boolean)) as is_fork_private,
    json_extract_scalar(payload, '$.forkee.default_branch')                as forkee_default_branch,

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


