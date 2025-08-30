{{ config(
  materialized='incremental',
  incremental_strategy='append',
  on_schema_change='ignore',
  on_table_exists='replace',
  pre_hook=[
    "{{ delete_by_base_date() }}"
  ],
  properties={
    "location": "'s3://gh-archive-delta/dw/dw_watch_daily'",
    "partitioned_by": "ARRAY['base_date']"
  }
) }}

-- DW: 일자 × 조직 × 레포 × 유저 단위 WatchEvent (started) 카운트

with base as (
  select base_date, organization, repo_name, user_login
  from {{ ref('dl_watch_events') }}
{% if is_incremental() %}
  where base_date = {{ load_base_date_kst() }}
{% endif %}
)

select
  base_date,
  organization,
  case
    when repo_name is not null and strpos(repo_name, '/') > 0 then split_part(repo_name, '/', 2)
    else repo_name
  end as repo_name,
  user_login,
  count(*) as watch_started_count
from base
group by 1,2,3,4


