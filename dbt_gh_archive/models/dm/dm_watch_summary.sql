{{ config(
  materialized='incremental',
  incremental_strategy='append',
  on_schema_change='ignore',
  on_table_exists='replace',
  pre_hook=[
    "{{ delete_by_base_date() }}"
  ],
  properties={
    "location": "'s3://gh-archive-delta/dm/dm_watch_summary'",
    "partitioned_by": "ARRAY['base_date']"
  }
) }}

-- DM: 조직 × 레포 × 일자별 신규 Watch(=Star) 사용자 수

with base as (
  select base_date, organization, repo_name, user_login
  from {{ ref('dw_watch_daily') }}
{% if is_incremental() %}
  where base_date = {{ load_base_date_kst() }}
{% endif %}
)

select
  base_date,
  organization,
  repo_name as repo,
  count(distinct user_login) as unique_watchers
from base
group by 1,2,3
{% if is_incremental() %}
-- unique_key 대신 delete+insert 보장으로 중복 제거, 추가 where 필요 없음
{% endif %}


