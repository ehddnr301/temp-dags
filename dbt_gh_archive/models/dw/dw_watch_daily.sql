{{ config(materialized='incremental', unique_key=['base_date','organization','repo_name','user_login']) }}

-- DW: 일자 × 조직 × 레포 × 유저 단위 WatchEvent (started) 카운트

with base as (
  select base_date, organization, repo_name, user_login
  from {{ ref('dl_watch_events') }}
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
{% if is_incremental() %}
where base_date > (select coalesce(max(base_date), date '1900-01-01') from {{ this }})
{% endif %}
group by 1,2,3,4


