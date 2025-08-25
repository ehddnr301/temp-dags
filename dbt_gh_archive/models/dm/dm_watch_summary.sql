{{ config(materialized='table') }}

-- DM: 조직 × 레포 × 일자별 신규 Watch(=Star) 사용자 수

with base as (
  select base_date, organization, repo_name, user_login
  from {{ ref('dw_watch_daily') }}
)

select
  base_date,
  organization,
  repo_name as repo,
  count(distinct user_login) as unique_watchers
from base
group by 1,2,3


