{{ config(materialized='table') }}

-- 레포 DIM: organization 포함 + 레포 최초 등장일(first_seen_date)
with base as (
  select
    organization,
    case
      when repo_name is not null and strpos(repo_name, '/') > 0 then split_part(repo_name, '/', 2)
      else repo_name
    end as repo,
    min(base_date) as first_seen_date
  from {{ ref('dw_activity_daily') }}
  where repo_name is not null and organization is not null
  group by 1, 2
)

select
  b.organization,
  b.repo AS repo_name,
  b.first_seen_date
from base b

