{{ config(materialized='table') }}

-- 레포 필터용 DIM: organization 포함
select distinct
  organization,
  case
    when repo_name is not null and strpos(repo_name, '/') > 0 then split_part(repo_name, '/', 2)
    else repo_name
  end as repo
from {{ ref('dw_activity_daily') }}
where repo_name is not null and organization is not null

