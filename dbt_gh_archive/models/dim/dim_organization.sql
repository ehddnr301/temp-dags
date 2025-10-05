{{ config(materialized='table') }}

-- 조직 DIM: 조직별 최초 등장일(first_seen_date) 포함
with base as (
  select organization, min(base_date) as first_seen_date
  from {{ ref('dw_activity_daily') }}
  where organization is not null
  group by 1
)

select
  b.organization,
  b.first_seen_date
from base b

