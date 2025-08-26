{{ config(materialized='table') }}

-- 조직 필터용 DIM: 단일 컬럼
select distinct
  organization
from {{ ref('dw_activity_daily') }}
where organization is not null

