{{ config(materialized='table') }}

-- 이벤트 타입 필터용 DIM: 단일 컬럼
select distinct
  event_type
from {{ ref('dw_activity_daily') }}
where event_type is not null

