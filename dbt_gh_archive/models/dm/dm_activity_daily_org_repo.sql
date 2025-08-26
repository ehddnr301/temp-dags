{{ config(materialized='view') }}

-- DM: 일자 × 조직 × 레포 × 이벤트 타입별 카운트 상세 뷰

select
  base_date,
  organization,
  repo_name as repo,
  event_type,
  event_count
from {{ ref('dw_activity_daily') }}


