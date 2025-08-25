{{ config(materialized='view') }}

-- DM: Heatmap용 일자 × 조직 × 레포 단위 이벤트 합계와 요일 라벨

with base as (
  select base_date, organization, repo_name, event_type, event_count
  from {{ ref('dw_activity_daily') }}
)

select
  base_date,
  organization,
  repo_name as repo,
  event_type,
  sum(event_count) as events,
  day_of_week(base_date) as dow_num,
  format_datetime(cast(base_date as timestamp), 'EEE') as dow_en,
  case day_of_week(base_date)
    when 1 then '월' when 2 then '화' when 3 then '수'
    when 4 then '목' when 5 then '금' when 6 then '토' when 7 then '일'
  end as dow_ko
from base
group by base_date, organization, repo_name, event_type



