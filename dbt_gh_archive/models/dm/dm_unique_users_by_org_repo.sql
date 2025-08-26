{{ config(materialized='table') }}

-- DM: 조직 × 레포별 유니크한 유저 목록
-- 소스: dw_activity_daily (일자별 집계 사실 테이블)

with base as (
  select organization, repo_name, user_login
  from {{ ref('dw_activity_daily') }}
)

select distinct
  organization,
  repo_name as repo,
  user_login
from base


