{{ config(materialized='incremental', unique_key='event_id') }}

-- GitHub Archive 이벤트 표준화 스테이징
-- 통합 테이블에서 dt_kst와 organization 파티셔닝을 활용
-- actor, repo, org는 딕셔너리 형태로 저장되어 있음

with src as (
  select * from {{ source('delta_default', 'gh_archive_filtered') }}
  {% if is_incremental() %}
  where cast(dt_kst as date) > (
    select coalesce(max(dt_kst), date '1900-01-01') from {{ this }}
  )
  {% endif %}
),

normalized as (
  select
    cast(id as varchar)                                as event_id,
    cast(type as varchar)                              as event_type,
    
    -- 디버깅을 위한 원본 데이터
    repo                                                as repo_raw,
    org                                                 as org_raw,
    actor                                               as actor_raw,
    
    -- repo.name 추출 (딕셔너리에서 직접 추출)
    try(json_extract_scalar(repo, '$.name'))           as repo_name_simple,
    
    -- org.login 추출 (딕셔너리에서 직접 추출)
    try(json_extract_scalar(org, '$.login'))           as org_login_simple,
    
    -- actor.login 추출 (딕셔너리에서 직접 추출)
    try(json_extract_scalar(actor, '$.login'))         as actor_login_simple,
    
    ts_kst,
    cast(dt_kst as date)                               as base_date,
    organization
  from src
)

select
  event_id,
  event_type,
  repo_name_simple                                     as repo_name,
  coalesce(
    org_login_simple,
    case when repo_name_simple is not null and length(repo_name_simple) > 0 and strpos(repo_name_simple, '/') > 0 then split(repo_name_simple, '/')[1] end,
    organization
  ) as organization,
  actor_login_simple                                   as actor_login,
  ts_kst,
  base_date
from normalized