--
-- DW: 일자 × 조직 × 레포 × 이벤트 타입별 활동 카운트 사실 테이블
-- 목적
--   - GitHub Archive 이벤트를 일자(dt_kst)·조직·레포·이벤트 타입 단위로 집계
--   - 리포팅/DM의 기본 사실 테이블로 사용
-- 물리화(materialization)
--   - incremental: 매 실행 시 새로운 일자만 추가 적재
--   - unique_key: [dt_kst, organization, repo_name, event_type] 중복 방지(merge/upsert)
-- is_incremental()란?
--   - dbt 매크로로, 현재 실행이 증분 모드일 때 true
--   - 최초 생성/--full-refresh 시 false → 전체 재적재
{{ config(
  materialized='incremental',
  incremental_strategy='append',
  on_schema_change='ignore',
  on_table_exists='replace',
  pre_hook=[
    "{{ delete_by_base_date() }}"
  ],
  properties={
    "location": "'s3://gh-archive-delta/dw/dw_activity_daily'",
    "partitioned_by": "ARRAY['base_date']"
  }
) }}

with e as (
  select base_date, organization, repo_name, event_type, actor_login
  from {{ ref('stg_gh_events') }}
{% if is_incremental() %}
  where base_date = {{ load_base_date_kst() }}
{% endif %}
)

select
  base_date,
  organization,
  case
    when repo_name is not null and strpos(repo_name, '/') > 0 then split_part(repo_name, '/', 2)
    else repo_name
  end as repo_name,
  event_type,
  actor_login as user_login,
  count(*) as event_count
from e
group by 1,2,3,4,5


