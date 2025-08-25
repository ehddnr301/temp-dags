# 새로운 대시보드 구상

┌──────────────────────────────────────────────────────────────────────────────┐
│ Filters                                                                      │
│ ─ Date Range ─ Org ─ Repo ─ User ─ Event Types [✓Push ✓PR ✓Issue …] ─ TZ     │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────┬──────────────┬──────────────┬──────────────┬───────────────┐
│    DAU       │     WAU      │     MAU      │ New Contribs │ Retention D1  │
│  (uniq user) │  (7d uniq)   │ (30d uniq)   │   (today)    │   (pct)       │
└──────────────┴──────────────┴──────────────┴──────────────┴───────────────┘

┌──────────────────────────────────────┬───────────────────────────────────────┐
│            DAU Trend (14/30/90d)     │  Event Type Trend (Stacked Area)      │
│  line: uniq(user) by day             │  area: users by type over time        │
└──────────────────────────────────────┴───────────────────────────────────────┘

┌───────────────────────────────┬──────────────────────────────────────────────┐
│ Top Repos by Contributors     │  Contributors by Repo (Bar)                 │
│ table: repo | contribs | PRs  │  bar: uniq(user) by repo (top N)            │
│       | pushes | issues       │                                             │
└───────────────────────────────┴──────────────────────────────────────────────┘

┌──────────────────────────────────────┬───────────────────────────────────────┐
│ Top Contributors (User Leaderboard)  │  User Engagement Funnel               │
│ table: user | pushes | PRs | issues  │ Push → PR → Review → Comment          │
│       | reviews | comments | total   │ step conv. rates + counts             │
└──────────────────────────────────────┴───────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│ Issue/PR Quality Slice (선택)                                              │
│ • PR LT (open→merged median) • Issue First-Response Time • Review Depth     │
└──────────────────────────────────────────────────────────────────────────────┘


## 데이터 소스 매핑 가이드

- **핵심 DM/DW 테이블**
  - `dm_user_daily_activity`: 일자 × 사용자 × 조직 기준 일일 활동 합계 및 가중치 점수
  - `dm_activity_daily_org_repo`: 일자 × 조직 × 레포 × 이벤트 타입별 카운트(필드: `base_date`, `organization`, `repo`, `event_type`, `event_count`)
  - `dm_repo_performance`: 조직 × 레포 성과 요약(기여자 수, 엔게이지먼트 스코어, 최근 30/60일 추세 등)
  - `dm_top_contributors`: 조직별 사용자 활동 합계/랭킹
  - `dm_user_engagement_funnel`: Push → PR → Review → Comment 퍼널(일자 × 조직)
  - `dm_pr_issue_quality`: PR/Issue 품질 지표(리뷰/댓글/커뮤니티 지표, quality_score 등)
  - `dw_activity_daily`: 원천 사실 테이블(일자 × 조직 × 레포 × 이벤트 타입 × 사용자)


## 공통 필터 바 설정

- **날짜**: `activity_date`(user 기반 지표) 또는 `base_date`(이벤트 기반 지표)
- **조직**: `org_login` 또는 `organization`
- **레포**: `repo_name`(DW) 또는 `repo`(DM)
- **사용자**: `user_login`
- **이벤트 타입**: `event_type`
- **타임존**: 소스는 일자 컬럼이 이미 정규화되어 있음. BI에서 날짜 위젯의 TZ만 통일 적용 권장

예시 WHERE 절 스니펫(필요에 맞게 치환):

```sql
-- 날짜/조직/레포/유저/이벤트 타입 공통 필터 예시
WHERE   activity_date BETWEEN DATE ':start_date' AND DATE ':end_date'
    AND (:org IS NULL OR org_login = :org)
    AND (:repo IS NULL OR repo_name = :repo)
    AND (:user IS NULL OR user_login = :user)
    AND (:event_types IS NULL OR event_type IN (:event_types))
```


## 상단 카드: DAU, WAU, MAU, New Contribs, D1 Retention

- **데이터 소스**: `dm_user_daily_activity`
- **차트**: 단일 KPI 카드 5개
- **권장 파라미터**: `:as_of_date`(기준일), 또는 현재 날짜

```sql
-- DAU / WAU / MAU (기준일 기준 윈도우)
WITH base AS (
  SELECT activity_date, user_login, org_login
  FROM dm_user_daily_activity
  WHERE activity_date <= DATE ':as_of_date'
    AND (:org IS NULL OR org_login = :org)
)
SELECT
  COUNT(DISTINCT CASE WHEN activity_date = DATE ':as_of_date' THEN user_login END) AS dau,
  COUNT(DISTINCT CASE WHEN activity_date BETWEEN DATE_ADD('day', -6, DATE ':as_of_date') AND DATE ':as_of_date' THEN user_login END) AS wau,
  COUNT(DISTINCT CASE WHEN activity_date BETWEEN DATE_ADD('day', -29, DATE ':as_of_date') AND DATE ':as_of_date' THEN user_login END) AS mau
FROM base;

-- New Contributors (기준일에 첫 활동한 사용자 수)
WITH first_activity AS (
  SELECT user_login, MIN(activity_date) AS first_date
  FROM dm_user_daily_activity
  WHERE (:org IS NULL OR org_login = :org)
  GROUP BY user_login
)
SELECT COUNT(*) AS new_contributors
FROM first_activity
WHERE first_date = DATE ':as_of_date';

-- D1 Retention (D-1 활동 사용자 중 D에도 활동한 비율)
WITH d0 AS (
  SELECT DISTINCT user_login
  FROM dm_user_daily_activity
  WHERE activity_date = DATE_ADD('day', -1, DATE ':as_of_date')
    AND (:org IS NULL OR org_login = :org)
), d1 AS (
  SELECT DISTINCT user_login
  FROM dm_user_daily_activity
  WHERE activity_date = DATE ':as_of_date'
    AND (:org IS NULL OR org_login = :org)
)
SELECT
  CASE WHEN COUNT(*) > 0 THEN ROUND(100.0 * (
    SELECT COUNT(*) FROM d0 JOIN d1 USING (user_login)
  ) / COUNT(*), 2) ELSE 0 END AS d1_retention_pct
FROM d0;
```


## DAU Trend (14/30/90일)

- **데이터 소스**: `dm_user_daily_activity`
- **차트**: 선형(Line). X: `activity_date`, Y: `COUNT(DISTINCT user_login)`

```sql
SELECT
  activity_date,
  COUNT(DISTINCT user_login) AS dau
FROM dm_user_daily_activity
WHERE activity_date BETWEEN DATE ':start_date' AND DATE ':end_date'
  AND (:org IS NULL OR org_login = :org)
GROUP BY activity_date
ORDER BY activity_date;
```


## Event Type Trend (Stacked Area)

- **데이터 소스**: `dm_activity_daily_org_repo`
- **차트**: 누적 면적(Stacked Area). X: `base_date`, Y: `SUM(event_count)`, 색상: `event_type`

```sql
SELECT
  base_date AS activity_date,
  event_type,
  SUM(event_count) AS events
FROM dm_activity_daily_org_repo
WHERE base_date BETWEEN DATE ':start_date' AND DATE ':end_date'
  AND (:org IS NULL OR organization = :org)
  AND (:repo IS NULL OR repo = :repo)
  AND (:event_types IS NULL OR event_type IN (:event_types))
GROUP BY 1, 2
ORDER BY 1;
```


## Top Repos by Contributors (Table)

- **데이터 소스(기간 한정 권장)**: `dw_activity_daily` 또는 `dm_activity_daily_org_repo`
- **차트**: 테이블. 컬럼: repo | contributors | PRs | pushes | issues
- `dm_repo_performance`는 전체기간 요약에 적합. 기간 필터가 필요하면 아래 동적 쿼리 사용 권장

```sql
SELECT
  organization AS org_login,
  repo_name,
  COUNT(DISTINCT user_login) AS contributors,
  SUM(CASE WHEN event_type = 'PullRequestEvent' THEN event_count ELSE 0 END) AS prs,
  SUM(CASE WHEN event_type = 'PushEvent' THEN event_count ELSE 0 END) AS pushes,
  SUM(CASE WHEN event_type = 'IssuesEvent' THEN event_count ELSE 0 END) AS issues
FROM dw_activity_daily
WHERE base_date BETWEEN DATE ':start_date' AND DATE ':end_date'
  AND (:org IS NULL OR organization = :org)
GROUP BY 1, 2
ORDER BY contributors DESC
LIMIT :top_n;
```


## Contributors by Repo (Bar)

- **데이터 소스**: 위 Top Repos용 집계 결과 재사용
- **차트**: 가로 막대(Bar). X: `contributors`, Y: `repo_name`

```sql
-- Top Repos 쿼리를 서브쿼리로 감싸 정렬만 조정
WITH repo_stats AS (
  SELECT
    repo_name,
    COUNT(DISTINCT user_login) AS contributors
  FROM dw_activity_daily
  WHERE base_date BETWEEN DATE ':start_date' AND DATE ':end_date'
    AND (:org IS NULL OR organization = :org)
  GROUP BY repo_name
)
SELECT *
FROM repo_stats
ORDER BY contributors DESC
LIMIT :top_n;
```


## Top Contributors (User Leaderboard)

- **데이터 소스(기간 한정 권장)**: `dm_user_daily_activity`
- **차트**: 테이블. 기본 정렬: `total_engagement_score` 또는 `pr_count`/`push_count`
- 참고: `dm_top_contributors`는 전체기간 랭킹. 기간 필터가 필요하면 아래 쿼리 권장

```sql
SELECT
  user_login,
  SUM(push_count)   AS pushes,
  SUM(pr_count)     AS prs,
  SUM(issue_count)  AS issues,
  SUM(review_count) AS reviews,
  SUM(comment_count) AS comments,
  SUM(star_count)   AS stars,
  SUM(fork_count)   AS forks,
  SUM(engagement_score) AS total_engagement_score
FROM dm_user_daily_activity
WHERE activity_date BETWEEN DATE ':start_date' AND DATE ':end_date'
  AND (:org IS NULL OR org_login = :org)
GROUP BY user_login
ORDER BY total_engagement_score DESC
LIMIT :top_n;
```


## User Engagement Funnel

- **데이터 소스**: `dm_user_engagement_funnel`
- **차트**: 퍼널 또는 누적 막대. 기간 합산 후 전환율 재계산

```sql
WITH agg AS (
  SELECT
    COALESCE(:org, 'ALL') AS org_key,
    SUM(total_active_users) AS total_active_users,
    SUM(users_with_push)    AS users_with_push,
    SUM(users_push_to_pr)   AS users_push_to_pr,
    SUM(users_pr_to_review) AS users_pr_to_review,
    SUM(users_full_funnel)  AS users_full_funnel
  FROM dm_user_engagement_funnel
  WHERE activity_date BETWEEN DATE ':start_date' AND DATE ':end_date'
    AND (:org IS NULL OR org_login = :org)
)
SELECT
  total_active_users,
  users_with_push,
  users_push_to_pr,
  users_pr_to_review,
  users_full_funnel,
  CASE WHEN total_active_users > 0 THEN ROUND(100.0 * users_with_push / total_active_users, 2) ELSE 0 END AS conv_active_to_push,
  CASE WHEN users_with_push > 0 THEN ROUND(100.0 * users_push_to_pr / users_with_push, 2) ELSE 0 END AS conv_push_to_pr,
  CASE WHEN users_push_to_pr > 0 THEN ROUND(100.0 * users_pr_to_review / users_push_to_pr, 2) ELSE 0 END AS conv_pr_to_review,
  CASE WHEN users_pr_to_review > 0 THEN ROUND(100.0 * users_full_funnel / users_pr_to_review, 2) ELSE 0 END AS conv_review_to_comment
FROM agg;
```


## Issue/PR Quality Slice

- **데이터 소스**: `dm_pr_issue_quality`
- **차트**:
  - 테이블: `repo_name`, `avg_reviews_per_pr`, `avg_comments_per_issue`, `reviewer_to_contributor_ratio`, `community_engagement_pct`, `avg_activity_per_day`, `quality_score`, `maturity_level`, `activity_status`
  - 보조 차트: Radar 또는 Bar(품질 지표), Scatter(quality_score vs total_prs)

```sql
SELECT
  org_login,
  repo_name,
  total_prs,
  total_issues,
  total_reviews,
  total_comments,
  avg_reviews_per_pr,
  avg_comments_per_issue,
  reviewer_to_contributor_ratio,
  community_engagement_pct,
  avg_activity_per_day,
  quality_score,
  maturity_level,
  activity_status
FROM dm_pr_issue_quality
WHERE (:org IS NULL OR org_login = :org)
  AND (:repo IS NULL OR repo_name = :repo)
ORDER BY quality_score DESC, total_prs DESC
LIMIT :top_n;
```


## Repo Performance (성장/최근성 지표 활용)

- **데이터 소스**: `dm_repo_performance`
- **차트**:
  - 테이블: `repo_name`, `contributor_count`, `recent_contributors`, `previous_contributors`, `contributor_growth_pct`, `recently_active`, `total_prs`, `total_stars`, `avg_engagement_per_contributor`
  - Bar/Arrow: 성장률(`contributor_growth_pct`, `engagement_growth_pct`)

```sql
SELECT
  org_login,
  repo_name,
  contributor_count,
  recent_contributors,
  previous_contributors,
  contributor_growth_pct,
  recent_engagement,
  previous_engagement,
  engagement_growth_pct,
  total_prs,
  total_stars,
  avg_engagement_per_contributor,
  recently_active
FROM dm_repo_performance
WHERE (:org IS NULL OR org_login = :org)
ORDER BY contributor_growth_pct DESC NULLS LAST, contributor_count DESC
LIMIT :top_n;
```


## 구현 팁

- **스키마 접두사**: BI에서 직접 쿼리 시 `schema.table` 형식으로 모델명을 교체하세요.
- **파라미터**: `:start_date`, `:end_date`, `:as_of_date`, `:org`, `:repo`, `:user`, `:event_types`, `:top_n` 등은 BI의 변수/컨트롤과 연결.
- **성능**: 기간 상단/좌측 위젯은 최근 90일로 제한, 테이블은 `LIMIT :top_n` 권장.
- **정렬**: 랭킹 테이블은 기본 정렬 기준을 명확히(예: 참여도 → `total_engagement_score`, 품질 → `quality_score`).

# 구상해본 DW

```dw_user_daily_activity
SELECT
    dt_kst AS event_date,   -- KST
    try(json_extract_scalar(actor, '$.login'))        AS user_login,
    type               AS event_type,
    organization,
    try(json_extract_scalar(repo, '$.name'))          AS repo_name,
    COUNT(*)           AS activity_count
from gh_archive_filtered 
GROUP BY 1,2,3,4,5
```

