# GitHub Archive Dashboard Implementation Guide

이 문서는 NEW.md에서 제안한 대시보드 구상을 구현하기 위해 생성된 dbt 모델들의 사용법을 안내합니다.

## 📊 대시보드 구성 요소별 모델 매핑

### 1. 상단 필터 및 핵심 메트릭 패널
**사용 모델:** `dm_user_metrics`

```sql
-- DAU/WAU/MAU, 신규 기여자, 리텐션 D1
SELECT 
    activity_date,
    daily_active_users,
    weekly_active_users,
    monthly_active_users,
    new_contributors,
    retention_d1_pct
FROM dm_user_metrics
WHERE activity_date >= '2024-01-01'
ORDER BY activity_date DESC;
```

### 2. DAU 트렌드 차트
**사용 모델:** `dm_user_metrics`

```sql
-- 14/30/90일 DAU 트렌드
SELECT 
    activity_date,
    daily_active_users
FROM dm_user_metrics
WHERE activity_date >= CURRENT_DATE() - INTERVAL 90 DAY
ORDER BY activity_date;
```

### 3. 이벤트 타입별 트렌드 (스택드 영역 차트)
**사용 모델:** `dm_user_daily_activity`

```sql
-- 이벤트 타입별 사용자 수 시계열
SELECT 
    activity_date,
    org_login,
    COUNT(DISTINCT CASE WHEN push_count > 0 THEN user_login END) as push_users,
    COUNT(DISTINCT CASE WHEN pr_count > 0 THEN user_login END) as pr_users,
    COUNT(DISTINCT CASE WHEN review_count > 0 THEN user_login END) as review_users,
    COUNT(DISTINCT CASE WHEN comment_count > 0 THEN user_login END) as comment_users,
    COUNT(DISTINCT CASE WHEN issue_count > 0 THEN user_login END) as issue_users
FROM dm_user_daily_activity
GROUP BY activity_date, org_login
ORDER BY activity_date;
```

### 4. 상위 저장소 (기여자별)
**사용 모델:** `dm_repo_performance`

```sql
-- 기여자 수 기준 상위 저장소
SELECT 
    repo_name,
    contributor_count,
    total_prs,
    total_pushes,
    total_issues,
    repo_category,
    contributor_growth_pct
FROM dm_repo_performance
WHERE org_login = 'your_org'
ORDER BY contributors_rank
LIMIT 10;
```

### 5. 기여자별 저장소 기여도 (바 차트)
**사용 모델:** `dm_repo_performance`

```sql
-- 저장소별 기여자 수 (상위 N개)
SELECT 
    repo_name,
    contributor_count
FROM dm_repo_performance
WHERE org_login = 'your_org'
ORDER BY contributor_count DESC
LIMIT 20;
```

### 6. 상위 기여자 (사용자 리더보드)
**사용 모델:** `dm_top_contributors`

```sql
-- 기여자 리더보드
SELECT 
    user_login,
    total_pushes,
    total_prs,
    total_issues,
    total_reviews,
    total_comments,
    total_engagement_score,
    contributor_type,
    recently_active
FROM dm_top_contributors
WHERE org_login = 'your_org'
ORDER BY engagement_rank
LIMIT 50;
```

### 7. 사용자 참여 퍼널
**사용 모델:** `dm_user_engagement_funnel`

```sql
-- Push → PR → Review → Comment 퍼널
SELECT 
    activity_date,
    total_active_users,
    users_with_push,
    users_push_to_pr,
    users_pr_to_review,
    users_full_funnel,
    conv_active_to_push,
    conv_push_to_pr,
    conv_pr_to_review,
    conv_review_to_comment,
    conv_overall_funnel,
    funnel_health_score
FROM dm_user_engagement_funnel
WHERE org_login = 'your_org'
ORDER BY activity_date DESC;
```

### 8. Issue/PR 품질 지표 (선택적)
**사용 모델:** `dm_pr_issue_quality`

```sql
-- PR/Issue 품질 메트릭
SELECT 
    repo_name,
    avg_reviews_per_pr,
    avg_comments_per_issue,
    quality_score,
    maturity_level,
    review_health,
    community_engagement_pct
FROM dm_pr_issue_quality
WHERE org_login = 'your_org'
ORDER BY quality_score DESC;
```

## 🎯 대시보드 구현 팁

### 필터 구현
- **날짜 범위:** 모든 모델에서 `activity_date` 필드 사용
- **조직:** `org_login` 필드로 필터링
- **저장소:** `repo_name` 필드로 필터링 (해당하는 모델에서)
- **사용자:** `user_login` 필드로 필터링 (해당하는 모델에서)
- **이벤트 타입:** `dm_user_daily_activity`에서 각 이벤트 카운트 필드 활용

### 성능 최적화
1. **파티셔닝:** 모든 모델이 `activity_date`로 파티션되어 있음
2. **클러스터링:** 주요 필터 조건에 따라 클러스터링 적용됨
3. **집계 테이블:** 모든 모델이 사전 집계되어 쿼리 성능 최적화

### 실시간 업데이트
- 모델들은 일별 배치로 업데이트되도록 설계됨
- 실시간 대시보드가 필요한 경우 증분 업데이트 전략 고려

## 🔄 모델 의존성
```
dw_activity_daily (base)
    ↓
dm_user_daily_activity
    ↓
├── dm_user_metrics
├── dm_top_contributors  
├── dm_repo_performance
├── dm_user_engagement_funnel
└── dm_pr_issue_quality
```

## 📈 추가 메트릭 아이디어

### 커뮤니티 건강도 지표
- 신규 vs 기존 기여자 비율
- 기여자 다양성 지수
- 기여 패턴 분석 (버스트 vs 지속적)

### 프로젝트 성숙도 지표  
- 코드 리뷰 커버리지
- 문서화 지표
- 테스트 커버리지 (추가 데이터 필요)

### 협업 지표
- 크로스 저장소 기여자
- 멘토링 관계 분석
- 지식 공유 패턴

이 모델들을 사용하여 Apache Superset, Grafana, 또는 다른 BI 도구에서 강력한 GitHub Archive 분석 대시보드를 구축할 수 있습니다.