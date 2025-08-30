-- 델타 테이블을 Trino에 등록
-- 통합 GitHub Archive 테이블을 dt_kst와 organization으로 파티셔닝하여 등록

-- 기존 테이블이 있다면 제거
CALL delta.system.unregister_table(
    schema_name => 'default',
    table_name => 'gh_archive_filtered'
);

CALL delta.system.unregister_table(
    schema_name => 'default',
    table_name => 'gh_user_relations'
);

CALL delta.system.unregister_table(
    schema_name => 'default',
    table_name => 'dl_actor_week'
);

CALL delta.system.unregister_table(
    schema_name => 'default',
    table_name => 'dl_repo_week'
);

CALL delta.system.unregister_table(
    schema_name => 'default',
    table_name => 'dl_watch_events'
);

CALL delta.system.unregister_table(
    schema_name => 'default',
    table_name => 'dl_push_events'
);

CALL delta.system.unregister_table(
    schema_name => 'default',
    table_name => 'dl_issues_events'
);

CALL delta.system.unregister_table(
    schema_name => 'default',
    table_name => 'dl_issue_comment_events'
);

CALL delta.system.unregister_table(
    schema_name => 'default',
    table_name => 'dl_pull_request_events'
);

CALL delta.system.unregister_table(
    schema_name => 'default',
    table_name => 'dl_pull_request_review_events'
);

CALL delta.system.unregister_table(
    schema_name => 'default',
    table_name => 'dl_pull_request_review_comment_events'
);

CALL delta.system.unregister_table(
    schema_name => 'default',
    table_name => 'dl_fork_events'
);

CALL delta.system.unregister_table(
    schema_name => 'default',
    table_name => 'dl_member_events'
);

CALL delta.system.unregister_table(
    schema_name => 'default',
    table_name => 'dl_delete_events'
);

CALL delta.system.unregister_table(
    schema_name => 'default',
    table_name => 'dl_create_events'
);

CALL delta.system.unregister_table(
    schema_name => 'default',
    table_name => 'dl_gollum_events'
);

CALL delta.system.unregister_table(
    schema_name => 'default',
    table_name => 'dw_activity_feed'
);

-- 새로 등록 (기본 설정)
CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'gh_archive_filtered',
    table_location => 's3://gh-archive-delta/gh_archive_filtered'
);

CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'gh_user_relations',
    table_location => 's3://gh-archive-delta/gh_user_relations'
);

CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'dl_actor_week',
    table_location => 's3://gh-archive-delta/dl_actor_week'
);

CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'dl_repo_week',
    table_location => 's3://gh-archive-delta/dl_repo_week'
);

CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'dl_watch_events',
    table_location => 's3://gh-archive-delta/dl/dl_watch_events'
);

CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'dl_push_events',
    table_location => 's3://gh-archive-delta/dl/dl_push_events'
);

CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'dl_issues_events',
    table_location => 's3://gh-archive-delta/dl/dl_issues_events'
);

CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'dl_issue_comment_events',
    table_location => 's3://gh-archive-delta/dl/dl_issue_comment_events'
);

CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'dl_pull_request_events',
    table_location => 's3://gh-archive-delta/dl/dl_pull_request_events'
);

CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'dl_pull_request_review_events',
    table_location => 's3://gh-archive-delta/dl/dl_pull_request_review_events'
);

CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'dl_pull_request_review_comment_events',
    table_location => 's3://gh-archive-delta/dl/dl_pull_request_review_comment_events'
);

CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'dl_fork_events',
    table_location => 's3://gh-archive-delta/dl/dl_fork_events'
);

CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'dl_member_events',
    table_location => 's3://gh-archive-delta/dl/dl_member_events'
);

CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'dl_delete_events',
    table_location => 's3://gh-archive-delta/dl/dl_delete_events'
);

CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'dl_create_events',
    table_location => 's3://gh-archive-delta/dl/dl_create_events'
);

CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'dl_gollum_events',
    table_location => 's3://gh-archive-delta/dl/dl_gollum_events'
);

CALL delta.system.register_table(
    schema_name => 'default',
    table_name => 'dw_activity_feed',
    table_location => 's3://gh-archive-delta/dw/dw_activity_feed'
);

-- 참고: Delta Lake에서 JSON 컬럼을 자동으로 인식하지 못하는 경우
-- 다음과 같은 대안을 시도할 수 있습니다:
-- 1. Delta Lake 파일을 다시 생성할 때 JSON 컬럼으로 저장
-- 2. Trino에서 CAST를 사용하여 JSON으로 변환
-- 3. dbt 모델에서 JSON 파싱 로직 수정