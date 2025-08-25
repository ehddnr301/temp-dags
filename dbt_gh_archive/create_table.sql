-- dl_watch_events
CREATE TABLE IF NOT EXISTS delta.default.dl_watch_events (
       event_id      varchar,
       repo_name     varchar,
       organization  varchar,
       user_login    varchar,
       ts_kst        timestamp(3) with time zone,
       base_date     date
     )
     WITH (
       location = 's3://gh-archive-delta/dl/dl_watch_events',
       partitioned_by = ARRAY['base_date']
     );

-- dl_push_events
CREATE TABLE IF NOT EXISTS delta.default.dl_push_events (
       event_id              varchar,
       repo_name             varchar,
       organization          varchar,
       user_login            varchar,
       repository_id         bigint,
       push_id               bigint,
       size                  integer,
       distinct_size         integer,
       ref_full              varchar,
       ref_name              varchar,
       is_branch_ref         boolean,
       is_tag_ref            boolean,
       head_sha              varchar,
       before_sha            varchar,
       commit_sha            varchar,
       commit_author_name    varchar,
       commit_author_email   varchar,
       commit_message        varchar,
       commit_distinct       boolean,
       commit_url            varchar,
       ts_kst                timestamp(3) with time zone,
       base_date             date
     )
     WITH (
       location = 's3://gh-archive-delta/dl/dl_push_events',
       partitioned_by = ARRAY['base_date']
     );

-- dl_issues_events
CREATE TABLE IF NOT EXISTS delta.default.dl_issues_events (
       event_id            varchar,
       repo_name           varchar,
       organization        varchar,
       user_login          varchar,
       issue_action        varchar,
       issue_number        integer,
       issue_title         varchar,
       issue_body          varchar,
       issue_state         varchar,
       issue_html_url      varchar,
       issue_author_login  varchar,
       issue_author_id     bigint,
       ts_kst              timestamp(3) with time zone,
       base_date           date
     )
     WITH (
       location = 's3://gh-archive-delta/dl/dl_issues_events',
       partitioned_by = ARRAY['base_date']
     );

-- dl_issue_comment_events
CREATE TABLE IF NOT EXISTS delta.default.dl_issue_comment_events (
       event_id             varchar,
       repo_name            varchar,
       organization         varchar,
       user_login           varchar,
       ic_action            varchar,
       issue_number         integer,
       issue_title          varchar,
       issue_state          varchar,
       issue_html_url       varchar,
       issue_author_login   varchar,
       issue_author_id      bigint,
       comment_id           bigint,
       comment_html_url     varchar,
       comment_body         varchar,
       commenter_login      varchar,
       commenter_id         bigint,
       ts_kst               timestamp(3) with time zone,
       base_date            date
     )
     WITH (
       location = 's3://gh-archive-delta/dl/dl_issue_comment_events',
       partitioned_by = ARRAY['base_date']
     );

-- dl_pull_request_events
CREATE TABLE IF NOT EXISTS delta.default.dl_pull_request_events (
       event_id                varchar,
       repo_name               varchar,
       organization            varchar,
       user_login              varchar,
       pr_action               varchar,
       pr_number               integer,
       pr_title                varchar,
       pr_html_url             varchar,
       pr_state                varchar,
       is_draft                boolean,
       pr_body                 varchar,
       pr_author_login         varchar,
       pr_author_id            bigint,
       head_ref                varchar,
       base_ref                varchar,
       head_sha                varchar,
       base_sha                varchar,
       commits_count           integer,
       additions               integer,
       deletions               integer,
       changed_files           integer,
       issue_comments_count    integer,
       review_comments_count   integer,
       is_merged               boolean,
       merge_commit_sha        varchar,
       ts_kst                  timestamp(3) with time zone,
       base_date               date
     )
     WITH (
       location = 's3://gh-archive-delta/dl/dl_pull_request_events',
       partitioned_by = ARRAY['base_date']
     );

-- dl_pull_request_review_events
CREATE TABLE IF NOT EXISTS delta.default.dl_pull_request_review_events (
       event_id                varchar,
       repo_name               varchar,
       organization            varchar,
       user_login              varchar,
       reviewer_login          varchar,
       reviewer_id             bigint,
       review_state            varchar,
       review_body             varchar,
       review_html_url         varchar,
       commit_sha              varchar,
       review_submitted_at_utc varchar,
       review_submitted_at_kst varchar,
       pr_number               integer,
       pr_title                varchar,
       pr_html_url             varchar,
       pr_author_login         varchar,
       ts_kst                  timestamp(3) with time zone,
       base_date               date
     )
     WITH (
       location = 's3://gh-archive-delta/dl/dl_pull_request_review_events',
       partitioned_by = ARRAY['base_date']
     );

-- dl_pull_request_review_comment_events
CREATE TABLE IF NOT EXISTS delta.default.dl_pull_request_review_comment_events (
       event_id                   varchar,
       repo_name                  varchar,
       organization               varchar,
       user_login                 varchar,
       pr_review_comment_action   varchar,
       review_comment_id          bigint,
       review_comment_html_url    varchar,
       review_comment_body        varchar,
       diff_hunk                  varchar,
       file_path                  varchar,
       commit_id                  varchar,
       review_id                  bigint,
       commenter_login            varchar,
       commenter_id               bigint,
       pr_number                  integer,
       pr_title                   varchar,
       pr_html_url                varchar,
       pr_state                   varchar,
       ts_kst                     timestamp(3) with time zone,
       base_date                  date
     )
     WITH (
       location = 's3://gh-archive-delta/dl/dl_pull_request_review_comment_events',
       partitioned_by = ARRAY['base_date']
     );

-- dl_fork_events
CREATE TABLE IF NOT EXISTS delta.default.dl_fork_events (
       event_id            varchar,
       repo_name           varchar,
       organization        varchar,
       user_login          varchar,
       forkee_id           bigint,
       forkee_full_name    varchar,
       forkee_html_url     varchar,
       forkee_owner_login  varchar,
       forkee_owner_id     bigint,
       forkee_description  varchar,
       is_fork_private     boolean,
       forkee_default_branch varchar,
       ts_kst              timestamp(3) with time zone,
       base_date           date
     )
     WITH (
       location = 's3://gh-archive-delta/dl/dl_fork_events',
       partitioned_by = ARRAY['base_date']
     );

-- dl_member_events
CREATE TABLE IF NOT EXISTS delta.default.dl_member_events (
       event_id          varchar,
       repo_name         varchar,
       organization      varchar,
       user_login        varchar,
       member_login      varchar,
       member_id         bigint,
       member_html_url   varchar,
       member_action     varchar,
       is_member_added   boolean,
       is_member_removed boolean,
       ts_kst            timestamp(3) with time zone,
       base_date         date
     )
     WITH (
       location = 's3://gh-archive-delta/dl/dl_member_events',
       partitioned_by = ARRAY['base_date']
     );

-- dl_delete_events
CREATE TABLE IF NOT EXISTS delta.default.dl_delete_events (
       event_id          varchar,
       repo_name         varchar,
       organization      varchar,
       user_login        varchar,
       deleted_ref_type  varchar,
       deleted_ref_name  varchar,
       pusher_type       varchar,
       is_branch_deletion boolean,
       is_tag_deletion    boolean,
       ts_kst            timestamp(3) with time zone,
       base_date         date
     )
     WITH (
       location = 's3://gh-archive-delta/dl/dl_delete_events',
       partitioned_by = ARRAY['base_date']
     );

-- dl_create_events
CREATE TABLE IF NOT EXISTS delta.default.dl_create_events (
       event_id            varchar,
       repo_name           varchar,
       organization        varchar,
       user_login          varchar,
       created_ref_type    varchar,
       created_ref_name    varchar,
       default_branch      varchar,
       repo_description    varchar,
       pusher_type         varchar,
       is_repo_creation    boolean,
       is_branch_creation  boolean,
       is_tag_creation     boolean,
       ts_kst              timestamp(3) with time zone,
       base_date           date
     )
     WITH (
       location = 's3://gh-archive-delta/dl/dl_create_events',
       partitioned_by = ARRAY['base_date']
     );

-- dl_gollum_events
CREATE TABLE IF NOT EXISTS delta.default.dl_gollum_events (
       event_id        varchar,
       repo_name       varchar,
       organization    varchar,
       user_login      varchar,
       page_name       varchar,
       page_title      varchar,
       page_summary    varchar,
       page_action     varchar,
       page_sha        varchar,
       page_html_url   varchar,
       is_page_created boolean,
       is_page_edited  boolean,
       is_page_deleted boolean,
       ts_kst          timestamp(3) with time zone,
       base_date       date
     )
     WITH (
       location = 's3://gh-archive-delta/dl/dl_gollum_events',
       partitioned_by = ARRAY['base_date']
     );

-- dw_activity_feed
CREATE TABLE IF NOT EXISTS delta.default.dw_activity_feed (
       activity_id       varchar,
       source_event_type varchar,
       organization      varchar,
       repo_name         varchar,
       actor_login       varchar,
       activity_text     varchar,
       ts_kst            timestamp(3) with time zone,
       base_date         date
     )
     WITH (
       location = 's3://gh-archive-delta/dw/dw_activity_feed',
       partitioned_by = ARRAY['base_date']
     );