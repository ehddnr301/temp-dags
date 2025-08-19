import os
import time
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests
import logging
import sys as _sys
import pandas as pd

# Utils from this repo
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from gh_archive_utils import get_storage_options, get_delta_table_path, to_snake_case  # type: ignore
from timezone_utils import add_timezone_columns  # type: ignore

# Delta Lake
from deltalake import DeltaTable, write_deltalake

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
log_level_value = getattr(logging, LOG_LEVEL, logging.INFO)
logging.basicConfig(
    level=log_level_value,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    stream=_sys.stdout,
    force=True,
)
logger = logging.getLogger(__name__)
logger.debug("LOG_LEVEL=%s", LOG_LEVEL)


# -----------------------------
# Configuration (via env)
# -----------------------------
DELTA_BUCKET = os.getenv("DELTA_BUCKET", "gh-archive-delta")
DELTA_TABLE_NAME = os.getenv("DELTA_TABLE_NAME", "gh_user_relations")
DELTA_PATH = get_delta_table_path(DELTA_BUCKET, DELTA_TABLE_NAME)
logger.debug(
    "Configured Delta Lake: bucket=%s, table=%s, path=%s",
    DELTA_BUCKET,
    DELTA_TABLE_NAME,
    DELTA_PATH,
)

# Partition columns for Delta Lake
DELTA_PARTITIONS = ["dt_kst", "username"]

# GitHub API
GITHUB_API_BASE = "https://api.github.com"
DEFAULT_PER_PAGE = 100

# Trino Configuration (via env)
TRINO_HOST = os.getenv("TRINO_SERVICE_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_SERVICE_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "admin")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "delta")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "default")
TRINO_USERS_SQL = os.getenv(
    "TRINO_USERS_SQL",
    (
        """
        SELECT DISTINCT user_login
        FROM dm_unique_users_by_org_repo
        WHERE organization != 'apache'
        """
    ).strip(),
)
logger.debug(
    "Trino config: host=%s, port=%s, user=%s, catalog=%s, schema=%s",
    TRINO_HOST,
    TRINO_PORT,
    TRINO_USER,
    TRINO_CATALOG,
    TRINO_SCHEMA,
)


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _headers(token: Optional[str]) -> Dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "gh-followers-collector",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _fetch_all_pages(url: str, token: Optional[str]) -> List[Dict]:
    results: List[Dict] = []
    session = requests.Session()
    page = 1
    while True:
        time.sleep(1)
        resp = session.get(
            url,
            headers=_headers(token),
            params={"per_page": DEFAULT_PER_PAGE, "page": page},
            timeout=30,
        )
        if resp.status_code == 403 and "rate limit" in resp.text.lower():
            logging.warning("GitHub API rate limit hit for %s page=%s; sleeping 60s", url, page)
            time.sleep(60)
            continue
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            break
        if len(data) == 0:
            break
        logging.debug("Fetched %s items from %s page=%s", len(data), url, page)
        results.extend(data)
        if len(data) < DEFAULT_PER_PAGE:
            break
        page += 1
    return results


def get_usernames_from_trino() -> List[str]:
    try:
        from trino import dbapi  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "Python 'trino' 패키지가 필요합니다. `pip install trino` 후 다시 실행하세요."
        ) from exc

    logger.debug(
        "Connecting to Trino at %s:%s (catalog=%s, schema=%s) as user=%s",
        TRINO_HOST,
        TRINO_PORT,
        TRINO_CATALOG,
        TRINO_SCHEMA,
        TRINO_USER,
    )
    conn = dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
        http_scheme="http",
    )

    try:
        with conn.cursor() as cur:
            logger.debug("Executing TRINO_USERS_SQL to fetch usernames")
            cur.execute(TRINO_USERS_SQL)
            rows = cur.fetchall()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    usernames = [str(r[0]).strip() for r in rows if r and r[0]]
    logger.debug("Fetched %s usernames from Trino", len(usernames))
    seen = set()
    ordered_unique: List[str] = []
    for u in usernames:
        if u not in seen:
            seen.add(u)
            ordered_unique.append(u)
    return ordered_unique


def collect_user_relations(usernames: List[str], token: Optional[str]) -> pd.DataFrame:
    collected_rows: List[Dict] = []
    collected_at = _now_utc_iso()

    for username in usernames:
        time.sleep(1)
        username = username.strip()
        if not username:
            continue

        followers_url = f"{GITHUB_API_BASE}/users/{username}/followers"
        following_url = f"{GITHUB_API_BASE}/users/{username}/following"

        try:
            followers = _fetch_all_pages(followers_url, token)
        except Exception as exc:
            logger.warning("Failed to fetch followers for %s: %s", username, exc)
            followers = []
        try:
            following = _fetch_all_pages(following_url, token)
        except Exception as exc:
            logger.warning("Failed to fetch following for %s: %s", username, exc)
            following = []

        logger.info(
            "Collected followers=%s, following=%s for %s",
            len(followers),
            len(following),
            username,
        )

        for row in followers:
            collected_rows.append({
                "username": username,
                "relation_type": "follower",
                "related_login": row.get("login"),
                "related_id": row.get("id"),
                "related_type": row.get("type"),
                "related_html_url": row.get("html_url"),
                "raw": json.dumps(row, ensure_ascii=False),
                "collected_at": collected_at,
            })

        for row in following:
            collected_rows.append({
                "username": username,
                "relation_type": "following",
                "related_login": row.get("login"),
                "related_id": row.get("id"),
                "related_type": row.get("type"),
                "related_html_url": row.get("html_url"),
                "raw": json.dumps(row, ensure_ascii=False),
                "collected_at": collected_at,
            })

    if not collected_rows:
        return pd.DataFrame()

    df = pd.DataFrame(collected_rows)
    logger.debug("Built dataframe with shape=%s", df.shape)

    # Add timezone columns using shared utility (adds ts_utc, dt_utc, ts_kst(+9h), dt_kst)
    try:
        df = add_timezone_columns(df, created_at_col='collected_at')
        logger.debug("Timezone columns added via add_timezone_columns")
    except Exception as exc:
        logger.exception("Failed to add timezone columns: %s", exc)
        raise

    df["username"] = df["username"].apply(lambda x: to_snake_case(str(x)))

    return df


def write_to_delta(df: pd.DataFrame) -> None:
    if df is None or len(df) == 0:
        return

    storage_options = get_storage_options()

    try:
        from minio import Minio
        endpoint = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000").replace("http://", "").replace("https://", "")
        client = Minio(
            endpoint,
            access_key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
        )
        if not client.bucket_exists(DELTA_BUCKET):
            client.make_bucket(DELTA_BUCKET)
    except Exception:
        pass

    table_exists = False
    try:
        DeltaTable(DELTA_PATH, storage_options=storage_options)
        table_exists = True
    except Exception:
        table_exists = False

    mode = "append" if table_exists else "overwrite"
    logger.debug(
        "Writing %s rows to Delta Lake (mode=%s, path=%s, partitions=%s)",
        len(df),
        mode,
        DELTA_PATH,
        DELTA_PARTITIONS,
    )

    write_kwargs = dict(
        mode=mode,
        storage_options=storage_options,
        partition_by=DELTA_PARTITIONS,
    )
    if mode == "append":
        write_kwargs["schema_mode"] = "merge"

    write_deltalake(DELTA_PATH, df, **write_kwargs)
    logger.debug("Delta write complete")


def run_collection() -> None:
    # Trino에서 사용자 목록을 조회하여 수집 대상을 결정
    usernames: List[str] = get_usernames_from_trino()
    total_users = len(usernames)
    logger.info("총 대상 사용자 수: %s", total_users)
    if not usernames:
        raise ValueError(
            "수집 대상 유저가 없습니다. Trino 쿼리 결과가 비어있습니다. "
            "'TRINO_HOST', 'TRINO_PORT', 'TRINO_USER', 'TRINO_CATALOG', 'TRINO_SCHEMA', 'TRINO_USERS_SQL' 환경 설정을 확인하세요."
        )
    token = os.getenv("GITHUB_TOKEN")
    logger.info("GITHUB_TOKEN=%s", token)

    success_count = 0
    for index, username in enumerate(usernames, start=1):
        u = str(username).strip()
        if not u:
            continue
        logger.info("[%s/%s] 사용자 적재 시작: %s", index, total_users, u)
        df = collect_user_relations([u], token)
        if df is None or len(df) == 0:
            logger.debug("[%s/%s] 데이터 없음: %s", index, total_users, u)
            continue
        write_to_delta(df)
        success_count += 1
        logger.info("[%s/%s] 사용자 적재 완료: %s, rows=%s", index, total_users, u, len(df))

    logger.info("총 적재 사용자 수: %s/%s", success_count, total_users)


