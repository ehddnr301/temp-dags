import pandas as pd
from trino import dbapi
import argparse
import json
import os
from datetime import datetime, timedelta, date
from langchain_chains import (
    generate_actor_week_report,
    generate_repo_week_report,
    generate_actor_week_reports_batch,
    generate_repo_week_reports_batch,
)
from dotenv import load_dotenv
from write_delta import write_delta

load_dotenv()

def get_week_start_str(arg_week_start: str | None) -> str:
    """입력(YYYY-MM-DD)이 있으면 그 주의 월요일, 없으면 오늘이 속한 주의 월요일을 반환."""
    if arg_week_start:
        dt = datetime.strptime(arg_week_start, "%Y-%m-%d").date()
    else:
        today = date.today()
        dt = today
    monday = dt - timedelta(days=dt.weekday())
    return monday.strftime("%Y-%m-%d")


parser = argparse.ArgumentParser(description="Fetch weekly GitHub activity rows from Trino and optionally generate LLM reports")
parser.add_argument(
    "--week-start",
    dest="week_start",
    help="주 시작일(월요일), 형식 YYYY-MM-DD. 미지정 시 현재 주의 월요일.",
)
parser.add_argument(
    "--report",
    choices=["actor-week", "repo-week"],
    help="LLM 리포트 생성 종류(생략 시 DataFrame만 출력)",
)
parser.add_argument("--actor-login", dest="actor_login", help="actor-week용 액터 로그인 (미지정 시 전체)" )
parser.add_argument("--organization", dest="organization", help="repo-week용 조직명 (미지정 시 전체)")
parser.add_argument("--repo-name", dest="repo_name", help="repo-week용 레포 이름 (미지정 시 전체)")
parser.add_argument("--output", dest="output", help="리포트 JSON 저장 경로(미지정 시 stdout)")
# Delta 저장 옵션
parser.add_argument("--save-delta", action="store_true", help="생성된 리포트를 MinIO Delta에 저장")
parser.add_argument("--delta-s3-uri", dest="delta_s3_uri", help="s3://bucket/path 직접 지정")
parser.add_argument("--delta-bucket", dest="delta_bucket", default="gh-archive-delta", help="버킷명")
parser.add_argument("--delta-path", dest="delta_path", help="버킷 내 경로(미지정 시 dl_actor_week/dl_repo_week)")
parser.add_argument(
    "--delta-mode",
    dest="delta_mode",
    choices=["auto", "overwrite", "append"],
    default="auto",
    help="쓰기 모드: auto(기존 테이블 존재 시 append+schema merge, 없으면 overwrite)",
)
args = parser.parse_args()
week_start = get_week_start_str(args.week_start)
week_end = (datetime.strptime(week_start, "%Y-%m-%d").date() + timedelta(days=6)).strftime("%Y-%m-%d")

# Trino 연결 설정
conn = dbapi.connect(
    host=os.getenv("TRINO_SERVICE_HOST"),
    port=os.getenv("TRINO_SERVICE_PORT"),
    user=os.getenv("TRINO_USER", "admin"),
    catalog=os.getenv("TRINO_CATALOG", "delta"),
    schema=os.getenv("TRINO_SCHEMA", "default"),
    http_scheme="http"
)

# Cursor 생성
cur = conn.cursor()

# 쿼리 실행 (주간 범위: [week_start, week_start + 7일))
cur.execute(
    f"""
select cast('{week_start}' as date) as week_start
, base_date
, organization
, repo_name
, actor_login
, source_event_type
, activity_text
from dw_activity_feed
where base_date >= date '{week_start}'
  and base_date < (date '{week_start}' + interval '7' day)
  and organization != 'apache'
order by base_date
"""
)

# 결과 가져오기
rows = cur.fetchall()
df = pd.DataFrame(
    rows,
    columns=[
        "week_start",
        "base_date",
        "organization",
        "repo_name",
        "actor_login",
        "source_event_type",
        "activity_text",
    ],
)

df["week_end"] = week_end

if not args.report:
    print(df)
else:
    if args.report == "actor-week":
        if args.actor_login:
            result = generate_actor_week_report(
                df=df,
                week_start=week_start,
                week_end=week_end,
                actor_login=args.actor_login,
            )
        else:
            result = generate_actor_week_reports_batch(
                df=df,
                week_start=week_start,
                week_end=week_end,
            )
    else:  # repo-week
        if args.organization and args.repo_name:
            result = generate_repo_week_report(
                df=df,
                week_start=week_start,
                week_end=week_end,
                organization=args.organization,
                repo_name=args.repo_name,
            )
        else:
            result = generate_repo_week_reports_batch(
                df=df,
                week_start=week_start,
                week_end=week_end,
            )

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"saved: {args.output}")
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2))

    # Delta 저장 (리포트가 생성된 경우에만)
    if args.save_delta:
        rows = result if isinstance(result, list) else [result]
        # 기본 경로 결정
        if args.delta_s3_uri:
            dst = args.delta_s3_uri
        else:
            base = args.delta_path
            if not base:
                base = "dl_actor_week" if args.report == "actor-week" else "dl_repo_week"
                if not base.startswith("dl_"):
                    base = f"dl_{base}"
            dst = f"s3://{args.delta_bucket}/{base}"

        # 통합 유틸 사용: 버킷 보장, 모드 자동결정, 스키마 병합 등 처리
        write_delta(rows, dst, mode=args.delta_mode)
