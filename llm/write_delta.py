import os
import json
import argparse
from typing import List, Dict, Any, Tuple

import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
from dotenv import load_dotenv

load_dotenv()


def _get_storage_options() -> dict:
    storage_options = {
        "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
        "AWS_ALLOW_HTTP": "true"
    }
    return storage_options


def _parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    """s3://bucket/path -> (bucket, path)"""
    if not s3_uri.startswith("s3://"):
        raise ValueError("s3_uri must start with s3://")
    without_scheme = s3_uri[5:]
    parts = without_scheme.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


def _ensure_bucket_exists(bucket: str) -> None:
    """Ensure the bucket exists in MinIO/S3-compatible endpoint. Best-effort."""
    try:
        from minio import Minio

        endpoint = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000").replace(
            "http://", ""
        ).replace("https://", "")
        client = Minio(
            endpoint,
            access_key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
        )
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
    except Exception:
        # Best-effort only; ignore if we cannot ensure the bucket
        pass


def _delta_table_exists(dst: str, storage_options: Dict[str, Any]) -> bool:
    try:
        DeltaTable(dst, storage_options=storage_options)
        return True
    except Exception:
        return False


def write_delta(rows: List[Dict[str, Any]], dst: str, mode: str = "auto") -> None:
    """
    Write rows to a Delta Lake path with behavior similar to other modules:
    - Ensure destination bucket exists (best-effort)
    - If table exists and mode==auto: append with schema merge
    - If table does not exist and mode==auto: overwrite (create)
    - If mode explicitly provided (append/overwrite), respect it
    """
    storage_options = _get_storage_options()

    # Ensure bucket exists (best-effort)
    try:
        bucket, _ = _parse_s3_uri(dst)
        _ensure_bucket_exists(bucket)
    except Exception:
        pass

    table_exists = _delta_table_exists(dst, storage_options)

    resolved_mode = mode
    if mode == "auto":
        resolved_mode = "append" if table_exists else "overwrite"

    table = pa.Table.from_pylist(rows)

    write_kwargs: Dict[str, Any] = dict(
        mode=resolved_mode,
        storage_options=storage_options,
    )
    # Align with other writer: on append, allow schema merge
    if resolved_mode == "append":
        write_kwargs["schema_mode"] = "merge"

    write_deltalake(dst, table, **write_kwargs)
    print(f"✅ wrote delta: {dst} (mode={resolved_mode})")


def _default_table_name(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "dl_results"
    s = rows[0]
    if "actor_login" in s and "organization_set" in s:
        return "dl_actor_week"
    if "organization" in s and "repo_name" in s:
        return "dl_repo_week"
    return "dl_results"


def main() -> None:
    parser = argparse.ArgumentParser(description="Write JSON (weekly reports) to MinIO Delta Lake")
    parser.add_argument("--json", default="temp.json", help="입력 JSON 파일 경로")
    parser.add_argument("--table", help="테이블명(기본: dl_actor_week/dl_repo_week 자동 판별)")
    parser.add_argument("--s3-uri", help="s3://bucket/path 직접 지정(미지정 시 버킷/경로 사용)")
    parser.add_argument("--bucket", default="gh-archive-delta", help="버킷명")
    parser.add_argument("--path", help="버킷 내 경로(미지정 시 테이블명과 동일)")
    parser.add_argument(
        "--mode",
        choices=["auto", "overwrite", "append"],
        default="auto",
        help="쓰기 모드: auto(기존 테이블 존재 시 append+schema merge, 없으면 overwrite)",
    )
    args = parser.parse_args()

    with open(args.json, "r", encoding="utf-8") as f:
        rows: List[Dict[str, Any]] = json.load(f)

    table_name = args.table or _default_table_name(rows)
    if not table_name.startswith("dl_"):
        table_name = f"dl_{table_name}"

    if args.s3_uri:
        dst = args.s3_uri
    else:
        base_path = args.path or table_name
        dst = f"s3://{args.bucket}/{base_path}"

    write_delta(rows, dst, mode=args.mode)
    print(f"✅ wrote delta: {dst} (table={table_name})")


if __name__ == "__main__":
    main()


