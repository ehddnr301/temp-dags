import os
import subprocess
import logging
import gzip
import json
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def find_nulltype_paths(df: pd.DataFrame) -> list[str]:
    tbl = pa.Table.from_pandas(df, preserve_index=False)
    bad = []

    def walk(prefix: str, typ: pa.DataType):
        if pa.types.is_null(typ):                # list<null>, struct<…null…> 등
            bad.append(prefix.rstrip("."))
        elif pa.types.is_struct(typ):
            for field in typ:                    # pa.Field
                walk(f"{prefix}{field.name}.", field.type)
        elif pa.types.is_list(typ) or pa.types.is_large_list(typ):
            walk(prefix, typ.value_type)         # list 값 타입 검사
        elif pa.types.is_map(typ):
            walk(prefix + "key.", typ.key_type)
            walk(prefix + "value.", typ.item_type)

    for field in tbl.schema:                     # ← Schema → Field
        walk(f"{field.name}.", field.type)

    return sorted(set(bad))

NULL_PATHS = [
    "payload.forkee.language",
    "payload.forkee.mirror_url",
    "payload.forkee.topics",
    "payload.pages.summary",
    "payload.pull_request.active_lock_reason",
    "payload.pull_request.base.repo.mirror_url",
    "payload.pull_request.head.repo.mirror_url",
    "payload.pull_request.milestone.closed_at",
    "payload.pull_request.requested_teams.parent",
]

def strip_null_paths(event: dict) -> dict:
    """
    ① NULL_PATHS 에 명시된 키 삭제  
    ② dict/list 가 '텅 빈' 상태로 남으면 상위에서도 삭제
    """
    # 1) 지정된 경로 키 제거
    for path in NULL_PATHS:
        cur = event
        keys = path.split(".")
        for k in keys[:-1]:
            cur = cur.get(k)
            if not isinstance(cur, dict):
                break
        else:
            cur.pop(keys[-1], None)

    # 2) 재귀적으로 빈 컨테이너(dict/list) 제거
    def prune(obj):
        if isinstance(obj, dict):
            for k in list(obj.keys()):
                if prune(obj[k]):        # 하위가 모두 비면 삭제
                    obj.pop(k, None)
            return len(obj) == 0
        if isinstance(obj, list):
            obj[:] = [v for v in obj if not prune(v)]
            return len(obj) == 0
        # 스칼라(null 이 아닌 값) → False
        return obj is None

    return event

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MinIO 클라이언트 설정
def get_minio_client():
    """MinIO 클라이언트 생성"""
    try:
        from minio import Minio
        from minio.error import S3Error

        print(os.getenv("AWS_ENDPOINT_URL", ""))
        print(os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
        print(os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"))
        print(os.getenv("MINIO_SECURE", "false").lower() == "true")
        
        return Minio(
            os.getenv("AWS_ENDPOINT_URL", "localhost:30090"),  # MinIO 서버 주소
            access_key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),  # 액세스 키
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),  # 시크릿 키
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true"  # HTTPS 사용 여부
        )
    except ImportError:
        logger.error("❌ MinIO 클라이언트가 설치되지 않았습니다. 'pip install minio'를 실행하세요.")
        raise

def ensure_bucket_exists(bucket_name: str):
    """버킷이 존재하는지 확인하고 없으면 생성"""
    try:
        from minio.error import S3Error
        client = get_minio_client()
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"📦 버킷 생성 완료: {bucket_name}")
    except Exception as e:
        logger.error(f"❌ 버킷 생성 실패: {e}")
        raise

def gharchive_url_for_hour(date_str: str, hour: int) -> str:
    """
    주어진 날짜(date_str)와 시간(hour)에 해당하는 GH Archive URL을 반환.
    ex) 2025-07-12, 0시 -> "http://data.gharchive.org/2025-07-12-0.json.gz"
    """
    return f"http://data.gharchive.org/{date_str}-{hour}.json.gz"

def generate_urls_for_date(date_str: str) -> list[str]:
    """날짜 date_str에 대한 0시부터 23시까지 24개의 URL 목록을 생성."""
    return [gharchive_url_for_hour(date_str, h) for h in range(24)]

def download_and_upload_to_minio(url: str, date: str, organization: str, hour: int):
    """wget으로 데이터를 다운로드하고 바로 MinIO에 업로드"""
    try:
        bucket_name = "gh-archive-raw"
        ensure_bucket_exists(bucket_name)
        
        # 임시 파일명 생성
        temp_filename = f"/tmp/{date}-{hour}.json.gz"
        object_name = f"{organization}/{date}/{date}-{hour}.json.gz"
        
        # wget으로 다운로드
        wget_cmd = [
            "wget", 
            "-O", temp_filename,
            "--timeout=30",
            "--tries=3",
            url
        ]
        
        logger.info(f"📥 다운로드 시작: {url}")
        result = subprocess.run(wget_cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"❌ wget 실패: {result.stderr}")
            return False
        
        # MinIO에 업로드
        client = get_minio_client()
        with open(temp_filename, 'rb') as file_data:
            client.put_object(
                bucket_name,
                object_name,
                file_data,
                length=os.path.getsize(temp_filename),
                content_type="application/gzip"
            )
        
        # 임시 파일 삭제
        os.remove(temp_filename)
        
        logger.info(f"✅ 업로드 완료: {bucket_name}/{object_name}")
        return True
        
    except Exception as e:
        logger.error(f"❌ 다운로드/업로드 실패: {e}")
        # 임시 파일 정리
        if os.path.exists(temp_filename):
            os.remove(temp_filename)
        return False

def download_all_hours(date: str, organization: str):
    """하루치 모든 시간대 데이터를 다운로드하고 MinIO에 업로드"""
    urls = generate_urls_for_date(date)
    success_count = 0
    
    for hour, url in enumerate(urls):
        if download_and_upload_to_minio(url, date, organization, hour):
            success_count += 1
    
    logger.info(f"📊 {date} - 총 {len(urls)}개 중 {success_count}개 성공")
    return success_count

def process_and_save_to_delta(date: str, organization: str):
    """MinIO에서 gz 파일을 읽어서 Delta Lake 테이블로 저장 (시간대별 개별 처리)"""
    try:
        import pandas as pd
        from deltalake import DeltaTable, write_deltalake
    except ImportError:
        logger.error("❌ pandas 또는 deltalake가 설치되지 않았습니다.")
        return False
    
    bucket_name = "gh-archive-raw"
    delta_bucket_name = "gh-archive-delta"
    client = get_minio_client()
    
    # Delta Lake 버킷 생성
    ensure_bucket_exists(delta_bucket_name)
    
    # Delta Lake 경로 설정 (MinIO S3 호환)
    minio_endpoint = os.getenv("AWS_ENDPOINT_URL", "localhost:30090")
    delta_path = f"s3://{delta_bucket_name}/{organization}/{date}"
    
    # S3 호환 설정
    storage_options = {
        "AWS_ENDPOINT_URL": f"http://{minio_endpoint}",
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "AWS_REGION": "us-east-1",  # MinIO는 기본적으로 us-east-1 사용
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }
    
    total_rows = 0
    success_count = 0
    
    # 24시간 데이터를 시간대별로 개별 처리
    for hour in range(24):
        object_name = f"{organization}/{date}/{date}-{hour}.json.gz"
        temp_file = f"./tmp/{date}-{hour}.json.gz"
        
        try:
            # MinIO에서 gz 파일 다운로드
            client.fget_object(bucket_name, object_name, temp_file)
            
            logger.info(f"✅ {hour}시 데이터 다운로드 완료")
            
            # gz 파일 압축 해제 및 JSON 파싱 (시간대별로 개별 처리)
            hour_data = []
            with gzip.open(temp_file, 'rt', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        data = json.loads(line)
                        if not data.get("org"):
                            continue
                        data = strip_null_paths(data)
                        hour_data.append(data)
            
            # 임시 파일 삭제
            os.remove(temp_file)
            
            if hour_data:
                # 현재 시간대 데이터를 DataFrame으로 변환
                df_hour = pd.DataFrame(hour_data)

                print(df_hour)
                print(df_hour.columns)
                print(df_hour.columns[df_hour.isna().any()])

                bad_fields = find_nulltype_paths(df_hour)
                print(bad_fields)
                
                # 첫 번째 시간대는 새로 생성, 이후는 append 모드로 추가
                mode = "overwrite" if hour == 0 else "append"
                
                # Delta Lake에 저장
                write_deltalake(delta_path, df_hour, mode=mode, storage_options=storage_options)
                
                total_rows += len(df_hour)
                success_count += 1
                logger.info(f"✅ {hour}시 데이터 처리 완료 - {len(df_hour)} 행 저장됨")
            else:
                logger.warning(f"⚠️ {hour}시 데이터가 비어있습니다.")
        except Exception as e:
            logger.error(f"❌ {hour}시 데이터 처리 실패: {e}")
            if os.path.exists(temp_file):
                os.remove(temp_file)
    
    if success_count == 0:
        logger.error("❌ 처리할 데이터가 없습니다.")
        return False
    
    logger.info(f"✅ Delta Lake 테이블 저장 완료: {delta_path} (총 {total_rows} 행, {success_count}개 시간대)")
    return True

def main():
    """메인 실행 함수"""
    import sys
    
    if len(sys.argv) != 4:
        print("사용법: python gh_archive_daily_collector.py <YYYY-MM-DD> <organization> <type>")
        print("  type: 'download' (gz 파일 다운로드) 또는 'process' (압축 해제 후 delta table에 저장)")
        sys.exit(1)
    
    date = sys.argv[1]
    organization = sys.argv[2]
    process_type = sys.argv[3]
    
    # 타입 검증
    if process_type not in ['download', 'process']:
        logger.error(f"❌ 잘못된 타입: {process_type}. 'download' 또는 'process'를 사용하세요.")
        sys.exit(1)
    
    try:
        datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        logger.error(f"❌ 잘못된 날짜 형식: {date}")
        sys.exit(1)
    
    logger.info(f"🚀 {date} 데이터 처리 시작 - 조직: {organization}, 타입: {process_type}")
    
    if process_type == 'download':
        success_count = download_all_hours(date, organization)
        logger.info(f"✅ {date} 데이터 다운로드 완료 - {success_count}개 파일 업로드됨")
    elif process_type == 'process':
        logger.info(f"🔄 {date} 데이터 처리 시작 - 압축 해제 및 delta table 저장")
        success = process_and_save_to_delta(date, organization)
        if success:
            logger.info(f"✅ {date} 데이터 처리 완료")
        else:
            logger.error(f"❌ {date} 데이터 처리 실패")
            sys.exit(1)

if __name__ == "__main__":
    main()
