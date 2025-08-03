import os
import subprocess
import logging
import gzip
import json
from datetime import datetime
import ast
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.fs
from deltalake import DeltaTable, write_deltalake

import pandas as pd

def _convert_all_columns_to_string(df: pd.DataFrame) -> pd.DataFrame:
    """DataFrame의 모든 컬럼을 string 타입으로 변환"""
    for col in df.columns:
        df[col] = df[col].astype(str)
    return df

def _filter_by_organization(df: pd.DataFrame, target_logins: list) -> pd.DataFrame:
    """조직 필터링 함수"""
    if 'org' not in df.columns or len(df) == 0:
        return df
    
    def safe_dict_parse(x):
        if pd.isna(x) or x is None:
            return None
        if isinstance(x, str):
            try:
                return ast.literal_eval(x)
            except (ValueError, SyntaxError):
                return None
        return x
    
    df['org_parsed'] = df['org'].apply(safe_dict_parse)
    filtered_df = df[df['org_parsed'].apply(lambda x: x and isinstance(x, dict) and x.get('login') in target_logins)]
    
    if len(filtered_df) > 0:
        filtered_df = filtered_df.drop('org_parsed', axis=1)
    
    return filtered_df


# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MinIO 클라이언트 설정
def _get_minio_client():
    """MinIO 클라이언트 생성"""
    try:
        from minio import Minio
        from minio.error import S3Error

        return Minio(
            os.getenv("AWS_ENDPOINT_URL", "minio:9000"),  # MinIO 서버 주소
            access_key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),  # 액세스 키
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),  # 시크릿 키
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true"  # HTTPS 사용 여부
        )
    except ImportError:
        logger.error("❌ MinIO 클라이언트가 설치되지 않았습니다. 'pip install minio'를 실행하세요.")
        raise

def _ensure_bucket_exists(bucket_name: str):
    """버킷이 존재하는지 확인하고 없으면 생성"""
    try:
        from minio.error import S3Error
        client = _get_minio_client()
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"📦 버킷 생성 완료: {bucket_name}")
    except Exception as e:
        logger.error(f"❌ 버킷 생성 실패: {e}")
        raise

def _gharchive_url_for_hour(date_str: str, hour: int) -> str:
    """
    주어진 날짜(date_str)와 시간(hour)에 해당하는 GH Archive URL을 반환.
    ex) 2025-07-12, 0시 -> "http://data.gharchive.org/2025-07-12-0.json.gz"
    """
    return f"http://data.gharchive.org/{date_str}-{hour}.json.gz"

def _generate_urls_for_date(date_str: str) -> list[str]:
    """날짜 date_str에 대한 0시부터 23시까지 24개의 URL 목록을 생성."""
    return [_gharchive_url_for_hour(date_str, h) for h in range(24)]

def _download_and_upload_to_minio(url: str, date: str, organization: str, hour: int):
    """wget으로 데이터를 다운로드하고 바로 MinIO에 업로드"""
    try:
        bucket_name = "gh-archive-raw"
        _ensure_bucket_exists(bucket_name)
        
        # tmp 디렉토리 생성
        os.makedirs("./tmp", exist_ok=True)
        
        # 임시 파일명 생성
        temp_filename = f"./tmp/{date}-{hour}.json.gz"
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
        client = _get_minio_client()
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
    urls = _generate_urls_for_date(date)
    success_count = 0
    
    for hour, url in enumerate(urls):
        if _download_and_upload_to_minio(url, date, organization, hour):
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
    client = _get_minio_client()
    
    # Delta Lake 버킷 생성
    _ensure_bucket_exists(delta_bucket_name)
    
    # Delta Lake 경로 설정 (MinIO S3 호환)
    minio_endpoint = os.getenv("AWS_ENDPOINT_URL", "minio:9000")
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
    
    # tmp 디렉토리 생성
    os.makedirs("./tmp", exist_ok=True)
    
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
                        # 기본 필터링: org가 있는 데이터만 처리
                        if not data.get("org"):
                            continue
                        hour_data.append(data)
            
            # 임시 파일 삭제
            os.remove(temp_file)
            
            if hour_data:
                # 현재 시간대 데이터를 DataFrame으로 변환
                df_hour = pd.DataFrame(hour_data)
                
                # 모든 컬럼을 string 타입으로 변환
                df_hour = _convert_all_columns_to_string(df_hour)
                
                # 첫 번째 시간대는 새로 생성, 이후는 append 모드로 추가
                mode = "overwrite" if hour == 0 else "append"
                print(df_hour.columns)
                # Delta Lake에 저장
                write_deltalake(delta_path, df_hour, mode=mode, storage_options=storage_options)
                
                # Delta Lake 저장 성공 후 MinIO raw 버킷에서 원본 파일 삭제
                try:
                    client.remove_object(bucket_name, object_name)
                    logger.info(f"🗑️ 원본 파일 삭제 완료: {bucket_name}/{object_name}")
                except Exception as e:
                    logger.warning(f"⚠️ 원본 파일 삭제 실패: {e}")
                
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

def split_filtered_data_by_organization(date: str, organization: str, target_logins: list):
    """필터링된 데이터를 조직별로 분리해서 저장"""
    try:
        # MinIO 설정
        minio_endpoint = os.getenv("AWS_ENDPOINT_URL", "minio:9000")
        storage_options = {
            "AWS_ENDPOINT_URL": f"http://{minio_endpoint}",
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
        }
        
        # 필터링된 데이터 경로
        source_path = f"s3://gh-archive-delta/filtered_{organization}/{date}"
        
        logger.info(f"🔄 조직별 분리 시작: {source_path}")
        
        # 필터링된 데이터 읽기
        dt = DeltaTable(source_path, storage_options=storage_options)
        df = dt.to_pandas()
        
        logger.info(f"📊 전체 데이터: {len(df)} 행")
        
        # org 컬럼 파싱
        def safe_dict_parse(x):
            if pd.isna(x) or x is None:
                return None
            if isinstance(x, str):
                try:
                    return ast.literal_eval(x)
                except (ValueError, SyntaxError):
                    return None
            return x
        
        df['org_parsed'] = df['org'].apply(safe_dict_parse)
        
        # 각 target_login별로 분리 저장
        success_count = 0
        for login in target_logins:
            org_df = df[df['org_parsed'].apply(lambda x: x and isinstance(x, dict) and x.get('login') == login)]
            
            if len(org_df) > 0:
                # org_parsed 컬럼 제거 및 string 변환
                org_df_clean = org_df.drop('org_parsed', axis=1)
                org_df_clean = _convert_all_columns_to_string(org_df_clean)
                
                # 조직별 저장 경로
                output_path = f"s3://gh-archive-delta/org_{login}/{date}"
                
                # Delta Lake에 저장
                write_deltalake(output_path, org_df_clean, mode="overwrite", storage_options=storage_options)
                logger.info(f"✅ {login}: {len(org_df_clean)}행 저장 완료")
                success_count += 1
            else:
                logger.warning(f"⚠️ {login}: 데이터 없음")
        
        logger.info(f"✅ 조직별 분리 완료: {success_count}/{len(target_logins)}개 조직")
        return success_count > 0
        
    except Exception as e:
        logger.error(f"❌ 조직별 분리 실패: {e}")
        return False

def filter_delta_table_by_organization(date: str, organization: str, target_logins: list):
    """Delta Lake 테이블에서 조직별로 데이터 필터링"""
    try:
        # MinIO 설정
        minio_endpoint = os.getenv("AWS_ENDPOINT_URL", "minio:9000")
        storage_options = {
            "AWS_ENDPOINT_URL": f"http://{minio_endpoint}",
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
        }
        
        # 원본 및 필터링된 테이블 경로
        source_path = f"s3://gh-archive-delta/{organization}/{date}"
        filtered_path = f"s3://gh-archive-delta/filtered_{organization}/{date}"
        
        logger.info(f"🔍 필터링 시작: {source_path} -> {filtered_path}")
        
        # Delta Lake 테이블 읽기
        dt = DeltaTable(source_path, storage_options=storage_options)
        df = dt.to_pandas()
        
        logger.info(f"📊 원본 데이터: {len(df)} 행")
        
        # 조직별 필터링
        filtered_df = _filter_by_organization(df, target_logins)
        
        if len(filtered_df) > 0:
            # 모든 컬럼을 string 타입으로 변환
            filtered_df = _convert_all_columns_to_string(filtered_df)
            
            # 필터링된 데이터 저장
            write_deltalake(filtered_path, filtered_df, mode="overwrite", storage_options=storage_options)
            logger.info(f"✅ 필터링 완료: {len(filtered_df)} 행 저장됨")
            return True
        else:
            logger.warning("⚠️ 필터링된 결과가 없습니다.")
            return False
            
    except Exception as e:
        logger.error(f"❌ 필터링 실패: {e}")
        return False

def main():
    """메인 실행 함수"""
    import sys
    
    if len(sys.argv) < 4:
        print("사용법: python gh_archive_daily_collector.py <YYYY-MM-DD> <organization> <type> [target_logins]")
        print("  type: 'download', 'process', 'filter', 'split'")
        print("    - download: gz 파일 다운로드")
        print("    - process: 압축 해제 후 delta table에 저장")
        print("    - filter: 조직별 필터링")
        print("    - split: 필터링된 데이터를 조직별로 분리")
        print("  target_logins (filter/split용): 쉼표로 구분된 조직명 (예: CausalInferenceLab,Pseudo-Lab,apache)")
        sys.exit(1)
    
    date = sys.argv[1]
    organization = sys.argv[2]
    process_type = sys.argv[3]
    
    # 타입 검증
    if process_type not in ['download', 'process', 'filter', 'split']:
        logger.error(f"❌ 잘못된 타입: {process_type}. 'download', 'process', 'filter' 또는 'split'을 사용하세요.")
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
    elif process_type == 'filter':
        if len(sys.argv) < 5:
            logger.error("❌ filter 타입에는 target_logins 파라미터가 필요합니다.")
            sys.exit(1)
        
        target_logins = sys.argv[4].split(',')
        logger.info(f"🔍 {date} 데이터 필터링 시작 - 대상 조직: {target_logins}")
        success = filter_delta_table_by_organization(date, organization, target_logins)
        if success:
            logger.info(f"✅ {date} 데이터 필터링 완료")
        else:
            logger.error(f"❌ {date} 데이터 필터링 실패")
            sys.exit(1)
    elif process_type == 'split':
        if len(sys.argv) < 5:
            logger.error("❌ split 타입에는 target_logins 파라미터가 필요합니다.")
            sys.exit(1)
        
        target_logins = sys.argv[4].split(',')
        logger.info(f"🔄 {date} 데이터 조직별 분리 시작 - 대상 조직: {target_logins}")
        success = split_filtered_data_by_organization(date, organization, target_logins)
        if success:
            logger.info(f"✅ {date} 데이터 조직별 분리 완료")
        else:
            logger.error(f"❌ {date} 데이터 조직별 분리 실패")
            sys.exit(1)

if __name__ == "__main__":
    main()
