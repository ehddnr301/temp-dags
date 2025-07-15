import os
import pandas as pd
import requests
import json
import gzip
import io
from datetime import datetime, timedelta
from deltalake import write_deltalake
import time
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MinIO 환경 변수
os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
os.environ["AWS_ENDPOINT_URL"] = "http://minio:9000"
os.environ["AWS_ALLOW_HTTP"] = "true"
os.environ["AWS_CONDITIONAL_PUT"] = "etag"

def gharchive_url_for_hour(date_str: str, hour: int) -> str:
    """
    주어진 날짜(date_str)와 시간(hour)에 해당하는 GH Archive URL을 반환.
    ex) 2025-07-12, 0시 -> "http://data.gharchive.org/2025-07-12-0.json.gz"
    """
    return f"http://data.gharchive.org/{date_str}-{hour}.json.gz"

def generate_urls_for_date(date_str: str) -> list[str]:
    """날짜 date_str에 대한 0시부터 23시까지 24개의 URL 목록을 생성."""
    return [gharchive_url_for_hour(date_str, h) for h in range(24)]

def fetch_organization_data(date: str, organization: str) -> pd.DataFrame:
    """특정 날짜와 organization의 GH Archive 데이터 수집"""
    all_events = []
    
    for url in generate_urls_for_date(date):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # gzip 압축 해제
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
                for line in f:
                    try:
                        event = json.loads(line.decode('utf-8'))
                        # event가 딕셔너리인지 확인
                        if not isinstance(event, dict):
                            continue
                        # organization 관련 이벤트만 필터링
                        if event.get("org", {}).get("login") == organization:
                            all_events.append(event)
                    except json.JSONDecodeError:
                        continue
            
            time.sleep(0.1)  # API 레이트 리밋 방지
            
        except requests.RequestException as e:
            logger.error(f"❌ {url} 데이터 수집 실패: {e}")
    
    if not all_events:
        return pd.DataFrame()
    
    df = pd.DataFrame(all_events)
    df['date'] = date
    df['created_at'] = pd.to_datetime(df['created_at'])
    
    logger.info(f"📊 {date} - {len(df)}개 이벤트 수집 완료")
    return df

def write_to_delta(df: pd.DataFrame, date: str, organization: str):
    """델타 테이블에 데이터 쓰기"""
    if df.empty:
        logger.warning(f"⚠️ {date} - 빈 데이터")
        return
    
    try:
        delta_path = f"s3://gh-archive/gh-archive-{organization.lower()}"
        partition_path = f"{delta_path}/date={date}"
        
        write_deltalake(partition_path, df, mode="overwrite", partition_by=["date"])
        logger.info(f"💾 {date} 데이터 저장 완료")
        
    except Exception as e:
        logger.error(f"❌ {date} 저장 실패: {e}")

def main():
    """메인 실행 함수"""
    import sys
    
    # 명령행 인수 처리
    if len(sys.argv) != 3:
        print("사용법: python gh_archive_daily_collector.py <YYYY-MM-DD> <organization>")
        print("예시: python gh_archive_daily_collector.py 2024-01-15 apache")
        sys.exit(1)
    
    date = sys.argv[1]
    organization = sys.argv[2]
    
    # 날짜 형식 검증
    try:
        datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        logger.error(f"❌ 잘못된 날짜 형식: {date}. YYYY-MM-DD 형식을 사용하세요.")
        sys.exit(1)
    
    logger.info(f"🚀 {date} {organization} 데이터 수집 시작")
    
    # 데이터 수집
    df = fetch_organization_data(date, organization)
    
    # 델타 테이블에 저장
    write_to_delta(df, date, organization)
    
    logger.info(f"✅ {date} {organization} 데이터 수집 완료")

if __name__ == "__main__":
    main()
