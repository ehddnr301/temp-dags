import os
import pandas as pd
import requests
import json
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

def fetch_organization_data(date: str, organization: str) -> pd.DataFrame:
    """특정 날짜와 organization의 GH Archive 데이터 수집"""
    base_url = "https://data.gharchive.org"
    all_events = []
    
    for hour in range(24):
        url = f"{base_url}/{date}-{hour}.json.gz"
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            for line in response.text.strip().split('\n'):
                if line:
                    try:
                        event = json.loads(line)
                        # organization 관련 이벤트만 필터링
                        if (event.get('repo', {}).get('name', '').startswith(f"{organization}/") or
                            event.get('org', {}).get('login') == organization):
                            all_events.append(event)
                    except json.JSONDecodeError:
                        continue
            
            time.sleep(0.1)  # API 레이트 리밋 방지
            
        except requests.RequestException as e:
            logger.error(f"❌ {date} {hour:02d}:00 데이터 수집 실패: {e}")
    
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
