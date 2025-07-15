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

def gharchive_url_for_hour(date_str: str, hour: int) -> str:
    """
    주어진 날짜(date_str)와 시간(hour)에 해당하는 GH Archive URL을 반환.
    ex) 2025-07-12, 0시 -> "http://data.gharchive.org/2025-07-12-0.json.gz"
    """
    return f"http://data.gharchive.org/{date_str}-{hour}.json.gz"

def generate_urls_for_date(date_str: str) -> list[str]:
    """날짜 date_str에 대한 0시부터 23시까지 24개의 URL 목록을 생성."""
    return [gharchive_url_for_hour(date_str, h) for h in range(24)]

def download_data(url: str) -> bytes:
    """데이터 다운로드"""
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.content

def unzip_data(compressed_data: bytes) -> list[dict]:
    """압축 해제 및 JSON 파싱"""
    events = []
    with gzip.GzipFile(fileobj=io.BytesIO(compressed_data)) as f:
        for line in f:
            try:
                event = json.loads(line.decode('utf-8'))
                if isinstance(event, dict):
                    events.append(event)
            except json.JSONDecodeError:
                continue
    return events

def save_to_delta(events: list[dict], date: str, organization: str):
    """델타 테이블에 저장"""
    if not events:
        logger.warning(f"⚠️ {date} - 저장할 데이터가 없습니다")
        return
    
    df = pd.DataFrame(events)
    df['date'] = date
    df['created_at'] = pd.to_datetime(df['created_at'])
    
    delta_path = f"s3://gh-archive/gh-archive-{organization.lower()}"
    partition_path = f"{delta_path}/date={date}"
    
    write_deltalake(partition_path, df, mode="overwrite", partition_by=["date"])
    logger.info(f"💾 {date} - {len(df)}개 이벤트 저장 완료")

def download_all_data(date: str) -> list[dict]:
    """특정 날짜의 모든 데이터 다운로드"""
    all_events = []
    
    for url in generate_urls_for_date(date):
        try:
            compressed_data = download_data(url)
            events = unzip_data(compressed_data)
            all_events.extend(events)
            time.sleep(0.1)  # 레이트 리밋 방지
        except Exception as e:
            logger.error(f"❌ {url} 처리 실패: {e}")
    
    logger.info(f"📊 {date} - 총 {len(all_events)}개 이벤트 다운로드 완료")
    return all_events

def main():
    """메인 실행 함수"""
    import sys
    
    if len(sys.argv) != 4:
        print("사용법: python gh_archive_daily_collector.py <YYYY-MM-DD> <organization> <func_type>")
        print("func_type 옵션:")
        print("  - download: 데이터 다운로드만")
        print("  - unzip: 압축 해제만")
        print("  - save: 델타 테이블 저장만")
        print("  - all: 전체 프로세스 실행")
        sys.exit(1)
    
    date = sys.argv[1]
    organization = sys.argv[2]
    func_type = sys.argv[3]
    
    try:
        datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        logger.error(f"❌ 잘못된 날짜 형식: {date}")
        sys.exit(1)
    
    logger.info(f"🚀 {date} 데이터 수집 시작 - 함수 타입: {func_type}")
    
    if func_type == "download":
        # 데이터 다운로드만
        events = download_all_data(date)
        logger.info(f"✅ {date} 데이터 다운로드 완료 - {len(events)}개 이벤트")
        
    elif func_type == "unzip":
        # 압축 해제만 (이미 다운로드된 데이터가 있다고 가정)
        logger.info(f"📦 {date} 압축 해제 작업 시작")
        # 실제로는 다운로드된 데이터를 다시 압축 해제하는 것이므로
        # 여기서는 다운로드와 압축 해제를 함께 수행
        events = download_all_data(date)
        logger.info(f"✅ {date} 압축 해제 완료 - {len(events)}개 이벤트")
        
    elif func_type == "save":
        # 델타 테이블 저장만 (이미 처리된 데이터가 있다고 가정)
        logger.info(f"💾 {date} 델타 테이블 저장 작업 시작")
        # 실제로는 이전 단계에서 처리된 데이터를 사용해야 하지만
        # 여기서는 전체 프로세스를 실행
        events = download_all_data(date)
        save_to_delta(events, date, organization)
        logger.info(f"✅ {date} 델타 테이블 저장 완료")
        
    elif func_type == "all":
        # 전체 프로세스 실행
        events = download_all_data(date)
        save_to_delta(events, date, organization)
        logger.info(f"✅ {date} 전체 프로세스 완료")
        
    else:
        logger.error(f"❌ 잘못된 함수 타입: {func_type}")
        print("사용 가능한 함수 타입: download, unzip, save, all")
        sys.exit(1)

if __name__ == "__main__":
    main()
