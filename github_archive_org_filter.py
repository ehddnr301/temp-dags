import json
import os
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.fs
from deltalake import DeltaTable, write_deltalake
import pandas as pd
import ast  # ast.literal_eval을 위해 추가
import sys
from datetime import datetime

def process_single_file(file_path, storage_options, target_logins):
    """
    단일 파일을 처리하는 함수
    """
    try:
        # S3 경로에서 bucket과 key 분리
        if file_path.startswith('s3://'):
            # s3://bucket/key 형태에서 bucket/key만 추출
            s3_path = file_path.replace('s3://', '')
        else:
            s3_path = file_path
            
        # Parquet 파일 직접 읽기
        s3_fs = pyarrow.fs.S3FileSystem(
            endpoint_override="localhost:30090",
            access_key="minioadmin",
            secret_key="minioadmin",
            scheme="http"
        )
        table = pq.read_table(s3_path, filesystem=s3_fs)
        pdf = table.to_pandas()
        
        print(f"  파일 처리 중: {file_path} ({len(pdf)} 행)")
        
        # org 컬럼 처리
        if 'org' in pdf.columns and len(pdf) > 0:
            def safe_dict_parse(x):
                if pd.isna(x) or x is None:
                    return None
                if isinstance(x, str):
                    try:
                        return ast.literal_eval(x)
                    except (ValueError, SyntaxError):
                        return None
                return x
            
            pdf['org_parsed'] = pdf['org'].apply(safe_dict_parse)
            filtered_pdf = pdf[pdf['org_parsed'].apply(lambda x: x and isinstance(x, dict) and x.get('login') in target_logins)]
            
            if len(filtered_pdf) > 0:
                filtered_pdf = filtered_pdf.drop('org_parsed', axis=1)
                # 모든 컬럼을 string 타입으로 변환
                for col in filtered_pdf.columns:
                    filtered_pdf[col] = filtered_pdf[col].astype(str)
                return filtered_pdf
                    
    except Exception as e:
        print(f"  ❌ 파일 처리 실패 {file_path}")
        print(f"     에러 타입: {type(e).__name__}")
        print(f"     에러 메시지: {str(e)}")
        import traceback
        print(f"     전체 트레이스:\n{traceback.format_exc()}")
    
    return None

def process_gh_archive_data(date_str):
    """
    GitHub Archive 데이터를 파일별로 처리하는 함수
    """
    # ───── MinIO (S3) 환경 변수 설정 ─────
    os.environ["AWS_ACCESS_KEY_ID"]     = "minioadmin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
    os.environ["AWS_ENDPOINT_URL"]      = "http://localhost:30090"
    os.environ["AWS_ALLOW_HTTP"]        = "true"
    os.environ["AWS_CONDITIONAL_PUT"]   = "etag"

    TABLE_PATH = f"s3://gh-archive-delta/CausalInferenceLab/{date_str}/"
    
    print(f"처리 시작: {date_str}")
    print(f"테이블 경로: {TABLE_PATH}")

    # Delta Lake 테이블에서 파일 목록 가져오기
    dt = DeltaTable(TABLE_PATH)
    files = dt.files()
    
    print(f"총 {len(files)}개 파일 발견")
    print("첫 번째 파일 경로 샘플:", files[0] if files else "없음")
    
    target_logins = ["CausalInferenceLab", "Pseudo-Lab", "apache"]
    storage_options = {
        "AWS_ENDPOINT_URL": "http://localhost:30090",
        "AWS_ACCESS_KEY_ID": "minioadmin",
        "AWS_SECRET_ACCESS_KEY": "minioadmin",
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }
    
    new_table_path = f"s3://gh-archive-delta/dl_org_filtered_gh_archive/{date_str}/"
    all_filtered_data = []
    
    # 각 파일을 개별적으로 처리
    for i, file_path in enumerate(files, 1):
        print(f"[{i}/{len(files)}] 파일 처리 중...")
        print(f"  원본 파일 경로: {file_path}")
        
        # TABLE_PATH를 기준으로 전체 파일 경로 구성
        # TABLE_PATH가 s3://gh-archive-delta/CausalInferenceLab/2025-01-01/ 형태이므로
        # 파일명을 그대로 붙이면 올바른 경로가 됨
        if not TABLE_PATH.endswith('/'):
            full_s3_path = f"{TABLE_PATH}/{file_path}"
        else:
            full_s3_path = f"{TABLE_PATH}{file_path}"
            
        print(f"  처리할 파일: {full_s3_path}")
        filtered_data = process_single_file(full_s3_path, storage_options, target_logins)
        
        if filtered_data is not None and len(filtered_data) > 0:
            all_filtered_data.append(filtered_data)
            print(f"  ✅ {len(filtered_data)}개 행 필터링됨")
    
    # 모든 필터링된 데이터 결합
    if all_filtered_data:
        final_pdf = pd.concat(all_filtered_data, ignore_index=True)
        print(f"\n=== 전체 필터링 결과: {len(final_pdf)} 행 ===")
        
        try:
            write_deltalake(new_table_path, final_pdf, mode="overwrite", storage_options=storage_options)
            print(f"✅ 필터링된 데이터 저장 완료: {new_table_path}")
            print(f"   저장된 행 수: {len(final_pdf)}")
            print(f"   저장된 컬럼 수: {len(final_pdf.columns)}")
        except Exception as e:
            print(f"❌ Delta Lake 저장 실패: {e}")
    else:
        print("필터링된 결과가 없습니다.")

def validate_date(date_str):
    """날짜 형식 검증"""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

if __name__ == "__main__":
    # 명령행 인수에서 날짜 받기
    if len(sys.argv) != 2:
        print("사용법: python temp.py YYYY-MM-DD")
        print("예시: python temp.py 2025-01-01")
        sys.exit(1)
    
    date_param = sys.argv[1]
    
    # 날짜 형식 검증
    if not validate_date(date_param):
        print("❌ 잘못된 날짜 형식입니다. YYYY-MM-DD 형식으로 입력해주세요.")
        print("예시: 2025-01-01")
        sys.exit(1)
    
    process_gh_archive_data(date_param)