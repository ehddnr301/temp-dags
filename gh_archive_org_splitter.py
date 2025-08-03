import os
import sys
import pandas as pd
from deltalake import DeltaTable, write_deltalake
from datetime import datetime
import ast

def process_by_org(date_str):
    """
    일자별 필터링된 데이터를 target_login별로 분리해서 저장
    """
    # MinIO 환경 설정
    os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
    os.environ["AWS_ENDPOINT_URL"] = "http://localhost:30090"
    os.environ["AWS_ALLOW_HTTP"] = "true"
    
    storage_options = {
        "AWS_ENDPOINT_URL": "http://localhost:30090",
        "AWS_ACCESS_KEY_ID": "minioadmin",
        "AWS_SECRET_ACCESS_KEY": "minioadmin",
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }
    
    # 소스 경로
    source_path = f"s3://gh-archive-delta/dl_org_filtered_gh_archive/{date_str}/"
    target_logins = ["CausalInferenceLab", "Pseudo-Lab", "apache"]
    
    print(f"📅 처리 일자: {date_str}")
    print(f"📂 소스 경로: {source_path}")
    
    try:
        # 필터링된 데이터 읽기
        dt = DeltaTable(source_path)
        df = dt.to_pandas()
        print(f"📊 전체 데이터: {len(df)} 행")
        
        # org 컬럼 파싱 및 각 login별로 분리
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
        
        # 각 target_login별로 처리
        for login in target_logins:
            # 특정 login 데이터만 필터링
            org_df = df[df['org_parsed'].apply(lambda x: x and isinstance(x, dict) and x.get('login') == login)]
            
            if len(org_df) > 0:
                # org_parsed 컬럼 제거
                org_df_clean = org_df.drop('org_parsed', axis=1)
                
                # 저장 경로
                output_path = f"s3://gh-archive-delta/dl_org_{login}_gh_archive/{date_str}/"
                
                # Delta Lake에 저장
                write_deltalake(output_path, org_df_clean, mode="overwrite", storage_options=storage_options)
                print(f"✅ {login}: {len(org_df_clean)}행 저장 완료 → {output_path}")
            else:
                print(f"⚠️ {login}: 데이터 없음")
                
    except Exception as e:
        print(f"❌ 처리 실패: {e}")
        raise

def validate_date(date_str):
    """날짜 형식 검증"""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("사용법: python process_by_org.py YYYY-MM-DD")
        print("예시: python process_by_org.py 2025-01-01")
        sys.exit(1)
    
    date_param = sys.argv[1]
    
    if not validate_date(date_param):
        print("❌ 잘못된 날짜 형식입니다. YYYY-MM-DD 형식으로 입력해주세요.")
        sys.exit(1)
    
    process_by_org(date_param) 