import os
import sys
import pandas as pd
import json
import ast
import re
from deltalake import DeltaTable, write_deltalake
from datetime import datetime
import pyarrow as pa
import pyarrow.compute as pc

def to_snake_case(name):
    """CamelCase를 snake_case로 변환"""
    # CausalInferenceLab -> causal_inference_lab
    # Pseudo-Lab -> pseudo_lab
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    s3 = s2.replace('-', '_')
    return s3

def convert_data_types(date_str):
    """
    CausalInferenceLab, Pseudo-Lab 데이터의 컬럼 타입을 적절히 변환
    JSON 문자열로 저장된 nested 컬럼들을 struct 타입으로 복원
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
    
    target_logins = ["CausalInferenceLab", "Pseudo-Lab"]
    
    def safe_json_parse(json_str):
        """JSON 문자열을 안전하게 파싱"""
        if pd.isna(json_str) or json_str is None or json_str == "" or json_str == "None":
            return None
        if isinstance(json_str, str):
            try:
                # 먼저 ast.literal_eval 시도 (single quote JSON)
                if json_str.strip().startswith('{'):
                    return ast.literal_eval(json_str)
                # 그 다음 json.loads 시도 (double quote JSON)
                return json.loads(json_str)
            except:
                return None
        return json_str
    
    def create_struct_from_dict_list(dict_list, struct_name):
        """딕셔너리 리스트에서 Arrow struct 타입을 생성"""
        if not dict_list or all(d is None for d in dict_list):
            # 모든 값이 None이면 null 컬럼 반환
            return pa.array([None] * len(dict_list))
        
        # None이 아닌 첫 번째 딕셔너리에서 스키마 추출
        sample_dict = next((d for d in dict_list if d is not None), None)
        if sample_dict is None:
            return pa.array([None] * len(dict_list))
        
        # 스키마 정의 (GitHub Archive 표준 스키마)
        if struct_name == 'actor':
            schema = pa.struct([
                ('id', pa.int64()),
                ('login', pa.string()),
                ('display_login', pa.string()),
                ('gravatar_id', pa.string()),
                ('url', pa.string()),
                ('avatar_url', pa.string())
            ])
        elif struct_name == 'repo':
            schema = pa.struct([
                ('id', pa.int64()),
                ('name', pa.string()),
                ('url', pa.string())
            ])
        elif struct_name == 'org':
            schema = pa.struct([
                ('id', pa.int64()),
                ('login', pa.string()),
                ('gravatar_id', pa.string()),
                ('url', pa.string()),
                ('avatar_url', pa.string())
            ])
        else:  # payload는 동적으로 처리
            # 샘플에서 필드 추출
            fields = []
            for key, value in sample_dict.items():
                if isinstance(value, int):
                    fields.append((key, pa.int64()))
                elif isinstance(value, bool):
                    fields.append((key, pa.bool_()))
                elif isinstance(value, list):
                    fields.append((key, pa.string()))  # 리스트는 JSON 문자열로
                else:
                    fields.append((key, pa.string()))
            schema = pa.struct(fields)
        
        # 딕셔너리를 스키마에 맞게 변환
        struct_data = []
        for d in dict_list:
            if d is None:
                struct_data.append(None)
            else:
                # 스키마에 맞게 값 변환
                converted = {}
                for field in schema:
                    field_name = field.name
                    field_type = field.type
                    value = d.get(field_name)
                    
                    if value is None:
                        converted[field_name] = None
                    elif pa.types.is_integer(field_type):
                        try:
                            converted[field_name] = int(value) if value != "" else None
                        except:
                            converted[field_name] = None
                    elif pa.types.is_boolean(field_type):
                        converted[field_name] = bool(value) if value != "" else None
                    else:  # string
                        converted[field_name] = str(value) if value != "" else None
                
                struct_data.append(converted)
        
        return pa.array(struct_data, type=schema)
    
    def convert_dtypes_arrow(arrow_table):
        """Arrow 테이블의 타입 변환"""
        columns = {}
        
        for field in arrow_table.schema:
            col_name = field.name
            col_data = arrow_table.column(col_name)
            
            # index level 컬럼 제거
            if col_name.startswith("__index_level"):
                continue
                
            # boolean 타입 변환
            if col_name == 'public':
                try:
                    if pa.types.is_string(field.type):
                        bool_col = pc.equal(col_data, pa.scalar("True"))
                        columns[col_name] = bool_col
                    else:
                        columns[col_name] = col_data
                except Exception:
                    columns[col_name] = col_data
            
            # datetime 타입 변환
            elif col_name == 'created_at':
                try:
                    if pa.types.is_string(field.type):
                        timestamp_col = pc.strptime(col_data, format='%Y-%m-%dT%H:%M:%SZ', unit='s')
                        columns[col_name] = timestamp_col
                    else:
                        columns[col_name] = col_data
                except Exception:
                    columns[col_name] = col_data
            
            # payload와 org는 JSON 문자열로 유지 (스키마 변동성이 큼)
            elif col_name in ['payload', 'org']:
                # JSON 문자열 그대로 유지
                columns[col_name] = col_data
            
            # 상대적으로 안정적인 구조만 struct로 변환
            elif col_name in ['actor', 'repo']:
                try:
                    # pandas로 변환해서 JSON 파싱
                    pandas_col = col_data.to_pandas()
                    dict_list = [safe_json_parse(x) for x in pandas_col]
                    struct_col = create_struct_from_dict_list(dict_list, col_name)
                    columns[col_name] = struct_col
                except Exception as e:
                    print(f"⚠️ {col_name} struct 변환 실패: {e}")
                    columns[col_name] = col_data
            
            # 다른 컬럼들
            else:
                if pa.types.is_null(field.type):
                    null_col = pa.array([None] * len(col_data), type=pa.string())
                    columns[col_name] = null_col
                else:
                    columns[col_name] = col_data
        
        return pa.table(columns)
    
    print(f"📅 처리 일자: {date_str}")
    
    for login in target_logins:
        try:
            # 데이터 읽기 (Arrow 테이블로 직접 읽기)
            source_path = f"s3://gh-archive-delta/dl_org_{login}_gh_archive/{date_str}/"
            print(f"📂 읽는 중: {source_path}")
            
            dt = DeltaTable(source_path)
            arrow_table = dt.to_pyarrow_table()
            print(f"📊 {login} 원본 데이터: {len(arrow_table)} 행")
            print(f"📋 원본 스키마:")
            for field in arrow_table.schema:
                print(f"   {field.name}: {field.type}")
            
            # 타입 변환
            arrow_table_converted = convert_dtypes_arrow(arrow_table)
            print(f"📋 변환된 스키마:")
            for field in arrow_table_converted.schema:
                print(f"   {field.name}: {field.type}")
            
            # base_date 컬럼 추가 (파티션용 및 멱등성 보장)
            base_date_col = pa.array([date_str] * len(arrow_table_converted), type=pa.string())
            arrow_table_with_date = arrow_table_converted.append_column('base_date', base_date_col)
            
            # 저장 경로 (통합된 테이블, snake_case 처리)
            login_snake = to_snake_case(login)
            output_path = f"s3://gh-archive-delta/dl_org_{login_snake}/"
            
            # 테이블이 존재하는지 확인
            try:
                existing_table = DeltaTable(output_path, storage_options=storage_options)
                table_exists = True
                print(f"📋 기존 테이블 발견: {output_path}")
            except Exception:
                table_exists = False
                print(f"📋 새 테이블 생성: {output_path}")
            
            if table_exists:
                # 기존 데이터가 있으면 해당 날짜 파티션 삭제 후 추가 (멱등성 보장)
                print(f"🔄 {date_str} 파티션 데이터 교체 중...")
                
                # 해당 날짜 파티션 삭제 (base_date 기준)
                existing_table.delete(f"base_date = '{date_str}'")
                
                # 새 데이터 추가 (스키마 진화 허용)
                write_deltalake(
                    output_path, 
                    arrow_table_with_date, 
                    mode="append", 
                    storage_options=storage_options,
                    partition_by=["base_date"],
                    schema_mode="merge"  # 스키마 진화 허용
                )
            else:
                # 첫 번째 실행 - 테이블 생성
                write_deltalake(
                    output_path, 
                    arrow_table_with_date, 
                    mode="overwrite", 
                    storage_options=storage_options,
                    partition_by=["base_date"]
                )
            print(f"✅ {login}: 타입 변환 완료 → {output_path}")
            
        except Exception as e:
            print(f"❌ {login} 처리 실패: {e}")
            import traceback
            print(f"   전체 트레이스:\n{traceback.format_exc()}")
            continue

def validate_date(date_str):
    """날짜 형식 검증"""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("사용법: python convert_data_types.py YYYY-MM-DD")
        print("예시: python convert_data_types.py 2025-01-01")
        sys.exit(1)
    
    date_param = sys.argv[1]
    
    if not validate_date(date_param):
        print("❌ 잘못된 날짜 형식입니다. YYYY-MM-DD 형식으로 입력해주세요.")
        sys.exit(1)
    
    convert_data_types(date_param) 