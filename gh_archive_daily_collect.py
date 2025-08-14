"""
Enhanced GitHub Archive Daily Collector
Optimized version with timezone support and improved data flow

Key improvements:
1. Maintains current execution format
2. Adds timezone columns (dt_kst, dt_utc, ts_kst, ts_utc)
3. Partitions by dt_kst and organization
4. Streamlined processing flow
5. Prepared for dbt integration
"""

import os
import sys
import subprocess
import logging
import gzip
import json
from datetime import datetime
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from typing import List, Optional

# Import utilities
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from timezone_utils import add_timezone_columns, get_partition_path
from gh_archive_utils import (
    safe_dict_parse, 
    convert_all_columns_to_string,
    to_snake_case,
    get_storage_options,
    filter_by_organizations,
    extract_organization_name,
    get_delta_table_path,
    clean_dataframe_for_delta
)

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
TARGET_ORGANIZATIONS = ["CausalInferenceLab", "Pseudo-Lab", "apache"]
BUCKETS = {
    'raw': 'gh-archive-raw',
    'delta': 'gh-archive-delta'
}


def _get_minio_client():
    """Get MinIO client instance"""
    try:
        from minio import Minio
        return Minio(
            os.getenv("AWS_ENDPOINT_URL", "minio:9000").replace("http://", ""),
            access_key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true"
        )
    except ImportError:
        logger.error("❌ MinIO client not installed. Run 'pip install minio'")
        raise


def _ensure_bucket_exists(bucket_name: str):
    """Ensure bucket exists, create if not"""
    try:
        client = _get_minio_client()
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"📦 Created bucket: {bucket_name}")
    except Exception as e:
        logger.error(f"❌ Failed to create bucket {bucket_name}: {e}")
        raise


def _gharchive_url_for_hour(date_str: str, hour: int) -> str:
    """Generate GitHub Archive URL for specific hour"""
    return f"http://data.gharchive.org/{date_str}-{hour}.json.gz"


def _download_and_upload_to_minio(url: str, date: str, hour: int) -> bool:
    """Download data and upload to MinIO"""
    try:
        bucket_name = BUCKETS['raw']
        _ensure_bucket_exists(bucket_name)
        
        os.makedirs("./tmp", exist_ok=True)
        temp_filename = f"./tmp/{date}-{hour}.json.gz"
        object_name = f"raw_data/{date}/{date}-{hour}.json.gz"
        
        # Download with wget
        wget_cmd = [
            "wget", 
            "-O", temp_filename,
            "--timeout=30",
            "--tries=3",
            url
        ]
        
        logger.info(f"📥 Downloading: {url}")
        result = subprocess.run(wget_cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"❌ wget failed: {result.stderr}")
            return False
        
        # Upload to MinIO
        client = _get_minio_client()
        with open(temp_filename, 'rb') as file_data:
            client.put_object(
                bucket_name,
                object_name,
                file_data,
                length=os.path.getsize(temp_filename),
                content_type="application/gzip"
            )
        
        # Cleanup
        os.remove(temp_filename)
        logger.info(f"✅ Uploaded: {bucket_name}/{object_name}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Download/upload failed: {e}")
        if os.path.exists(temp_filename):
            os.remove(temp_filename)
        return False


def download_all_hours(date: str) -> int:
    """Download all 24 hours of data for a given date"""
    success_count = 0
    
    for hour in range(24):
        url = _gharchive_url_for_hour(date, hour)
        if _download_and_upload_to_minio(url, date, hour):
            success_count += 1
    
    logger.info(f"📊 {date} - Downloaded {success_count}/24 files successfully")
    return success_count


def process_and_filter_to_delta(date: str) -> bool:
    """
    Process downloaded data, filter by organizations, and save to Delta Lake
    with timezone columns and proper partitioning
    """
    try:
        client = _get_minio_client()
        storage_options = get_storage_options()
        
        # Setup Delta Lake bucket
        delta_bucket = BUCKETS['delta']
        _ensure_bucket_exists(delta_bucket)
        
        # Delta table path
        table_name = "gh_archive_filtered"
        delta_path = get_delta_table_path(delta_bucket, table_name)
        
        # Process 24-hour data
        all_data = []
        os.makedirs("./tmp", exist_ok=True)
        
        for hour in range(24):
            raw_object = f"raw_data/{date}/{date}-{hour}.json.gz"
            temp_file = f"./tmp/{date}-{hour}.json.gz"
            
            try:
                # Download from MinIO
                client.fget_object(BUCKETS['raw'], raw_object, temp_file)
                
                # Process gz file
                hour_events = []
                with gzip.open(temp_file, 'rt', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            try:
                                event = json.loads(line)
                                # Quick filter: only events with org field
                                if event.get("org"):
                                    hour_events.append(event)
                            except json.JSONDecodeError:
                                continue  # Skip malformed JSON
                
                if hour_events:
                    all_data.extend(hour_events)
                    logger.info(f"✅ Processed hour {hour}: {len(hour_events)} events")
                
                # Cleanup temp file
                os.remove(temp_file)
                
                # Remove processed raw file to save space
                try:
                    client.remove_object(BUCKETS['raw'], raw_object)
                    logger.info(f"🗑️ Removed processed file: {raw_object}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to remove raw file: {e}")
                    
            except Exception as e:
                logger.error(f"❌ Failed to process hour {hour}: {e}")
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                continue
        
        if not all_data:
            logger.warning("⚠️ No data to process")
            return False
        
        # Convert to DataFrame
        df = pd.DataFrame(all_data)
        logger.info(f"📊 Total events loaded: {len(df)}")
        
        # Filter by target organizations
        df_filtered = filter_by_organizations(df, TARGET_ORGANIZATIONS)
        logger.info(f"📊 Events after org filtering: {len(df_filtered)}")
        
        if len(df_filtered) == 0:
            logger.warning("⚠️ No events match target organizations")
            return False
        
        # Add timezone columns
        df_with_tz = add_timezone_columns(df_filtered)
        
        # Add organization column for partitioning
        df_with_tz['organization'] = df_with_tz['org'].apply(extract_organization_name)
        
        # Clean organization names for partitioning
        df_with_tz['organization'] = df_with_tz['organization'].apply(
            lambda x: to_snake_case(x) if x else 'unknown'
        )
        
        # Clean DataFrame for Delta Lake
        df_clean = clean_dataframe_for_delta(df_with_tz)
        
        # Convert all columns to string for consistency (except timezone columns)
        for col in df_clean.columns:
            if col not in ['ts_kst', 'ts_utc', 'dt_kst', 'dt_utc']:
                df_clean[col] = df_clean[col].astype(str)
        
        # Save to Delta Lake with partitioning
        logger.info(f"💾 Saving to Delta Lake: {delta_path}")
        
        # Check if table exists
        try:
            dt = DeltaTable(delta_path, storage_options=storage_options)
            table_exists = True
            logger.info("📋 Existing table found, appending data")
        except:
            table_exists = False
            logger.info("📋 Creating new table")
        
        if table_exists:
            # For existing table, append with merge on dt_kst + organization
            write_deltalake(
                delta_path,
                df_clean,
                mode="append",
                storage_options=storage_options,
                partition_by=["dt_kst", "organization"],
                schema_mode="merge"
            )
        else:
            # Create new table
            write_deltalake(
                delta_path,
                df_clean,
                mode="overwrite",
                storage_options=storage_options,
                partition_by=["dt_kst", "organization"]
            )
        
        logger.info(f"✅ Successfully saved {len(df_clean)} events to Delta Lake")
        
        # Log partition summary
        partition_summary = df_clean.groupby(['dt_kst', 'organization']).size()
        logger.info(f"📊 Partition summary:\n{partition_summary}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Processing failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


def main():
    """Main execution function"""
    if len(sys.argv) < 3:
        print("Usage: python gh_archive_enhanced_collector.py <YYYY-MM-DD> <type>")
        print("  type: 'download', 'process'")
        print("    - download: Download gz files to MinIO")
        print("    - process: Filter and process to Delta Lake with timezone columns")
        sys.exit(1)
    
    date = sys.argv[1]
    process_type = sys.argv[2]
    
    # Validate inputs
    if process_type not in ['download', 'process']:
        logger.error(f"❌ Invalid type: {process_type}. Use 'download' or 'process'")
        sys.exit(1)
    
    try:
        datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        logger.error(f"❌ Invalid date format: {date}")
        sys.exit(1)
    
    logger.info(f"🚀 Starting {process_type} for date: {date}")
    
    if process_type == 'download':
        success_count = download_all_hours(date)
        if success_count > 0:
            logger.info(f"✅ Download completed: {success_count} files")
        else:
            logger.error("❌ Download failed")
            sys.exit(1)
            
    elif process_type == 'process':
        success = process_and_filter_to_delta(date)
        if success:
            logger.info(f"✅ Processing completed successfully")
        else:
            logger.error("❌ Processing failed")
            sys.exit(1)


if __name__ == "__main__":
    main()