"""
Timezone utility functions for GitHub Archive data processing
"""
import pandas as pd
from datetime import datetime
import pytz
from typing import Optional, Union


def add_timezone_columns(df: pd.DataFrame, created_at_col: str = 'created_at') -> pd.DataFrame:
    """
    Add timezone-aware datetime columns to DataFrame
    
    Args:
        df: Input DataFrame with created_at column
        created_at_col: Name of the timestamp column to process
    
    Returns:
        DataFrame with additional timezone columns:
        - dt_kst: Date in KST timezone (for partitioning)
        - dt_utc: Date in UTC timezone
        - ts_kst: Timestamp in KST timezone
        - ts_utc: Timestamp in UTC timezone
    """
    df_copy = df.copy()
    
    # Parse created_at to timestamp if it's string
    if created_at_col in df_copy.columns:
        # Handle string timestamps
        if df_copy[created_at_col].dtype == 'object':
            df_copy[created_at_col] = pd.to_datetime(df_copy[created_at_col], format='%Y-%m-%dT%H:%M:%SZ', errors='coerce')
        
        # Ensure timezone awareness (GitHub data is in UTC)
        if df_copy[created_at_col].dt.tz is None:
            df_copy[created_at_col] = df_copy[created_at_col].dt.tz_localize('UTC')
        
        # Create KST timezone object
        kst_tz = pytz.timezone('Asia/Seoul')
        
        # Add UTC timestamp and date
        df_copy['ts_utc'] = df_copy[created_at_col]
        df_copy['dt_utc'] = df_copy[created_at_col].dt.date
        
        # Add KST timestamp and date
        df_copy['ts_kst'] = df_copy[created_at_col].dt.tz_convert(kst_tz)
        df_copy['dt_kst'] = df_copy['ts_kst'].dt.date
        
    else:
        # If created_at column doesn't exist, create null columns
        df_copy['ts_utc'] = pd.NaT
        df_copy['dt_utc'] = None
        df_copy['ts_kst'] = pd.NaT
        df_copy['dt_kst'] = None
    
    return df_copy


def get_kst_date_from_utc_date(utc_date_str: str) -> str:
    """
    Convert UTC date string to KST date string for partitioning
    
    Args:
        utc_date_str: Date string in YYYY-MM-DD format (UTC)
    
    Returns:
        Date string in YYYY-MM-DD format (KST)
    """
    utc_dt = datetime.strptime(utc_date_str, '%Y-%m-%d')
    utc_dt = pytz.UTC.localize(utc_dt)
    
    kst_tz = pytz.timezone('Asia/Seoul')
    kst_dt = utc_dt.astimezone(kst_tz)
    
    return kst_dt.strftime('%Y-%m-%d')


def get_partition_path(dt_kst: str, organization: str) -> str:
    """
    Generate partition path for Delta Lake storage
    
    Args:
        dt_kst: Date string in YYYY-MM-DD format (KST)
        organization: Organization name
    
    Returns:
        Partition path string like "dt_kst=2024-01-15/organization=apache"
    """
    # Clean organization name for path (convert to snake_case)
    org_clean = organization.lower().replace('-', '_').replace(' ', '_')
    return f"dt_kst={dt_kst}/organization={org_clean}"


def validate_timezone_columns(df: pd.DataFrame) -> bool:
    """
    Validate that timezone columns are properly formatted
    
    Args:
        df: DataFrame to validate
    
    Returns:
        True if all timezone columns are valid, False otherwise
    """
    required_cols = ['dt_kst', 'dt_utc', 'ts_kst', 'ts_utc']
    
    # Check if all required columns exist
    for col in required_cols:
        if col not in df.columns:
            return False
    
    # Check if timestamp columns have timezone info
    for ts_col in ['ts_kst', 'ts_utc']:
        if df[ts_col].dtype == 'datetime64[ns]':
            # Should have timezone info
            return False
    
    return True