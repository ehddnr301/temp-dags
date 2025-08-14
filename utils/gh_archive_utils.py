"""
Common utility functions for GitHub Archive data processing
Consolidates duplicate functions from the original implementation
"""
import os
import ast
import re
import json
import pandas as pd
import pytz
from typing import Dict, List, Optional, Union
from datetime import datetime


def safe_dict_parse(json_str: Union[str, dict, None]) -> Optional[dict]:
    """
    Safely parse JSON string to dictionary
    Unified version to replace multiple implementations
    
    Args:
        json_str: JSON string, dict, or None
    
    Returns:
        Parsed dictionary or None if parsing fails
    """
    if pd.isna(json_str) or json_str is None or json_str == "" or json_str == "None":
        return None
    
    if isinstance(json_str, dict):
        return json_str
    
    if isinstance(json_str, str):
        try:
            # Try literal_eval first for safer parsing
            if json_str.strip().startswith('{'):
                return ast.literal_eval(json_str)
            return json.loads(json_str)
        except (ValueError, SyntaxError, json.JSONDecodeError):
            return None
    
    return None


def convert_all_columns_to_string(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert all DataFrame columns to string type
    Unified version to replace duplicate implementations
    
    Args:
        df: Input DataFrame
    
    Returns:
        DataFrame with all columns converted to string
    """
    df_copy = df.copy()
    for col in df_copy.columns:
        df_copy[col] = df_copy[col].astype(str)
    return df_copy


def to_snake_case(name: str) -> str:
    """
    Convert CamelCase to snake_case
    
    Args:
        name: String to convert
    
    Returns:
        snake_case string
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    s3 = s2.replace('-', '_')
    return s3


def get_storage_options() -> Dict[str, str]:
    """
    Get MinIO storage options from environment variables
    
    Returns:
        Dictionary with storage options for Delta Lake
    """
    minio_endpoint = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
    
    return {
        "AWS_ENDPOINT_URL": minio_endpoint,
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def filter_by_organizations(df: pd.DataFrame, target_orgs: List[str], org_column: str = 'org') -> pd.DataFrame:
    """
    Filter DataFrame by target organizations
    
    Args:
        df: Input DataFrame
        target_orgs: List of target organization names
        org_column: Column name containing organization data
    
    Returns:
        Filtered DataFrame containing only events from target organizations
    """
    if org_column not in df.columns or len(df) == 0:
        return df
    
    # Parse org column and filter
    df_copy = df.copy()
    df_copy['org_parsed'] = df_copy[org_column].apply(safe_dict_parse)
    
    def is_target_org(org_dict):
        if not org_dict or not isinstance(org_dict, dict):
            return False
        org_login = org_dict.get('login', '')
        return org_login in target_orgs
    
    filtered_df = df_copy[df_copy['org_parsed'].apply(is_target_org)]
    
    # Clean up temporary column
    if len(filtered_df) > 0:
        filtered_df = filtered_df.drop('org_parsed', axis=1)
    
    return filtered_df


def extract_organization_name(org_data: Union[str, dict, None]) -> Optional[str]:
    """
    Extract organization name from org field
    
    Args:
        org_data: Organization data (JSON string or dict)
    
    Returns:
        Organization login name or None
    """
    org_dict = safe_dict_parse(org_data) if isinstance(org_data, str) else org_data
    
    if isinstance(org_dict, dict):
        return org_dict.get('login')
    
    return None


def get_delta_table_path(bucket: str, table_name: str) -> str:
    """
    Generate Delta Lake table path
    
    Args:
        bucket: S3 bucket name
        table_name: Table name
    
    Returns:
        Full S3 path for Delta Lake table
    """
    return f"s3://{bucket}/{table_name}"


def validate_required_columns(df: pd.DataFrame, required_cols: List[str]) -> List[str]:
    """
    Validate that DataFrame has required columns
    
    Args:
        df: DataFrame to validate
        required_cols: List of required column names
    
    Returns:
        List of missing column names
    """
    missing_cols = []
    for col in required_cols:
        if col not in df.columns:
            missing_cols.append(col)
    
    return missing_cols


def clean_dataframe_for_delta(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DataFrame for Delta Lake storage
    - Remove index level columns
    - Handle null values
    - Ensure consistent data types
    
    Args:
        df: Input DataFrame
    
    Returns:
        Cleaned DataFrame ready for Delta Lake storage
    """
    df_clean = df.copy()
    
    # Remove __index_level columns that cause schema conflicts
    index_cols = [col for col in df_clean.columns if col.startswith('__index_level')]
    if index_cols:
        df_clean = df_clean.drop(columns=index_cols)
    
    # Replace 'None' strings with actual None values for better data quality
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            df_clean[col] = df_clean[col].replace('None', None)
    
    return df_clean