"""
Data transformation module.
Transforms data during the ETL process.
"""
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Optional, Callable

from cleaner import clean_dataframe

# Configure logger
logger = logging.getLogger(__name__)

def transform_data(df: pd.DataFrame, table_name: str, column_types: Dict[str, str]) -> pd.DataFrame:
    """
    Transform data for a given table.
    
    Args:
        df: Input DataFrame
        table_name: Name of the table
        column_types: Dictionary mapping column names to their types
        
    Returns:
        Transformed DataFrame
    """
    logger.info(f"Transforming data for table {table_name}")
    
    # First, clean the data
    df = clean_dataframe(df, table_name, column_types)
    
    # Apply table-specific transformations
    # This can be customized based on your specific needs
    transformed_df = _apply_table_transformations(df, table_name)
    
    # Apply general transformations
    transformed_df = _apply_general_transformations(transformed_df)
    
    return transformed_df

def _apply_table_transformations(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Apply table-specific transformations.
    
    Args:
        df: Input DataFrame
        table_name: Name of the table
        
    Returns:
        Transformed DataFrame
    """
    # Table-specific transformations can be defined here
    # Example: If you want to transform the 'users' table differently than the 'orders' table
    
    # Create a copy to avoid modifying the original DataFrame
    transformed_df = df.copy()
    
    # Define transformations for specific tables
    # This is just an example and should be customized based on your specific requirements
    transformation_map = {
        # 'users': _transform_users_table,
        # 'orders': _transform_orders_table,
        # Add more table-specific transformations as needed
    }
    
    # Apply the transformation if a specific one is defined for this table
    if table_name in transformation_map:
        transformer = transformation_map[table_name]
        transformed_df = transformer(transformed_df)
        logger.info(f"Applied specific transformation for table {table_name}")
    
    return transformed_df

def _apply_general_transformations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply general transformations that apply to all tables.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Transformed DataFrame
    """
    # Create a copy to avoid modifying the original DataFrame
    transformed_df = df.copy()
    
    # Add any general transformations here
    # For example, masking sensitive data, standardizing formats, etc.
    
    # Example: Mask email addresses to protect PII
    if 'email' in transformed_df.columns:
        transformed_df['email'] = _mask_email_addresses(transformed_df['email'])
    
    # Example: Mask personal identifiers 
    for col in transformed_df.columns:
        if any(sensitive_field in col.lower() for sensitive_field in ['ssn', 'social', 'tax_id', 'passport']):
            transformed_df[col] = _mask_sensitive_data(transformed_df[col])
    
    return transformed_df

# Example table-specific transformation functions
def _transform_users_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply transformations specific to the users table.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Transformed DataFrame
    """
    # Create a copy to avoid modifying the original DataFrame
    result = df.copy()
    
    # Example transformations for a users table
    # For testing purposes, you might want to anonymize user data
    if 'username' in result.columns:
        result['username'] = 'user_' + result.index.astype(str)
    
    if 'password' in result.columns:
        result['password'] = '******'  # Replace actual passwords
    
    return result

def _transform_orders_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply transformations specific to the orders table.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Transformed DataFrame
    """
    # Create a copy to avoid modifying the original DataFrame
    result = df.copy()
    
    # Example transformations for an orders table
    # For example, standardize order status values
    if 'status' in result.columns:
        # Map various status values to standard ones
        status_mapping = {
            'processing': 'PROCESSING',
            'in process': 'PROCESSING',
            'in_process': 'PROCESSING',
            'shipped': 'SHIPPED',
            'delivered': 'DELIVERED',
            'complete': 'DELIVERED',
            'completed': 'DELIVERED',
            'cancelled': 'CANCELLED', 
            'canceled': 'CANCELLED'
        }
        
        # Apply case-insensitive mapping
        result['status'] = result['status'].astype(str).str.lower()
        result['status'] = result['status'].map(
            lambda x: next((status_mapping[k] for k in status_mapping.keys() if k == x.lower()), x.upper())
        )
    
    return result

# Helper functions for transformations
def _mask_email_addresses(series: pd.Series) -> pd.Series:
    """
    Mask email addresses for privacy.
    
    Args:
        series: Series containing email addresses
        
    Returns:
        Series with masked email addresses
    """
    if series.dtype != 'object':
        return series
        
    # Function to mask an individual email
    def mask_email(email):
        if not isinstance(email, str) or '@' not in email:
            return email
            
        username, domain = email.split('@', 1)
        if len(username) <= 2:
            masked_username = username[0] + '*' * (len(username) - 1) if len(username) > 0 else ''
        else:
            masked_username = username[0] + '*' * (len(username) - 2) + username[-1]
            
        return f"{masked_username}@{domain}"
    
    return series.apply(mask_email)

def _mask_sensitive_data(series: pd.Series) -> pd.Series:
    """
    Mask sensitive data like SSNs, credit card numbers, etc.
    
    Args:
        series: Series containing sensitive data
        
    Returns:
        Series with masked sensitive data
    """
    if series.dtype != 'object':
        return series
        
    # Function to mask an individual value
    def mask_value(value):
        if not isinstance(value, str):
            return value
            
        if len(value) <= 4:
            return '*' * len(value)
        else:
            # Keep last 4 characters, mask the rest
            return '*' * (len(value) - 4) + value[-4:]
    
    return series.apply(mask_value)