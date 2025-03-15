"""
Data cleaning module.
Provides functions to clean and sanitize data during the ETL process.
"""
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Optional, Callable

from settings import ETL_SETTINGS

# Configure logger
logger = logging.getLogger(__name__)

def clean_dataframe(df: pd.DataFrame, table_name: str, column_types: Dict[str, Any]) -> pd.DataFrame:
    """
    Clean a DataFrame based on column types and configuration.
    
    Args:
        df: Input DataFrame
        table_name: Name of the table
        column_types: Dictionary mapping column names to their types
        
    Returns:
        Cleaned DataFrame
    """
    if not ETL_SETTINGS['enable_cleaning']:
        logger.info(f"Data cleaning disabled. Skipping cleaning for table {table_name}")
        return df
    
    logger.info(f"Cleaning data for table {table_name}")
    cleaned_df = df.copy()
    
    for column, dtype in column_types.items():
        if column not in cleaned_df.columns:
            continue
            
        # Call appropriate cleaning function based on column type
        cleaned_df[column] = _clean_column(cleaned_df[column], dtype)
    
    return cleaned_df

def _clean_column(series: pd.Series, dtype: str) -> pd.Series:
    """
    Clean a single column based on its data type.
    
    Args:
        series: Input series
        dtype: Data type of the column
        
    Returns:
        Cleaned series
    """
    # Get the appropriate cleaning function based on the data type
    cleaning_functions = {
        'string': _clean_string,
        'text': _clean_string,
        'varchar': _clean_string,
        'character varying': _clean_string,
        'char': _clean_string,
        'integer': _clean_numeric,
        'int': _clean_numeric,
        'bigint': _clean_numeric,
        'smallint': _clean_numeric,
        'double': _clean_numeric,
        'double precision': _clean_numeric,
        'float': _clean_numeric,
        'real': _clean_numeric,
        'decimal': _clean_numeric,
        'numeric': _clean_numeric,
        'boolean': _clean_boolean,
        'bool': _clean_boolean,
        'date': _clean_date,
        'datetime': _clean_datetime,
        'timestamp': _clean_datetime,
        'timestamp without time zone': _clean_datetime,
        'timestamp with time zone': _clean_datetime,
        'time': _clean_time,
        'time without time zone': _clean_time,
        'time with time zone': _clean_time,
    }
    
    # Find the corresponding cleaning function
    dtype_lower = dtype.lower()
    cleaning_func = None
    
    for type_key, func in cleaning_functions.items():
        if type_key in dtype_lower:
            cleaning_func = func
            break
    
    # Apply the cleaning function if found
    if cleaning_func:
        return cleaning_func(series)
    else:
        # For unsupported types, return the original series
        return series

def _clean_string(series: pd.Series) -> pd.Series:
    """
    Clean string data.
    - Convert None to empty string
    - Strip whitespace
    - Handle control characters
    
    Args:
        series: Input string series
        
    Returns:
        Cleaned string series
    """
    # Handle null values
    series = series.fillna('')
    
    # Convert non-string values to strings
    series = series.astype(str)
    
    # Strip whitespace
    series = series.str.strip()
    
    # Remove control characters
    series = series.str.replace(r'[\x00-\x1F\x7F]', '', regex=True)
    
    return series

def _clean_numeric(series: pd.Series) -> pd.Series:
    """
    Clean numeric data.
    - Convert invalid numerics to NaN
    - Handle outliers based on configuration
    
    Args:
        series: Input numeric series
        
    Returns:
        Cleaned numeric series
    """
    # Convert to numeric, coercing errors to NaN
    series = pd.to_numeric(series, errors='coerce')
    
    return series

def _clean_boolean(series: pd.Series) -> pd.Series:
    """
    Clean boolean data.
    - Map various values to True/False
    
    Args:
        series: Input boolean series
        
    Returns:
        Cleaned boolean series
    """
    # Define mappings for boolean values
    true_values = ['true', 't', 'yes', 'y', '1']
    false_values = ['false', 'f', 'no', 'n', '0']
    
    # Convert to string first to handle different types
    s_str = series.astype(str).str.lower()
    
    # Create a Series of NaN values initially
    result = pd.Series(index=series.index, data=np.nan)
    
    # Map values
    result[s_str.isin(true_values)] = True
    result[s_str.isin(false_values)] = False
    
    return result

def _clean_date(series: pd.Series) -> pd.Series:
    """
    Clean date data.
    - Convert invalid dates to NaT
    - Handle invalid date formats
    
    Args:
        series: Input date series
        
    Returns:
        Cleaned date series
    """
    # Convert to datetime with only date information
    series = pd.to_datetime(series, errors='coerce').dt.date
    
    return series

def _clean_datetime(series: pd.Series) -> pd.Series:
    """
    Clean datetime data.
    - Convert invalid datetimes to NaT
    - Handle invalid datetime formats
    
    Args:
        series: Input datetime series
        
    Returns:
        Cleaned datetime series
    """
    # Convert to datetime
    series = pd.to_datetime(series, errors='coerce')
    
    return series

def _clean_time(series: pd.Series) -> pd.Series:
    """
    Clean time data.
    - Convert invalid times to NaT
    - Handle invalid time formats
    
    Args:
        series: Input time series
        
    Returns:
        Cleaned time series
    """
    # This is more complex as pandas doesn't have a time-only type
    # Convert to datetime, then extract time component
    try:
        # Try to convert to datetime first
        dt_series = pd.to_datetime(series, errors='coerce')
        # Extract time component if conversion was successful
        return dt_series.dt.time
    except Exception:
        # If any conversion fails, return original series
        return series