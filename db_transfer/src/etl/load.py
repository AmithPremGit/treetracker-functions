"""
Data loading module.
Loads transformed data into the target database.
Supports batch loading for better performance with large datasets.
"""
import logging
import pandas as pd
from typing import Dict, List, Any, Optional
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.exc import SQLAlchemyError

from connections import get_target_connection_string
from settings import ETL_SETTINGS

# Configure logger
logger = logging.getLogger(__name__)

def get_target_engine():
    """
    Get SQLAlchemy engine for target database.
    
    Returns:
        SQLAlchemy engine
    """
    conn_string = get_target_connection_string()
    return create_engine(conn_string)

def _truncate_table(engine, table_name: str) -> None:
    """
    Truncate a table in the target database.
    
    Args:
        engine: SQLAlchemy engine
        table_name: Name of the table to truncate
    """
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name}"))

def _load_dataframe(engine, df: pd.DataFrame, table_name: str) -> int:
    """
    Load a DataFrame into a table.
    
    Args:
        engine: SQLAlchemy engine
        df: DataFrame to load
        table_name: Target table name
        
    Returns:
        Number of rows loaded
    """
    # Get the max number of retries from settings
    max_retries = ETL_SETTINGS['max_retries']
    retries = 0
    
    while retries <= max_retries:
        try:
            # Use pandas to_sql with 'append' mode to add data to the table
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False,
                chunksize=1000  # Process in chunks for better performance
            )
            return len(df)
        except SQLAlchemyError as e:
            retries += 1
            if retries > max_retries:
                raise
            logger.warning(f"Retry {retries}/{max_retries} loading data into {table_name}: {str(e)}")
    
    return 0  # Should never reach here due to the exception above

def load_data(df: pd.DataFrame, table_name: str, truncate_first: Optional[bool] = None) -> int:
    """
    Load data into the target database.
    
    Args:
        df: DataFrame to load
        table_name: Target table name
        truncate_first: Whether to truncate the target table before loading
        
    Returns:
        Number of rows loaded
    """
    if df.empty:
        logger.warning(f"No data to load for table {table_name}")
        return 0
    
    engine = get_target_engine()
    
    # Get truncate setting from parameters or global settings
    should_truncate = truncate_first if truncate_first is not None else ETL_SETTINGS['truncate_target']
    
    try:
        # Truncate table if requested
        if should_truncate:
            _truncate_table(engine, table_name)
            logger.info(f"Truncated table {table_name}")
        
        # Load data
        row_count = _load_dataframe(engine, df, table_name)
        logger.info(f"Loaded {row_count} rows into table {table_name}")
        
        return row_count
    except Exception as e:
        logger.error(f"Error loading data into table {table_name}: {str(e)}")
        raise