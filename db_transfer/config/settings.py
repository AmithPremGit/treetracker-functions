import os
from typing import Dict, Any, List, Optional

# ETL pipeline settings
ETL_SETTINGS = {
    # Tables to include in the ETL process
    # If empty, will attempt to transfer all tables
    'include_tables': os.getenv('ETL_INCLUDE_TABLES', '').split(',') if os.getenv('ETL_INCLUDE_TABLES') else [],
    
    # Tables to exclude from the ETL process
    'exclude_tables': os.getenv('ETL_EXCLUDE_TABLES', '').split(',') if os.getenv('ETL_EXCLUDE_TABLES') else [],
    
    # Batch size for processing large tables
    'batch_size': int(os.getenv('ETL_BATCH_SIZE', '1000')),
    
    # Number of retry attempts for database operations
    'max_retries': int(os.getenv('ETL_MAX_RETRIES', '3')),
    
    # Enable/disable data cleaning
    'enable_cleaning': os.getenv('ETL_ENABLE_CLEANING', 'true').lower() == 'true',
    
    # Enable/disable schema validation
    'validate_schema': os.getenv('ETL_VALIDATE_SCHEMA', 'true').lower() == 'true',
    
    # Truncate target tables before loading
    'truncate_target': os.getenv('ETL_TRUNCATE_TARGET', 'false').lower() == 'true',
}

# Logging settings
LOG_SETTINGS = {
    'level': os.getenv('LOG_LEVEL', 'INFO'),
    'file': os.getenv('LOG_FILE', 'logs/etl.log'),
    'rotation': os.getenv('LOG_ROTATION', '10 MB'),  # Size-based rotation
    'retention': os.getenv('LOG_RETENTION', '30 days'),  # Time-based retention
}

def get_table_list() -> List[str]:
    """
    Get the list of tables to process based on include/exclude settings.
    This will be populated at runtime with the discovered tables.
    
    Returns:
        List of table names to process
    """
    # This is a placeholder - actual implementation will use
    # schema discovery to get all tables and filter based on settings
    return ETL_SETTINGS['include_tables']

def should_process_table(table_name: str) -> bool:
    """
    Determine if a table should be processed based on include/exclude settings.
    
    Args:
        table_name: Name of the table to check
        
    Returns:
        True if the table should be processed, False otherwise
    """
    include_tables = ETL_SETTINGS['include_tables']
    exclude_tables = ETL_SETTINGS['exclude_tables']
    
    # If include_tables is not empty, only process tables in that list
    if include_tables and table_name not in include_tables:
        return False
    
    # If exclude_tables is not empty, skip tables in that list
    if exclude_tables and table_name in exclude_tables:
        return False
    
    return True