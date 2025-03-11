"""
Database connection configuration for ETL processes.
Supports both source and target database connections.
"""
import os
from typing import Dict, Any

# Source database configuration
SOURCE_DB = {
    'type': os.environ.get('SOURCE_DB_TYPE', 'postgresql'),
    'host': os.environ.get('SOURCE_DB_HOST', 'localhost'),
    'port': os.environ.get('SOURCE_DB_PORT', '5432'),
    'database': os.environ.get('SOURCE_DB_NAME', 'source_db'),
    'user': os.environ.get('SOURCE_DB_USER', 'user'),
    'password': os.environ.get('SOURCE_DB_PASSWORD', 'password'),
}

# Target database configuration
TARGET_DB = {
    'type': os.environ.get('TARGET_DB_TYPE', 'postgresql'),
    'host': os.environ.get('TARGET_DB_HOST', 'localhost'),
    'port': os.environ.get('TARGET_DB_PORT', '5432'),
    'database': os.environ.get('TARGET_DB_NAME', 'target_db'),
    'user': os.environ.get('TARGET_DB_USER', 'user'),
    'password': os.environ.get('TARGET_DB_PASSWORD', 'password'),
}

def get_connection_string(db_config: Dict[str, Any]) -> str:
    """
    Generate a SQLAlchemy connection string from database configuration.
    
    Args:
        db_config: Dictionary containing database configuration
        
    Returns:
        SQLAlchemy connection string
    """
    db_type = db_config['type']
    
    if db_type == 'postgresql':
        return f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    elif db_type == 'mysql':
        return f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    elif db_type == 'sqlite':
        return f"sqlite:///{db_config['database']}"
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

def get_source_connection_string() -> str:
    """Get the connection string for the source database."""
    return get_connection_string(SOURCE_DB)

def get_target_connection_string() -> str:
    """Get the connection string for the target database."""
    return get_connection_string(TARGET_DB)