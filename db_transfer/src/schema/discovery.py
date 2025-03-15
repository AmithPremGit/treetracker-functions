"""
Database schema discovery module.
Dynamically discovers database tables, columns, and relationships.
"""
from typing import Dict, List, Tuple, Any, Optional
import logging
from sqlalchemy import create_engine, MetaData, inspect, Table
from sqlalchemy.engine import Engine
from sqlalchemy.ext.automap import automap_base

from connections import get_source_connection_string, get_target_connection_string
from settings import should_process_table

# Configure logger
logger = logging.getLogger(__name__)

def discover_source_schema() -> Dict[str, Any]:
    """
    Discover the schema of the source database.
    
    Returns:
        Dictionary with table metadata
    """
    source_conn_string = get_source_connection_string()
    return discover_schema(source_conn_string)

def discover_target_schema() -> Dict[str, Any]:
    """
    Discover the schema of the target database.
    
    Returns:
        Dictionary with table metadata
    """
    target_conn_string = get_target_connection_string()
    return discover_schema(target_conn_string)

def discover_schema(connection_string: str) -> Dict[str, Any]:
    """
    Discover the schema of a database.
    
    Args:
        connection_string: SQLAlchemy connection string
        
    Returns:
        Dictionary with table metadata
    """
    engine = create_engine(connection_string)
    inspector = inspect(engine)
    
    schema_info = {}
    table_names = inspector.get_table_names()
    
    for table_name in table_names:
        if not should_process_table(table_name):
            logger.info(f"Skipping table {table_name} based on configuration")
            continue
            
        columns = inspector.get_columns(table_name)
        primary_key = inspector.get_primary_keys(table_name)
        foreign_keys = inspector.get_foreign_keys(table_name)
        indexes = inspector.get_indexes(table_name)
        
        schema_info[table_name] = {
            'columns': columns,
            'primary_key': primary_key,
            'foreign_keys': foreign_keys,
            'indexes': indexes
        }
    
    return schema_info

def get_table_objects(engine: Engine) -> Dict[str, Table]:
    """
    Get SQLAlchemy Table objects for all tables in the database.
    
    Args:
        engine: SQLAlchemy engine
        
    Returns:
        Dictionary mapping table names to SQLAlchemy Table objects
    """
    metadata = MetaData()
    metadata.reflect(engine)
    return {table.name: table for table in metadata.tables.values()}

def get_relationship_info(schema_info: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Extract relationship information from schema.
    
    Args:
        schema_info: Schema information dictionary
        
    Returns:
        List of dictionaries with relationship information
    """
    relationships = []
    
    for table_name, table_info in schema_info.items():
        for fk in table_info['foreign_keys']:
            relationships.append({
                'source_table': table_name,
                'source_column': fk['constrained_columns'][0],
                'target_table': fk['referred_table'],
                'target_column': fk['referred_columns'][0]
            })
    
    return relationships

def create_missing_tables(source_schema: Dict[str, Any], target_engine: Engine) -> None:
    """
    Create missing tables in the target database based on source schema.
    
    Args:
        source_schema: Source database schema
        target_engine: SQLAlchemy engine for target database
    """
    source_engine = create_engine(get_source_connection_string())
    source_metadata = MetaData()
    source_metadata.reflect(bind=source_engine)
    
    target_metadata = MetaData()
    target_metadata.reflect(bind=target_engine)
    
    # Get existing table names in target
    existing_tables = set(target_metadata.tables.keys())
    
    # Create missing tables
    for table_name in source_schema:
        if table_name not in existing_tables:
            logger.info(f"Creating missing table in target: {table_name}")
            source_table = source_metadata.tables[table_name]
            # Create the table in the target database
            source_table.tometadata(target_metadata).create(target_engine)
            
def compare_schemas(source_schema: Dict[str, Any], target_schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compare source and target schemas to identify differences.
    
    Args:
        source_schema: Source database schema
        target_schema: Target database schema
        
    Returns:
        Dictionary with schema differences
    """
    differences = {}
    
    # Check for missing tables
    source_tables = set(source_schema.keys())
    target_tables = set(target_schema.keys())
    
    missing_tables = source_tables - target_tables
    if missing_tables:
        differences['missing_tables'] = list(missing_tables)
    
    # Check for column differences in common tables
    common_tables = source_tables.intersection(target_tables)
    column_differences = {}
    
    for table in common_tables:
        source_columns = {col['name']: col for col in source_schema[table]['columns']}
        target_columns = {col['name']: col for col in target_schema[table]['columns']}
        
        source_column_names = set(source_columns.keys())
        target_column_names = set(target_columns.keys())
        
        missing_columns = source_column_names - target_column_names
        if missing_columns:
            if table not in column_differences:
                column_differences[table] = {}
            column_differences[table]['missing_columns'] = list(missing_columns)
            
        # Check for type differences in common columns
        common_columns = source_column_names.intersection(target_column_names)
        type_differences = []
        
        for col_name in common_columns:
            source_type = source_columns[col_name]['type']
            target_type = target_columns[col_name]['type']
            
            if str(source_type) != str(target_type):
                type_differences.append({
                    'column': col_name,
                    'source_type': str(source_type),
                    'target_type': str(target_type)
                })
                
        if type_differences:
            if table not in column_differences:
                column_differences[table] = {}
            column_differences[table]['type_differences'] = type_differences
    
    if column_differences:
        differences['column_differences'] = column_differences
        
    return differences