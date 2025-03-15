"""
Data extraction module.
Extracts data from source database based on discovered schema.
"""
import logging
from typing import Dict, List, Any, Generator, Optional
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, select, text
from sqlalchemy.engine import Engine

from connections import get_source_connection_string
from settings import ETL_SETTINGS

# Configure logger
logger = logging.getLogger(__name__)

def get_source_engine() -> Engine:
    """
    Get SQLAlchemy engine for source database.
    
    Returns:
        SQLAlchemy engine
    """
    conn_string = get_source_connection_string()
    return create_engine(conn_string)

def extract_table_data(table_name: str, batch_size: Optional[int] = None) -> Generator[pd.DataFrame, None, None]:
    """
    Extract data from a table in batches.
    
    Args:
        table_name: Name of the table to extract
        batch_size: Number of rows to extract in each batch (None for all rows)
        
    Yields:
        Pandas DataFrame with extracted data batch
    """
    engine = get_source_engine()
    batch_size = batch_size or ETL_SETTINGS['batch_size']
    
    # Get primary key for efficient batching
    metadata = MetaData()
    metadata.reflect(bind=engine, only=[table_name])
    table = metadata.tables[table_name]
    
    # Find primary key or unique index to use for batching
    pk_columns = [col for col in table.primary_key.columns]
    
    if not pk_columns:
        # If no primary key, try to find a unique index
        for idx in table.indexes:
            if idx.unique:
                pk_columns = list(idx.columns)
                break
    
    if pk_columns:
        # Use keyset pagination if we have a primary key or unique index
        return _extract_with_keyset_pagination(engine, table, pk_columns, batch_size)
    else:
        # Fall back to OFFSET/LIMIT if no suitable keys found
        return _extract_with_offset_pagination(engine, table_name, batch_size)

def _extract_with_keyset_pagination(
    engine: Engine, 
    table: Table, 
    key_columns: List[Any], 
    batch_size: int
) -> Generator[pd.DataFrame, None, None]:
    """
    Extract data using keyset pagination (more efficient for large tables).
    
    Args:
        engine: SQLAlchemy engine
        table: SQLAlchemy Table object
        key_columns: Primary key or unique index columns
        batch_size: Number of rows per batch
        
    Yields:
        Pandas DataFrame with extracted data batch
    """
    with engine.connect() as conn:
        # Initial query without where clause
        query = select(table).order_by(*key_columns).limit(batch_size)
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        while not df.empty:
            yield df
            
            if len(df) < batch_size:
                # End of data
                break
                
            # Get last values for key columns to use in next query
            last_row = df.iloc[-1]
            last_values = [last_row[col.name] for col in key_columns]
            
            # Build where clause for next batch
            where_clause = None
            for i, (col, val) in enumerate(zip(key_columns, last_values)):
                if i == 0:
                    where_clause = (col > val)
                else:
                    # For composite keys, we need to handle the case where previous columns are equal
                    prev_cols_equal = None
                    for j in range(i):
                        prev_col = key_columns[j]
                        prev_val = last_values[j]
                        if prev_cols_equal is None:
                            prev_cols_equal = (prev_col == prev_val)
                        else:
                            prev_cols_equal = prev_cols_equal & (prev_col == prev_val)
                    
                    where_clause = where_clause | (prev_cols_equal & (col > val))
            
            # Get next batch
            query = select(table).where(where_clause).order_by(*key_columns).limit(batch_size)
            result = conn.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

def _extract_with_offset_pagination(
    engine: Engine, 
    table_name: str, 
    batch_size: int
) -> Generator[pd.DataFrame, None, None]:
    """
    Extract data using offset pagination (less efficient but more general).
    
    Args:
        engine: SQLAlchemy engine
        table_name: Name of the table
        batch_size: Number of rows per batch
        
    Yields:
        Pandas DataFrame with extracted data batch
    """
    offset = 0
    
    while True:
        query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
        df = pd.read_sql(query, engine)
        
        if df.empty:
            break
            
        yield df
        
        if len(df) < batch_size:
            # End of data
            break
            
        offset += batch_size

def get_table_row_count(table_name: str) -> int:
    """
    Get the number of rows in a table.
    
    Args:
        table_name: Name of the table
        
    Returns:
        Number of rows
    """
    engine = get_source_engine()
    query = f"SELECT COUNT(*) as count FROM {table_name}"
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return result.scalar()

def extract_table_metadata(table_name: str) -> Dict[str, Any]:
    """
    Extract table metadata including column names and types.
    
    Args:
        table_name: Name of the table
        
    Returns:
        Dictionary with table metadata
    """
    engine = get_source_engine()
    metadata = MetaData()
    metadata.reflect(bind=engine, only=[table_name])
    table = metadata.tables[table_name]
    
    columns = []
    for col in table.columns:
        columns.append({
            'name': col.name,
            'type': str(col.type),
            'nullable': col.nullable,
            'primary_key': col.primary_key,
            'default': col.default.arg if col.default else None,
        })
    
    return {
        'name': table_name,
        'columns': columns,
        'primary_key': [col.name for col in table.primary_key.columns],
        'indexes': [{
            'name': idx.name,
            'columns': [col.name for col in idx.columns],
            'unique': idx.unique,
        } for idx in table.indexes]
    }