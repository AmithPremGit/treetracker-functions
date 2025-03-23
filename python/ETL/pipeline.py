import time
from typing import Dict, List, Set, Any, Optional, Tuple
from tqdm import tqdm
from sqlalchemy import select, insert, Table, text

from db_connector import DBConnector
from schema import SchemaManager

class ETLPipeline:
    """ETL pipeline for copying data between PostgreSQL databases."""
    
    def __init__(self, 
                 prod_db_uri: str, 
                 dev_db_uri: str, 
                 batch_size: int = 100,
                 tables: Optional[List[str]] = None,
                 dry_run: bool = False):
        """
        Initialize ETL pipeline.
        
        Args:
            prod_db_uri: Production database URI
            dev_db_uri: Development database URI
            batch_size: Batch size for processing records
            tables: Optional list of specific tables to copy
            dry_run: Whether to perform a dry run without making changes
        """
        self.prod_db_uri = prod_db_uri
        self.dev_db_uri = dev_db_uri
        self.batch_size = batch_size
        self.selected_tables = tables
        self.dry_run = dry_run
        
        # Initialize database connectors
        self.prod_db = DBConnector(prod_db_uri, read_only=True)  # Always enforce read-only for prod
        self.dev_db = DBConnector(dev_db_uri, read_only=False)
        
        # Initialize schema manager
        self.schema_manager = None
        
    def run(self) -> None:
        """Run the ETL pipeline."""
        start_time = time.time()
        
        # Connect to databases
        print("Connecting to production database...")
        self.prod_db.connect()
        
        print("Connecting to development database...")
        self.dev_db.connect()
        
        try:
            # Discover schema
            print("Discovering schema...")
            self.schema_manager = SchemaManager(self.prod_db)
            self.schemas = self.schema_manager.discover_schema()
            
            # Determine processing order
            print("Analyzing table dependencies...")
            processing_order = self.schema_manager.get_processing_order()
            
            # Filter tables if specified
            if self.selected_tables:
                # Include dependencies for selected tables
                required_tables = set(self.selected_tables)
                for table in self.selected_tables:
                    dependencies = self.schema_manager.get_table_dependencies(table)
                    required_tables.update(dependencies)
                
                # Filter and maintain order
                processing_order = [t for t in processing_order if t in required_tables]
                
            print(f"Processing tables in the following order: {', '.join(processing_order)}")
            
            if self.dry_run:
                print("DRY RUN: No changes will be made to the development database")
            
            # Process tables
            for table_name in processing_order:
                self._copy_table(table_name)
                
            print(f"ETL process completed in {time.time() - start_time:.2f} seconds")
            
        finally:
            # Disconnect from databases
            self.prod_db.disconnect()
            self.dev_db.disconnect()
    
    def _copy_table(self, table_name: str) -> None:
        """
        Copy data from a table in production to development.
        
        Args:
            table_name: Name of the table to copy
        """
        print(f"Processing table: {table_name}")
        
        # Get table schema
        schema = self.schemas[table_name]
        
        # Get table objects
        prod_table = self.prod_db.get_table_object(table_name)
        dev_table = self.dev_db.get_table_object(table_name)
        
        # Get primary keys
        primary_keys = schema['primary_keys']
        if not primary_keys:
            print(f"Warning: Table '{table_name}' has no primary key, using all columns for pagination")
            primary_keys = [col['name'] for col in schema['columns']]
        
        # Count total rows
        count_query = select([text('COUNT(*)')])
        count_result = self.prod_db.execute_query(count_query.select_from(prod_table))
        total_rows = count_result.scalar()
        
        print(f"Total rows in {table_name}: {total_rows}")
        
        if total_rows == 0:
            print(f"Table {table_name} is empty, skipping")
            return
        
        # Prepare for batched processing
        processed_rows = 0
        last_id = None
        
        with tqdm(total=total_rows, desc=f"Copying {table_name}") as pbar:
            while processed_rows < total_rows:
                # Build query for current batch
                query = select([prod_table])
                
                # Add pagination condition if we have processed rows
                if last_id is not None:
                    # Create condition for continuing from last processed ID
                    # This assumes a single primary key for simplicity
                    # For composite keys, would need more complex logic
                    pk_col = getattr(prod_table.c, primary_keys[0])
                    query = query.where(pk_col > last_id)
                
                # Order by primary key and limit batch size
                query = query.order_by(*[getattr(prod_table.c, pk) for pk in primary_keys])
                query = query.limit(self.batch_size)
                
                # Execute query
                result = self.prod_db.execute_query(query)
                batch_data = [dict(row) for row in result]
                
                if not batch_data:
                    break  # No more data
                
                batch_size = len(batch_data)
                
                # Update last processed ID
                if primary_keys and batch_size > 0:
                    last_id = batch_data[-1][primary_keys[0]]
                
                if not self.dry_run:
                    # Begin transaction
                    self.dev_db.begin_transaction()
                    
                    try:
                        # Insert data into development database
                        for row in batch_data:
                            # Check if row exists by primary key
                            if primary_keys:
                                pk_conditions = [getattr(dev_table.c, pk) == row[pk] for pk in primary_keys]
                                check_query = select([text('1')]).select_from(dev_table)
                                for condition in pk_conditions:
                                    check_query = check_query.where(condition)
                                    
                                exists_result = self.dev_db.execute_query(check_query)
                                exists = exists_result.scalar() is not None
                                
                                if exists:
                                    # Row exists, update it
                                    update_stmt = dev_table.update()
                                    for pk in primary_keys:
                                        update_stmt = update_stmt.where(getattr(dev_table.c, pk) == row[pk])
                                    
                                    self.dev_db.execute_query(update_stmt.values(**row))
                                else:
                                    # Row doesn't exist, insert it
                                    insert_stmt = dev_table.insert().values(**row)
                                    self.dev_db.execute_query(insert_stmt)
                            else:
                                # No primary key, just insert
                                insert_stmt = dev_table.insert().values(**row)
                                self.dev_db.execute_query(insert_stmt)
                        
                        # Commit transaction
                        self.dev_db.commit_transaction()
                        
                    except Exception as e:
                        # Rollback transaction on error
                        self.dev_db.rollback_transaction()
                        print(f"Error copying batch for table {table_name}: {str(e)}")
                        raise
                
                # Update progress
                processed_rows += batch_size
                pbar.update(batch_size)
                
        print(f"Copied {processed_rows} rows from {table_name}")