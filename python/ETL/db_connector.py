from typing import Dict, List, Tuple, Any, Optional, Set
import networkx as nx
from sqlalchemy import create_engine, MetaData, Table, inspect, select, text
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import SQLAlchemyError

class DBConnector:
    """Database connector for PostgreSQL databases."""
    
    def __init__(self, db_uri: str, read_only: bool = False):
        """
        Initialize database connector.
        
        Args:
            db_uri: Database URI in format postgresql://user:pass@host:port/dbname
            read_only: Whether to connect in read-only mode
        """
        self.db_uri = db_uri
        self.read_only = read_only
        self.engine = None
        self.connection = None
        self.metadata = None
        self.inspector = None
        
    def connect(self) -> None:
        """Establish connection to the database."""
        try:
            connect_args = {}
            if self.read_only:
                # Enforce read-only mode at PostgreSQL connection level
                connect_args["options"] = "-c default_transaction_read_only=on"
                print(f"Connecting to {self.get_db_name()} in READ-ONLY mode")
            
            self.engine = create_engine(self.db_uri, connect_args=connect_args)
            self.connection = self.engine.connect()
            
            # Double-check read-only status for production connections
            if self.read_only:
                # Verify read-only status
                result = self.connection.execute(text("SHOW default_transaction_read_only"))
                read_only_status = result.scalar()
                if read_only_status != 'on':
                    self.connection.close()
                    self.engine.dispose()
                    raise ValueError("Failed to establish read-only connection to database")
                print("READ-ONLY mode confirmed at database level")
            
            self.metadata = MetaData()
            self.metadata.reflect(bind=self.engine)
            self.inspector = inspect(self.engine)
            
            print(f"Connected to database: {self.get_db_name()}")
        except SQLAlchemyError as e:
            print(f"Failed to connect to database: {str(e)}")
            raise
    
    def disconnect(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()
        if self.engine:
            self.engine.dispose()
        print(f"Disconnected from database: {self.get_db_name()}")
    
    def get_db_name(self) -> str:
        """Get the database name from the URI."""
        return self.db_uri.split("/")[-1]
    
    def get_tables(self) -> List[str]:
        """Get list of all tables in the database."""
        return self.inspector.get_table_names()
    
    def get_table_object(self, table_name: str) -> Table:
        """Get SQLAlchemy Table object for a given table name."""
        if table_name in self.metadata.tables:
            return self.metadata.tables[table_name]
        else:
            raise ValueError(f"Table '{table_name}' not found in database")
    
    def get_foreign_keys(self, table_name: str) -> List[Dict[str, Any]]:
        """Get foreign keys for a table."""
        return self.inspector.get_foreign_keys(table_name)
    
    def get_primary_keys(self, table_name: str) -> List[str]:
        """Get primary keys for a table."""
        pk_constraint = self.inspector.get_primary_key_constraint(table_name)
        if pk_constraint and 'constrained_columns' in pk_constraint:
            return pk_constraint['constrained_columns']
        return []
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get schema information for a table."""
        columns = self.inspector.get_columns(table_name)
        primary_keys = self.get_primary_keys(table_name)
        foreign_keys = self.get_foreign_keys(table_name)
        
        return {
            'name': table_name,
            'columns': columns,
            'primary_keys': primary_keys,
            'foreign_keys': foreign_keys
        }
    
    def execute_query(self, query: Any) -> Any:
        """Execute a query and return results."""
        try:
            # Prevent modification queries on read-only connections
            if self.read_only and not self._is_select_query(query):
                raise ValueError("Cannot execute modification query on read-only connection")
                
            result = self.connection.execute(query)
            return result
        except SQLAlchemyError as e:
            print(f"Query execution failed: {str(e)}")
            raise
    
    def _is_select_query(self, query: Any) -> bool:
        """
        Check if a query is a SELECT query (read-only).
        
        This is a basic check. The PostgreSQL read-only mode provides
        the real protection at the database level.
        """
        if hasattr(query, 'is_select') and query.is_select:
            return True
        
        # Basic string check (for text() queries)
        if hasattr(query, 'text') and isinstance(query.text, str):
            query_text = query.text.strip().upper()
            return query_text.startswith('SELECT') or query_text.startswith('SHOW')
            
        return False
            
    def begin_transaction(self) -> None:
        """Begin a transaction."""
        self.transaction = self.connection.begin()
        
    def commit_transaction(self) -> None:
        """Commit the current transaction."""
        if hasattr(self, 'transaction') and self.transaction:
            self.transaction.commit()
            
    def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        if hasattr(self, 'transaction') and self.transaction:
            self.transaction.rollback()