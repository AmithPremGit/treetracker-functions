from typing import Dict, List, Set, Any, Optional
import networkx as nx
from sqlalchemy import Table

from db_connector import DBConnector

class SchemaManager:
    """Manages database schema discovery and table dependencies."""
    
    def __init__(self, db_connector: DBConnector):
        """
        Initialize schema manager.
        
        Args:
            db_connector: Database connector instance
        """
        self.db_connector = db_connector
        self.dependency_graph = None
        self.schemas = {}
    
    def discover_schema(self) -> Dict[str, Dict[str, Any]]:
        """
        Discover schema for all tables in the database.
        
        Returns:
            Dictionary mapping table names to schema information
        """
        tables = self.db_connector.get_tables()
        
        for table_name in tables:
            self.schemas[table_name] = self.db_connector.get_table_schema(table_name)
            
        return self.schemas
    
    def build_dependency_graph(self) -> nx.DiGraph:
        """
        Build a directed graph of table dependencies based on foreign keys.
        
        Returns:
            NetworkX directed graph of table dependencies
        """
        if not self.schemas:
            self.discover_schema()
            
        graph = nx.DiGraph()
        
        # Add all tables as nodes
        for table_name in self.schemas:
            graph.add_node(table_name)
        
        # Add edges for foreign key relationships
        for table_name, schema in self.schemas.items():
            for fk in schema['foreign_keys']:
                referenced_table = fk['referred_table']
                graph.add_edge(referenced_table, table_name)  # Parent -> Child
        
        self.dependency_graph = graph
        return graph
    
    def get_table_dependencies(self, table_name: str) -> Set[str]:
        """
        Get all tables that the specified table depends on (recursively).
        
        Args:
            table_name: Name of the table
            
        Returns:
            Set of table names that the specified table depends on
        """
        if not self.dependency_graph:
            self.build_dependency_graph()
        
        # Get all ancestors (tables this table depends on)
        if table_name in self.dependency_graph:
            return set(nx.ancestors(self.dependency_graph, table_name))
        return set()
    
    def get_dependent_tables(self, table_name: str) -> Set[str]:
        """
        Get all tables that depend on the specified table (recursively).
        
        Args:
            table_name: Name of the table
            
        Returns:
            Set of table names that depend on the specified table
        """
        if not self.dependency_graph:
            self.build_dependency_graph()
        
        # Get all descendants (tables that depend on this table)
        if table_name in self.dependency_graph:
            return set(nx.descendants(self.dependency_graph, table_name))
        return set()
    
    def get_processing_order(self) -> List[str]:
        """
        Get tables in order for processing, respecting dependencies.
        
        Returns:
            List of table names in processing order
        """
        if not self.dependency_graph:
            self.build_dependency_graph()
        
        # Topological sort gives us an order where parents come before children
        try:
            return list(nx.topological_sort(self.dependency_graph))
        except nx.NetworkXUnfeasible:
            print("Circular dependencies detected in database schema.")
            # Handle cycles by breaking them and finding a valid ordering
            cycles = list(nx.simple_cycles(self.dependency_graph))
            print(f"Found {len(cycles)} cycles in schema dependencies")
            
            # Create a copy of the graph to break cycles
            dag = self.dependency_graph.copy()
            
            # Break cycles by removing the "weakest" edges
            for cycle in cycles:
                # Identify the edge to remove (for simplicity, remove first edge in cycle)
                if len(cycle) > 1:
                    source = cycle[-1]
                    target = cycle[0]
                    if dag.has_edge(source, target):
                        dag.remove_edge(source, target)
                        print(f"Breaking cycle by removing dependency: {source} -> {target}")
            
            # Now the graph should be acyclic and we can do a topological sort
            return list(nx.topological_sort(dag))