"""
Database Query Base Class

This module provides a base class for handling common database query execution patterns,
including parameter handling, error management, and result processing.
"""

import logging
from typing import Dict, Any, List, Optional, Union, Tuple
from abc import ABC, abstractmethod


class DatabaseQuery(ABC):
    """
    Base class for database query operations with common patterns.
    
    This class encapsulates the repetitive query execution pattern found throughout
    the schema profiler, including parameter handling, error management, and logging.
    """
    
    def __init__(self, connector, database_name: str, schema_name: str = None, db_type: str = 'unknown'):
        """
        Initialize the database query handler.
        
        Args:
            connector: Database connector instance
            database_name: Name of the database
            schema_name: Schema name (for databases that support schemas)
            db_type: Database type (mysql, postgresql, mssql)
        """
        self.connector = connector
        self.database_name = database_name
        self.schema_name = schema_name
        self.db_type = db_type.lower()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _get_query_parameters(self, table_name: Optional[str] = None, **kwargs) -> List[Union[str, None]]:
        """
        Get database-specific query parameters.
        
        Args:
            table_name: Optional table name for table-specific queries
            **kwargs: Additional parameters
            
        Returns:
            List of parameters for the query
        """
        if self.db_type == 'mssql':
            if table_name:
                return [table_name]
            else:
                return [self.database_name]
        elif self.db_type == 'postgresql':
            schema = self.schema_name or 'public'
            if table_name:
                return [schema, table_name]
            else:
                return [schema, schema]  # For tables query that needs schema twice
        else:  # mysql
            schema = self.schema_name or self.database_name
            if table_name:
                return [schema, table_name]
            else:
                return [schema]
    
    def _execute_query_safe(self, query: str, params: List[Any] = None, 
                           operation_name: str = "query", table_name: str = None) -> List[Dict[str, Any]]:
        """
        Execute a query with error handling and logging.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            operation_name: Name of the operation for logging
            table_name: Optional table name for more specific error messages
            
        Returns:
            Query results or empty list on error
        """
        try:
            self.logger.debug(f"Executing {operation_name} query" + (f" for table {table_name}" if table_name else ""))
            result = self.connector.execute_query(query, params)
            return result if result else []
        except Exception as e:
            error_msg = f"Error executing {operation_name}"
            if table_name:
                error_msg += f" for table {table_name}"
            error_msg += f": {e}"
            self.logger.error(error_msg)
            return []
    
    def _execute_single_value_query(self, query: str, params: List[Any] = None, 
                                   operation_name: str = "query", table_name: str = None,
                                   value_key: str = None, default_value: Any = 0) -> Any:
        """
        Execute a query that returns a single value.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            operation_name: Name of the operation for logging
            table_name: Optional table name for error messages
            value_key: Key to extract from the first result row
            default_value: Default value to return on error
            
        Returns:
            Single value from query or default value
        """
        result = self._execute_query_safe(query, params, operation_name, table_name)
        if result and len(result) > 0:
            if value_key and value_key in result[0]:
                return result[0][value_key]
            elif len(result[0]) > 0:
                # Return first value from first row
                return list(result[0].values())[0]
        return default_value
    
    def _execute_list_query(self, query: str, params: List[Any] = None,
                           operation_name: str = "query", table_name: str = None,
                           extract_key: str = None) -> List[Any]:
        """
        Execute a query that returns a list of values.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            operation_name: Name of the operation for logging
            table_name: Optional table name for error messages
            extract_key: Key to extract from each result row
            
        Returns:
            List of values or empty list
        """
        result = self._execute_query_safe(query, params, operation_name, table_name)
        if extract_key:
            return [row[extract_key] for row in result if extract_key in row]
        return result
    
    def get_quoted_table_name(self, table_name: str, include_schema: bool = True) -> str:
        """
        Get properly quoted table name for the database type.
        
        Args:
            table_name: Name of the table
            include_schema: Whether to include schema prefix
            
        Returns:
            Properly quoted table name
        """
        # Import here to avoid circular imports
        from .database_dialect import DatabaseDialect
        dialect = DatabaseDialect(self.db_type)
        
        quoted_table = dialect.quote_identifier(table_name)
        
        if include_schema and self.schema_name and self.db_type != 'mysql':
            quoted_schema = dialect.quote_identifier(self.schema_name)
            quoted_table = f"{quoted_schema}.{quoted_table}"
        
        return quoted_table
    
    @abstractmethod
    def get_supported_operations(self) -> List[str]:
        """
        Get list of supported operations for this query handler.
        
        Returns:
            List of operation names
        """
        pass


class MetadataQueryMixin:
    """
    Mixin class providing common metadata query patterns.
    
    This mixin can be used by classes that need to execute common
    metadata queries with consistent parameter handling.
    """
    
    def _get_metadata_with_dialect(self, dialect_method_name: str, table_name: str = None,
                                  operation_name: str = None, extract_key: str = None) -> Union[List[Dict[str, Any]], List[Any]]:
        """
        Execute a metadata query using a dialect method.
        
        Args:
            dialect_method_name: Name of the dialect method to call
            table_name: Optional table name
            operation_name: Name of the operation for logging
            extract_key: Optional key to extract from results
            
        Returns:
            Query results or extracted values
        """
        if not hasattr(self, 'dialect'):
            raise AttributeError("MetadataQueryMixin requires 'dialect' attribute")
        
        # Get query from dialect
        query_method = getattr(self.dialect, dialect_method_name)
        query = query_method()
        
        # Get parameters
        params = self._get_query_parameters(table_name)
        
        # Execute query
        operation = operation_name or dialect_method_name.replace('get_', '').replace('_query', '')
        
        if extract_key:
            return self._execute_list_query(query, params, operation, table_name, extract_key)
        else:
            return self._execute_query_safe(query, params, operation, table_name)


class QueryExecutionStats:
    """
    Simple class to track query execution statistics.
    """
    
    def __init__(self):
        self.total_queries = 0
        self.successful_queries = 0
        self.failed_queries = 0
        self.queries_by_type = {}
    
    def record_query(self, operation_name: str, success: bool):
        """Record a query execution."""
        self.total_queries += 1
        if success:
            self.successful_queries += 1
        else:
            self.failed_queries += 1
        
        if operation_name not in self.queries_by_type:
            self.queries_by_type[operation_name] = {'success': 0, 'failed': 0}
        
        if success:
            self.queries_by_type[operation_name]['success'] += 1
        else:
            self.queries_by_type[operation_name]['failed'] += 1
    
    def get_success_rate(self) -> float:
        """Get overall success rate."""
        if self.total_queries == 0:
            return 0.0
        return (self.successful_queries / self.total_queries) * 100
    
    def get_stats_summary(self) -> Dict[str, Any]:
        """Get comprehensive statistics summary."""
        return {
            'total_queries': self.total_queries,
            'successful_queries': self.successful_queries,
            'failed_queries': self.failed_queries,
            'success_rate': round(self.get_success_rate(), 2),
            'queries_by_type': self.queries_by_type
        } 