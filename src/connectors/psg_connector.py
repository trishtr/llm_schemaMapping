"""
PostgreSQL Database Connector

This module provides a PostgreSQL-specific implementation of the base database connector
using psycopg2 library.
"""

import logging
from typing import Any, Dict, Optional, List, Tuple
import psycopg2
from psycopg2 import Error as PostgreSQLError
from psycopg2.extras import RealDictCursor, RealDictRow
from psycopg2.pool import SimpleConnectionPool, ThreadedConnectionPool
from psycopg2.extensions import connection as psycopg2_connection

from .base_connector import (
    BaseConnector, 
    ConnectionConfig, 
    ConnectionError, 
    QueryError,
    DatabaseError
)


class PostgreSQLConnector(BaseConnector):
    """
    PostgreSQL database connector implementation.
    
    Features:
    - Connection pooling
    - SSL support
    - Schema support
    - JSON/JSONB support
    - Array support
    - Transaction support
    """
    
    def __init__(self, config: ConnectionConfig):
        """
        Initialize PostgreSQL connector.
        
        Args:
            config: Connection configuration object
        """
        super().__init__(config)
        self._pool = None
        self._pool_config = None
        
    def _create_connection(self) -> Any:
        """
        Create a new PostgreSQL connection.
        
        Returns:
            PostgreSQL connection object
            
        Raises:
            ConnectionError: If connection creation fails
        """
        try:
            connection_params = {
                'host': self.config.host,
                'port': self.config.port,
                'user': self.config.username,
                'password': self.config.password,
                'database': self.config.database,
                'connect_timeout': self.config.connection_timeout,
                'options': '-c timezone=UTC'
            }
            
            # Add schema if specified
            if self.config.schema:
                connection_params['options'] += f' -c search_path={self.config.schema}'
            
            # Add SSL configuration if specified
            if self.config.ssl_mode:
                if self.config.ssl_mode == 'require':
                    connection_params['sslmode'] = 'require'
                elif self.config.ssl_mode == 'prefer':
                    connection_params['sslmode'] = 'prefer'
                elif self.config.ssl_mode == 'disable':
                    connection_params['sslmode'] = 'disable'
                elif self.config.ssl_mode == 'verify-ca':
                    connection_params['sslmode'] = 'verify-ca'
                elif self.config.ssl_mode == 'verify-full':
                    connection_params['sslmode'] = 'verify-full'
            
            # Create connection
            connection = psycopg2.connect(**connection_params)
            
            # Set session variables
            with connection.cursor() as cursor:
                cursor.execute("SET timezone = 'UTC'")
                cursor.execute("SET client_encoding = 'UTF8'")
                if self.config.schema:
                    cursor.execute(f"SET search_path TO {self.config.schema}")
                connection.commit()
            
            return connection
            
        except PostgreSQLError as e:
            raise ConnectionError(f"Failed to create PostgreSQL connection: {str(e)}")
        except Exception as e:
            raise ConnectionError(f"Unexpected error creating PostgreSQL connection: {str(e)}")
    
    def _test_connection(self, connection: Any) -> bool:
        """
        Test if PostgreSQL connection is still valid.
        
        Args:
            connection: PostgreSQL connection object
            
        Returns:
            True if connection is valid, False otherwise
        """
        try:
            if connection and not connection.closed:
                # Execute a simple query to test connection
                cursor = connection.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                return True
            return False
        except Exception:
            return False
    
    def _close_connection(self, connection: Any) -> None:
        """
        Close PostgreSQL connection.
        
        Args:
            connection: PostgreSQL connection object
        """
        try:
            if connection and not connection.closed:
                connection.close()
        except Exception as e:
            self.logger.warning(f"Error closing PostgreSQL connection: {str(e)}")
    
    def _execute_query(self, connection: Any, query: str, params: Optional[Dict] = None) -> Any:
        """
        Execute a query on PostgreSQL connection.
        
        Args:
            connection: PostgreSQL connection object
            query: SQL query string
            params: Query parameters
            
        Returns:
            Query result object
            
        Raises:
            QueryError: If query execution fails
        """
        try:
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Handle different query types
            if query.strip().upper().startswith(('SELECT', 'SHOW', 'DESCRIBE', 'EXPLAIN', 'WITH')):
                result = cursor.fetchall()
                cursor.close()
                # Convert RealDictRow objects to regular dictionaries
                return [dict(row) for row in result]
            else:
                # INSERT, UPDATE, DELETE, etc.
                connection.commit()
                affected_rows = cursor.rowcount
                cursor.close()
                return {"affected_rows": affected_rows}
                
        except PostgreSQLError as e:
            connection.rollback()
            raise QueryError(f"PostgreSQL query execution failed: {str(e)}")
        except Exception as e:
            connection.rollback()
            raise QueryError(f"Unexpected error executing PostgreSQL query: {str(e)}")
    
    def execute_transaction(self, queries: List[Tuple[str, Optional[Dict]]]) -> List[Any]:
        """
        Execute multiple queries in a single transaction.
        
        Args:
            queries: List of (query, params) tuples
            
        Returns:
            List of query results
            
        Raises:
            QueryError: If any query execution fails
        """
        with self.get_connection_context() as conn:
            try:
                results = []
                
                for query, params in queries:
                    result = self._execute_query(conn, query, params)
                    results.append(result)
                
                conn.commit()
                return results
                
            except Exception as e:
                conn.rollback()
                raise QueryError(f"Transaction failed: {str(e)}")
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get detailed information about a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table information
        """
        query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            udt_name,
            col_description(pgc.oid, c.ordinal_position) as column_comment
        FROM information_schema.columns c
        LEFT JOIN pg_class pgc ON pgc.relname = c.table_name
        WHERE c.table_schema = %(schema)s AND c.table_name = %(table_name)s
        ORDER BY c.ordinal_position
        """
        
        schema = self.config.schema or 'public'
        result = self.execute_query(query, {
            'schema': schema,
            'table_name': table_name
        })
        
        return {
            'table_name': table_name,
            'schema': schema,
            'columns': result
        }
    
    def get_database_tables(self) -> List[str]:
        """
        Get list of all tables in the database.
        
        Returns:
            List of table names
        """
        query = """
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = %(schema)s
        ORDER BY tablename
        """
        
        schema = self.config.schema or 'public'
        result = self.execute_query(query, {'schema': schema})
        return [row['tablename'] for row in result]
    
    def get_table_count(self, table_name: str) -> int:
        """
        Get row count for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Number of rows in the table
        """
        schema = self.config.schema or 'public'
        query = f'SELECT COUNT(*) as count FROM "{schema}"."{table_name}"'
        result = self.execute_query(query)
        return result[0]['count'] if result else 0
    
    def get_schemas(self) -> List[str]:
        """
        Get list of all schemas in the database.
        
        Returns:
            List of schema names
        """
        query = """
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY schema_name
        """
        
        result = self.execute_query(query)
        return [row['schema_name'] for row in result]
    
    def get_database_version(self) -> str:
        """
        Get PostgreSQL database version.
        
        Returns:
            Database version string
        """
        result = self.execute_query("SELECT version() as version")
        return result[0]['version'] if result else "Unknown"
    
    def execute_copy(self, table_name: str, data: List[Tuple], columns: Optional[List[str]] = None) -> int:
        """
        Execute COPY command for bulk data insertion.
        
        Args:
            table_name: Name of the table to insert into
            data: List of data tuples
            columns: Optional list of column names
            
        Returns:
            Number of rows copied
            
        Raises:
            QueryError: If COPY operation fails
        """
        with self.get_connection_context() as conn:
            try:
                cursor = conn.cursor()
                
                schema = self.config.schema or 'public'
                full_table_name = f'"{schema}"."{table_name}"'
                
                if columns:
                    column_str = f"({', '.join([f'"{col}"' for col in columns])})"
                    copy_query = f"COPY {full_table_name} {column_str} FROM STDIN"
                else:
                    copy_query = f"COPY {full_table_name} FROM STDIN"
                
                # Use copy_from for bulk insert
                import io
                data_io = io.StringIO()
                for row in data:
                    data_io.write('\t'.join([str(val) if val is not None else '\\N' for val in row]) + '\n')
                data_io.seek(0)
                
                cursor.copy_from(data_io, f'{schema}.{table_name}', columns=columns, sep='\t', null='\\N')
                conn.commit()
                
                affected_rows = cursor.rowcount
                cursor.close()
                return affected_rows
                
            except PostgreSQLError as e:
                conn.rollback()
                raise QueryError(f"COPY operation failed: {str(e)}")
            except Exception as e:
                conn.rollback()
                raise QueryError(f"Unexpected error in COPY operation: {str(e)}")
    
    def __del__(self):
        """Cleanup when connector is destroyed"""
        try:
            self.disconnect()
        except:
            pass
