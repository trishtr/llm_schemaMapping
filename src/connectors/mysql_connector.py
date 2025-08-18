"""
MySQL Database Connector

This module provides a MySQL-specific implementation of the base database connector
using mysql-connector-python library.
"""

import logging
from typing import Any, Dict, Optional, List, Tuple
import mysql.connector
from mysql.connector import Error as MySQLError
from mysql.connector.pooling import MySQLConnectionPool
from mysql.connector.cursor import MySQLCursor

from .base_connector import (
    BaseConnector, 
    ConnectionConfig, 
    ConnectionError, 
    QueryError,
    DatabaseError
)


class MySQLConnector(BaseConnector):
    """
    MySQL database connector implementation.
    
    Features:
    - Connection pooling
    - SSL support
    - Character set configuration
    - Transaction support
    - Prepared statements
    """
    
    def __init__(self, config: ConnectionConfig):
        """
        Initialize MySQL connector.
        
        Args:
            config: Connection configuration object
        """
        super().__init__(config)
        self._pool = None
        self._pool_config = None
        
    def _create_connection(self) -> Any:
        """
        Create a new MySQL connection.
        
        Returns:
            MySQL connection object
            
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
                'charset': self.config.charset or 'utf8mb4',
                'autocommit': False,
                'get_warnings': True,
                'raise_on_warnings': True,
                'connection_timeout': self.config.connection_timeout,
                'pool_reset_session': True
            }
            
            # Add SSL configuration if specified
            if self.config.ssl_mode:
                if self.config.ssl_mode == 'require':
                    connection_params['ssl_disabled'] = False
                    connection_params['ssl_verify_cert'] = True
                elif self.config.ssl_mode == 'preferred':
                    connection_params['ssl_disabled'] = False
                    connection_params['ssl_verify_cert'] = False
                elif self.config.ssl_mode == 'disable':
                    connection_params['ssl_disabled'] = True
            
            # Create connection
            connection = mysql.connector.connect(**connection_params)
            
            # Set session variables
            with connection.cursor() as cursor:
                cursor.execute("SET SESSION sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO'")
                cursor.execute("SET SESSION time_zone = '+00:00'")
            
            return connection
            
        except MySQLError as e:
            raise ConnectionError(f"Failed to create MySQL connection: {str(e)}")
        except Exception as e:
            raise ConnectionError(f"Unexpected error creating MySQL connection: {str(e)}")
    
    def _test_connection(self, connection: Any) -> bool:
        """
        Test if MySQL connection is still valid.
        
        Args:
            connection: MySQL connection object
            
        Returns:
            True if connection is valid, False otherwise
        """
        try:
            if connection and connection.is_connected():
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
        Close MySQL connection.
        
        Args:
            connection: MySQL connection object
        """
        try:
            if connection and connection.is_connected():
                connection.close()
        except Exception as e:
            self.logger.warning(f"Error closing MySQL connection: {str(e)}")
    
    def _execute_query(self, connection: Any, query: str, params: Optional[Dict] = None) -> Any:
        """
        Execute a query on MySQL connection.
        
        Args:
            connection: MySQL connection object
            query: SQL query string
            params: Query parameters
            
        Returns:
            Query result object
            
        Raises:
            QueryError: If query execution fails
        """
        try:
            cursor = connection.cursor(dictionary=True, buffered=True)
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Handle different query types
            if query.strip().upper().startswith(('SELECT', 'SHOW', 'DESCRIBE', 'EXPLAIN')):
                result = cursor.fetchall()
                cursor.close()
                return result
            else:
                # INSERT, UPDATE, DELETE, etc.
                connection.commit()
                affected_rows = cursor.rowcount
                lastrowid = cursor.lastrowid
                cursor.close()
                return {"affected_rows": affected_rows, "lastrowid": lastrowid}
                
        except MySQLError as e:
            connection.rollback()
            raise QueryError(f"MySQL query execution failed: {str(e)}")
        except Exception as e:
            connection.rollback()
            raise QueryError(f"Unexpected error executing MySQL query: {str(e)}")
    
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
                conn.start_transaction()
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
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            COLUMN_KEY,
            EXTRA,
            COLUMN_COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %(database)s AND TABLE_NAME = %(table_name)s
        ORDER BY ORDINAL_POSITION
        """
        
        result = self.execute_query(query, {
            'database': self.config.database,
            'table_name': table_name
        })
        
        return {
            'table_name': table_name,
            'columns': result
        }
    
    def get_database_tables(self) -> List[str]:
        """
        Get list of all tables in the database.
        
        Returns:
            List of table names
        """
        query = """
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = %(database)s AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        
        result = self.execute_query(query, {'database': self.config.database})
        return [row['TABLE_NAME'] for row in result]
    
    def get_table_count(self, table_name: str) -> int:
        """
        Get row count for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Number of rows in the table
        """
        query = f"SELECT COUNT(*) as count FROM `{table_name}`"
        result = self.execute_query(query)
        return result[0]['count'] if result else 0
    
    def get_database_version(self) -> str:
        """
        Get MySQL database version.
        
        Returns:
            Database version string
        """
        result = self.execute_query("SELECT VERSION() as version")
        return result[0]['version'] if result else "Unknown"
    
    def __del__(self):
        """Cleanup when connector is destroyed"""
        try:
            self.disconnect()
        except:
            pass
