"""
Microsoft SQL Server Database Connector

This module provides a MSSQL-specific implementation of the base database connector
using pyodbc library.
"""

import logging
from typing import Any, Dict, Optional, List, Tuple
import pyodbc
from pyodbc import Error as ODBCError
import urllib.parse

from .base_connector import (
    BaseConnector, 
    ConnectionConfig, 
    ConnectionError, 
    QueryError,
    DatabaseError
)


class MSSQLConnector(BaseConnector):
    """
    Microsoft SQL Server database connector implementation.
    
    Features:
    - Connection pooling
    - SSL/TLS encryption
    - Windows Authentication support
    - Multiple driver support
    - Transaction support
    - Stored procedure support
    """
    
    def __init__(self, config: ConnectionConfig):
        """
        Initialize MSSQL connector.
        
        Args:
            config: Connection configuration object
        """
        super().__init__(config)
        self._pool = None
        self._pool_config = None
        self._driver = self._detect_driver()
        
    def _detect_driver(self) -> str:
        """
        Detect available ODBC drivers for SQL Server.
        
        Returns:
            Available driver name
        """
        available_drivers = pyodbc.drivers()
        
        # Preferred drivers in order of preference
        preferred_drivers = [
            'ODBC Driver 18 for SQL Server',
            'ODBC Driver 17 for SQL Server',
            'ODBC Driver 13 for SQL Server',
            'SQL Server Native Client 11.0',
            'SQL Server',
            'FreeTDS'
        ]
        
        for driver in preferred_drivers:
            if driver in available_drivers:
                return driver
        
        # If no preferred drivers found, use the first available
        if available_drivers:
            return available_drivers[0]
        
        raise ConnectionError("No SQL Server ODBC drivers found on the system")
    
    def _create_connection(self) -> Any:
        """
        Create a new MSSQL connection.
        
        Returns:
            MSSQL connection object
            
        Raises:
            ConnectionError: If connection creation fails
        """
        try:
            # Build connection string
            connection_string = self._build_connection_string()
            
            # Create connection
            connection = pyodbc.connect(connection_string, autocommit=False)
            
            # Set connection properties
            connection.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
            connection.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
            connection.setencoding(encoding='utf-8')
            
            return connection
            
        except ODBCError as e:
            raise ConnectionError(f"Failed to create MSSQL connection: {str(e)}")
        except Exception as e:
            raise ConnectionError(f"Unexpected error creating MSSQL connection: {str(e)}")
    
    def _build_connection_string(self) -> str:
        """
        Build ODBC connection string for SQL Server.
        
        Returns:
            ODBC connection string
        """
        # Base connection parameters
        params = {
            'DRIVER': f'{{{self._driver}}}',
            'SERVER': f'{self.config.host},{self.config.port}',
            'DATABASE': self.config.database,
            'UID': self.config.username,
            'PWD': self.config.password,
            'Connection Timeout': self.config.connection_timeout,
            'Command Timeout': self.config.query_timeout,
            'MultipleActiveResultSets': 'True'
        }
        
        # Add encryption settings
        if self.config.encrypt is not None:
            if self.config.encrypt:
                params['Encrypt'] = 'yes'
                if self.config.trust_server_certificate:
                    params['TrustServerCertificate'] = 'yes'
            else:
                params['Encrypt'] = 'no'
        
        # Add charset if specified
        if self.config.charset:
            params['CharSet'] = self.config.charset
        
        # Build connection string
        connection_string = ';'.join([f'{k}={v}' for k, v in params.items()])
        return connection_string
    
    def _test_connection(self, connection: Any) -> bool:
        """
        Test if MSSQL connection is still valid.
        
        Args:
            connection: MSSQL connection object
            
        Returns:
            True if connection is valid, False otherwise
        """
        try:
            if connection:
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
        Close MSSQL connection.
        
        Args:
            connection: MSSQL connection object
        """
        try:
            if connection:
                connection.close()
        except Exception as e:
            self.logger.warning(f"Error closing MSSQL connection: {str(e)}")
    
    def _execute_query(self, connection: Any, query: str, params: Optional[Dict] = None) -> Any:
        """
        Execute a query on MSSQL connection.
        
        Args:
            connection: MSSQL connection object
            query: SQL query string
            params: Query parameters
            
        Returns:
            Query result object
            
        Raises:
            QueryError: If query execution fails
        """
        try:
            cursor = connection.cursor()
            
            if params:
                # Convert parameters to list for pyodbc
                param_list = list(params.values())
                cursor.execute(query, param_list)
            else:
                cursor.execute(query)
            
            # Handle different query types
            if query.strip().upper().startswith(('SELECT', 'WITH')):
                # Get column names
                columns = [column[0] for column in cursor.description] if cursor.description else []
                
                # Fetch all results
                rows = cursor.fetchall()
                cursor.close()
                
                # Convert to list of dictionaries
                result = []
                for row in rows:
                    result.append(dict(zip(columns, row)))
                
                return result
            else:
                # INSERT, UPDATE, DELETE, etc.
                connection.commit()
                affected_rows = cursor.rowcount
                cursor.close()
                return {"affected_rows": affected_rows}
                
        except ODBCError as e:
            connection.rollback()
            raise QueryError(f"MSSQL query execution failed: {str(e)}")
        except Exception as e:
            connection.rollback()
            raise QueryError(f"Unexpected error executing MSSQL query: {str(e)}")
    
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
    
    def execute_stored_procedure(self, procedure_name: str, params: Optional[Dict] = None) -> Any:
        """
        Execute a stored procedure.
        
        Args:
            procedure_name: Name of the stored procedure
            params: Parameters for the stored procedure
            
        Returns:
            Stored procedure result
            
        Raises:
            QueryError: If stored procedure execution fails
        """
        with self.get_connection_context() as conn:
            try:
                cursor = conn.cursor()
                
                if params:
                    # Build parameter placeholders
                    param_placeholders = ','.join(['?' for _ in params])
                    call_query = f"EXEC {procedure_name} {param_placeholders}"
                    param_list = list(params.values())
                    cursor.execute(call_query, param_list)
                else:
                    call_query = f"EXEC {procedure_name}"
                    cursor.execute(call_query)
                
                # Get results
                if cursor.description:
                    columns = [column[0] for column in cursor.description]
                    rows = cursor.fetchall()
                    result = [dict(zip(columns, row)) for row in rows]
                else:
                    result = {"affected_rows": cursor.rowcount}
                
                conn.commit()
                cursor.close()
                return result
                
            except ODBCError as e:
                conn.rollback()
                raise QueryError(f"Stored procedure execution failed: {str(e)}")
            except Exception as e:
                conn.rollback()
                raise QueryError(f"Unexpected error executing stored procedure: {str(e)}")
    
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
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            COLUMNPROPERTY(object_id(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') as IS_IDENTITY,
            ep.value as COLUMN_COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN sys.extended_properties ep ON ep.major_id = object_id(c.TABLE_SCHEMA + '.' + c.TABLE_NAME)
            AND ep.minor_id = c.ORDINAL_POSITION
            AND ep.name = 'MS_Description'
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
        """
        
        schema = self.config.schema or 'dbo'
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
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        
        schema = self.config.schema or 'dbo'
        result = self.execute_query(query, {'schema': schema})
        return [row['TABLE_NAME'] for row in result]
    
    def get_table_count(self, table_name: str) -> int:
        """
        Get row count for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Number of rows in the table
        """
        schema = self.config.schema or 'dbo'
        query = f"SELECT COUNT(*) as count FROM [{schema}].[{table_name}]"
        result = self.execute_query(query)
        return result[0]['count'] if result else 0
    
    def get_schemas(self) -> List[str]:
        """
        Get list of all schemas in the database.
        
        Returns:
            List of schema names
        """
        query = """
        SELECT SCHEMA_NAME 
        FROM INFORMATION_SCHEMA.SCHEMATA 
        WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA', 'sys', 'guest')
        ORDER BY SCHEMA_NAME
        """
        
        result = self.execute_query(query)
        return [row['SCHEMA_NAME'] for row in result]
    
    def get_database_version(self) -> str:
        """
        Get MSSQL database version.
        
        Returns:
            Database version string
        """
        result = self.execute_query("SELECT @@VERSION as version")
        return result[0]['version'] if result else "Unknown"
    
    def get_database_size(self) -> Dict[str, Any]:
        """
        Get database size information.
        
        Returns:
            Dictionary with database size details
        """
        query = """
        SELECT 
            DB_NAME() as database_name,
            SUM(size * 8.0 / 1024) as size_mb,
            SUM(size * 8.0 / 1024 / 1024) as size_gb
        FROM sys.database_files
        """
        
        result = self.execute_query(query)
        return result[0] if result else {}
    
    def get_table_size(self, table_name: str) -> Dict[str, Any]:
        """
        Get table size information.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table size details
        """
        query = """
        SELECT 
            t.name as table_name,
            s.name as schema_name,
            p.rows as row_count,
            SUM(a.total_pages) * 8.0 / 1024 as size_mb
        FROM sys.tables t
        INNER JOIN sys.indexes i ON t.object_id = i.object_id
        INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
        INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = ?
        GROUP BY t.name, s.name, p.rows
        """
        
        result = self.execute_query(query, {'table_name': table_name})
        return result[0] if result else {}
    
    def get_connection_string(self) -> str:
        """
        Get the current connection string.
        
        Returns:
            ODBC connection string
        """
        return self._build_connection_string()
    
    def __del__(self):
        """Cleanup when connector is destroyed"""
        try:
            self.disconnect()
        except:
            pass
