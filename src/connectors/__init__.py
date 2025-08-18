"""
Database Connectors Package

This package provides database connectors for different database systems:
- MySQL
- PostgreSQL
- Microsoft SQL Server

Each connector extends the base connector with database-specific functionality.
"""

from .base_connector import (
    BaseConnector,
    ConnectionConfig,
    ConnectionStatus,
    DatabaseError,
    ConnectionError,
    QueryError
)

from .mysql_connector import MySQLConnector
from .psg_connector import PostgreSQLConnector
from .mssql_connector import MSSQLConnector

__all__ = [
    'BaseConnector',
    'ConnectionConfig',
    'ConnectionStatus',
    'DatabaseError',
    'ConnectionError',
    'QueryError',
    'MySQLConnector',
    'PostgreSQLConnector',
    'MSSQLConnector',
    'create_connector'
]


def create_connector(db_type: str, config: ConnectionConfig) -> BaseConnector:
    """
    Factory function to create the appropriate database connector.
    
    Args:
        db_type: Database type ('mysql', 'postgresql', 'mssql')
        config: Connection configuration object
        
    Returns:
        Appropriate database connector instance
        
    Raises:
        ValueError: If unsupported database type is specified
    """
    db_type_lower = db_type.lower()
    
    if db_type_lower in ['mysql', 'mariadb']:
        return MySQLConnector(config)
    elif db_type_lower in ['postgresql', 'postgres', 'psg']:
        return PostgreSQLConnector(config)
    elif db_type_lower in ['mssql', 'sqlserver', 'sql_server']:
        return MSSQLConnector(config)
    else:
        raise ValueError(f"Unsupported database type: {db_type}. "
                        f"Supported types: mysql, postgresql, mssql")
