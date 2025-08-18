"""
Base Database Connector

This module provides a base class for database connectors with common functionality
including connection management, query execution, and error handling.
"""

import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, Tuple
from contextlib import contextmanager
import threading
from dataclasses import dataclass
from enum import Enum


class ConnectionStatus(Enum):
    """Connection status enumeration"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    ERROR = "error"
    CLOSING = "closing"


@dataclass
class ConnectionConfig:
    """Configuration for database connections"""
    host: str
    port: int
    username: str
    password: str
    database: str
    ssl_mode: Optional[str] = None
    charset: Optional[str] = None
    schema: Optional[str] = None
    encrypt: Optional[bool] = None
    trust_server_certificate: Optional[bool] = None
    max_connections: int = 10
    idle_timeout: int = 300
    connection_timeout: int = 30
    retry_attempts: int = 3
    retry_delay: int = 5
    query_timeout: int = 300


class DatabaseError(Exception):
    """Base exception for database operations"""
    pass


class ConnectionError(DatabaseError):
    """Exception raised for connection-related errors"""
    pass


class QueryError(DatabaseError):
    """Exception raised for query execution errors"""
    pass


class BaseConnector(ABC):
    """
    Abstract base class for database connectors.
    
    Provides common functionality for:
    - Connection management
    - Query execution
    - Connection pooling
    - Error handling
    - Health monitoring
    """
    
    def __init__(self, config: ConnectionConfig):
        """
        Initialize the base connector.
        
        Args:
            config: Connection configuration object
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self._status = ConnectionStatus.DISCONNECTED
        self._connection_lock = threading.Lock()
        self._last_health_check = 0
        self._health_check_interval = 60
        
        # Connection pool attributes
        self._pool = []
        self._pool_lock = threading.Lock()
        self._active_connections = 0
        
    @property
    def status(self) -> ConnectionStatus:
        """Get current connection status"""
        return self._status
    
    @property
    def is_connected(self) -> bool:
        """Check if connector is connected"""
        return self._status == ConnectionStatus.CONNECTED
    
    @abstractmethod
    def _create_connection(self) -> Any:
        """
        Create a new database connection.
        
        Returns:
            Database connection object
            
        Raises:
            ConnectionError: If connection creation fails
        """
        pass
    
    @abstractmethod
    def _test_connection(self, connection: Any) -> bool:
        """
        Test if a connection is still valid.
        
        Args:
            connection: Database connection object
            
        Returns:
            True if connection is valid, False otherwise
        """
        pass
    
    @abstractmethod
    def _close_connection(self, connection: Any) -> None:
        """
        Close a database connection.
        
        Args:
            connection: Database connection object
        """
        pass
    
    @abstractmethod
    def _execute_query(self, connection: Any, query: str, params: Optional[Dict] = None) -> Any:
        """
        Execute a query on the given connection.
        
        Args:
            connection: Database connection object
            query: SQL query string
            params: Query parameters
            
        Returns:
            Query result object
            
        Raises:
            QueryError: If query execution fails
        """
        pass
    
    def connect(self) -> bool:
        """
        Establish database connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        with self._connection_lock:
            if self._status == ConnectionStatus.CONNECTED:
                return True
                
            try:
                self._status = ConnectionStatus.CONNECTING
                self.logger.info(f"Connecting to database {self.config.database} on {self.config.host}:{self.config.port}")
                
                # Test connection
                test_conn = self._create_connection()
                if self._test_connection(test_conn):
                    self._close_connection(test_conn)
                    self._status = ConnectionStatus.CONNECTED
                    self.logger.info("Database connection established successfully")
                    return True
                else:
                    raise ConnectionError("Connection test failed")
                    
            except Exception as e:
                self._status = ConnectionStatus.ERROR
                self.logger.error(f"Failed to connect to database: {str(e)}")
                return False
    
    def disconnect(self) -> None:
        """Close all database connections and clean up resources."""
        with self._connection_lock:
            if self._status == ConnectionStatus.DISCONNECTED:
                return
                
            self._status = ConnectionStatus.CLOSING
            self.logger.info("Disconnecting from database...")
            
            # Close all connections in pool
            with self._pool_lock:
                for conn in self._pool:
                    try:
                        self._close_connection(conn)
                    except Exception as e:
                        self.logger.warning(f"Error closing connection: {str(e)}")
                
                self._pool.clear()
                self._active_connections = 0
            
            self._status = ConnectionStatus.DISCONNECTED
            self.logger.info("Database disconnected")
    
    def get_connection(self) -> Any:
        """
        Get a connection from the pool or create a new one.
        
        Returns:
            Database connection object
            
        Raises:
            ConnectionError: If no connection can be obtained
        """
        if not self.is_connected:
            if not self.connect():
                raise ConnectionError("Cannot establish database connection")
        
        with self._pool_lock:
            # Try to get connection from pool
            if self._pool:
                conn = self._pool.pop()
                if self._test_connection(conn):
                    self._active_connections += 1
                    return conn
                else:
                    self._close_connection(conn)
            
            # Create new connection if pool is empty or all connections are invalid
            try:
                conn = self._create_connection()
                if self._test_connection(conn):
                    self._active_connections += 1
                    return conn
                else:
                    raise ConnectionError("Failed to create valid connection")
            except Exception as e:
                raise ConnectionError(f"Failed to create connection: {str(e)}")
    
    def return_connection(self, connection: Any) -> None:
        """
        Return a connection to the pool.
        
        Args:
            connection: Database connection object
        """
        if not connection:
            return
            
        with self._pool_lock:
            if len(self._pool) < self.config.max_connections:
                try:
                    if self._test_connection(connection):
                        self._pool.append(connection)
                    else:
                        self._close_connection(connection)
                except Exception:
                    self._close_connection(connection)
            else:
                self._close_connection(connection)
            
            self._active_connections = max(0, self._active_connections - 1)
    
    @contextmanager
    def get_connection_context(self):
        """
        Context manager for database connections.
        
        Yields:
            Database connection object
        """
        connection = None
        try:
            connection = self.get_connection()
            yield connection
        finally:
            if connection:
                self.return_connection(connection)
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> Any:
        """
        Execute a query using a connection from the pool.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            Query result object
            
        Raises:
            QueryError: If query execution fails
        """
        with self.get_connection_context() as conn:
            try:
                return self._execute_query(conn, query, params)
            except Exception as e:
                raise QueryError(f"Query execution failed: {str(e)}")
    
    def execute_many(self, query: str, params_list: List[Dict]) -> List[Any]:
        """
        Execute a query multiple times with different parameters.
        
        Args:
            query: SQL query string
            params_list: List of parameter dictionaries
            
        Returns:
            List of query results
            
        Raises:
            QueryError: If query execution fails
        """
        results = []
        with self.get_connection_context() as conn:
            try:
                for params in params_list:
                    result = self._execute_query(conn, query, params)
                    results.append(result)
                return results
            except Exception as e:
                raise QueryError(f"Batch query execution failed: {str(e)}")
    
    def health_check(self) -> bool:
        """
        Perform a health check on the database connection.
        
        Returns:
            True if healthy, False otherwise
        """
        current_time = time.time()
        if current_time - self._last_health_check < self._health_check_interval:
            return self._status == ConnectionStatus.CONNECTED
        
        try:
            with self.get_connection_context() as conn:
                if self._test_connection(conn):
                    self._last_health_check = current_time
                    return True
                else:
                    self._status = ConnectionStatus.ERROR
                    return False
        except Exception as e:
            self.logger.error(f"Health check failed: {str(e)}")
            self._status = ConnectionStatus.ERROR
            return False
    
    def get_pool_status(self) -> Dict[str, Any]:
        """
        Get current connection pool status.
        
        Returns:
            Dictionary with pool statistics
        """
        with self._pool_lock:
            return {
                "total_connections": len(self._pool) + self._active_connections,
                "pool_size": len(self._pool),
                "active_connections": self._active_connections,
                "max_connections": self.config.max_connections,
                "status": self._status.value
            }
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
