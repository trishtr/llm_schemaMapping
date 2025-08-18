"""
Configuration Loader for Database Connectors

This module provides utilities to load database configurations from JSON files
and create appropriate database connectors.
"""

import json
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path

from .base_connector import ConnectionConfig, ConnectionError
from . import create_connector


class ConfigLoader:
    """
    Utility class for loading database configurations and creating connectors.
    """
    
    def __init__(self, config_file_path: str = None):
        """
        Initialize the configuration loader.
        
        Args:
            config_file_path: Path to the configuration JSON file
        """
        self.config_file_path = config_file_path or "config/database_configs.json"
        self.logger = logging.getLogger(self.__class__.__name__)
        self._config_data = None
        
    def load_config(self) -> Dict[str, Any]:
        """
        Load the configuration file.
        
        Returns:
            Configuration data dictionary
            
        Raises:
            FileNotFoundError: If configuration file doesn't exist
            json.JSONDecodeError: If configuration file is invalid JSON
        """
        if self._config_data is None:
            config_path = Path(self.config_file_path)
            
            if not config_path.exists():
                raise FileNotFoundError(f"Configuration file not found: {self.config_file_path}")
            
            try:
                with open(config_path, 'r') as f:
                    self._config_data = json.load(f)
                self.logger.info(f"Configuration loaded from {self.config_file_path}")
            except json.JSONDecodeError as e:
                raise json.JSONDecodeError(f"Invalid JSON in configuration file: {str(e)}", e.doc, e.pos)
        
        return self._config_data
    
    def get_client_config(self, client_key: str) -> Dict[str, Any]:
        """
        Get configuration for a specific client.
        
        Args:
            client_key: Client identifier (e.g., 'client_alpha', 'client_beta')
            
        Returns:
            Client configuration dictionary
            
        Raises:
            KeyError: If client configuration not found
        """
        config = self.load_config()
        sources = config.get('database_config', {}).get('sources', {})
        
        if client_key not in sources:
            available_clients = list(sources.keys())
            raise KeyError(f"Client '{client_key}' not found. Available clients: {available_clients}")
        
        return sources[client_key]
    
    def create_connector_from_config(self, client_key: str):
        """
        Create a database connector from client configuration.
        
        Args:
            client_key: Client identifier
            
        Returns:
            Database connector instance
            
        Raises:
            KeyError: If client configuration not found
            ValueError: If database type is unsupported
        """
        client_config = self.get_client_config(client_key)
        
        # Extract connection details
        connection_details = client_config.get('connection_details', {})
        pool_settings = client_config.get('pool_settings', {})
        connection_defaults = self._get_connection_defaults()
        
        # Create ConnectionConfig object
        config = ConnectionConfig(
            host=connection_details['host'],
            port=connection_details['port'],
            username=connection_details['username'],
            password=connection_details['password'],
            database=connection_details['database'],
            ssl_mode=connection_details.get('ssl_mode'),
            charset=connection_details.get('charset'),
            schema=connection_details.get('schema'),
            encrypt=connection_details.get('encrypt'),
            trust_server_certificate=connection_details.get('trust_server_certificate'),
            max_connections=pool_settings.get('max_connections', 10),
            idle_timeout=pool_settings.get('idle_timeout', 300),
            connection_timeout=pool_settings.get('connection_timeout', 30),
            retry_attempts=connection_defaults.get('retry_attempts', 3),
            retry_delay=connection_defaults.get('retry_delay', 5),
            query_timeout=connection_defaults.get('query_timeout', 300)
        )
        
        # Get database type and create connector
        db_type = client_config['db_type']
        return create_connector(db_type, config)
    
    def create_connector_and_connect(self, client_key: str):
        """
        Create a database connector and automatically connect to the database.
        
        Args:
            client_key: Client identifier
            
        Returns:
            Connected database connector instance
            
        Raises:
            KeyError: If client configuration not found
            ValueError: If database type is unsupported
            ConnectionError: If connection fails
        """
        connector = self.create_connector_from_config(client_key)
        
        # Attempt to connect
        if connector.connect():
            return connector
        else:
            raise ConnectionError(f"Failed to connect to database for client: {client_key}")
    
    def get_all_connectors(self):
        """
        Get connectors for all available clients.
        
        Returns:
            Dictionary mapping client keys to connector instances
        """
        connectors = {}
        client_keys = self.list_available_clients()
        
        for client_key in client_keys:
            try:
                connector = self.create_connector_from_config(client_key)
                connectors[client_key] = connector
            except Exception as e:
                self.logger.warning(f"Failed to create connector for {client_key}: {e}")
                continue
        
        return connectors
    
    def get_connector_by_database_type(self, db_type: str):
        """
        Get the first available connector for a specific database type.
        
        Args:
            db_type: Database type ('mysql', 'postgresql', 'mssql')
            
        Returns:
            Database connector instance or None if not found
        """
        client_keys = self.list_available_clients()
        
        for client_key in client_keys:
            try:
                client_config = self.get_client_config(client_key)
                if client_config.get('db_type', '').lower() == db_type.lower():
                    return self.create_connector_from_config(client_key)
            except Exception as e:
                self.logger.warning(f"Failed to get connector for {client_key}: {e}")
                continue
        
        return None
    
    def _get_connection_defaults(self) -> Dict[str, Any]:
        """
        Get connection default settings.
        
        Returns:
            Default connection settings
        """
        config = self.load_config()
        return config.get('database_config', {}).get('sources', {}).get('connection_defaults', {})
    
    def list_available_clients(self) -> list:
        """
        Get list of available client configurations.
        
        Returns:
            List of client keys
        """
        config = self.load_config()
        sources = config.get('database_config', {}).get('sources', {})
        
        # Filter out non-client entries
        client_keys = [key for key in sources.keys() 
                      if not key.startswith('connection_defaults') and 
                         not key.startswith('security') and
                         key != 'target_warehouse']  # Include target_warehouse as a valid client
        
        return client_keys
    
    def get_client_info(self, client_key: str) -> Dict[str, Any]:
        """
        Get information about a client configuration.
        
        Args:
            client_key: Client identifier
            
        Returns:
            Client information dictionary
        """
        client_config = self.get_client_config(client_key)
        
        return {
            'client_name': client_config.get('client_name', client_config.get('name', client_key)),
            'db_type': client_config.get('db_type'),
            'host': client_config.get('connection_details', {}).get('host'),
            'port': client_config.get('connection_details', {}).get('port'),
            'database': client_config.get('connection_details', {}).get('database'),
            'schema': client_config.get('connection_details', {}).get('schema'),
            'max_connections': client_config.get('pool_settings', {}).get('max_connections')
        }
    
    def validate_config(self) -> bool:
        """
        Validate the configuration file.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        try:
            config = self.load_config()
            
            # Check required top-level keys
            if 'database_config' not in config:
                self.logger.error("Missing 'database_config' key in configuration")
                return False
            
            if 'sources' not in config['database_config']:
                self.logger.error("Missing 'sources' key in database_config")
                return False
            
            sources = config['database_config']['sources']
            
            # Validate each client configuration
            for client_key, client_config in sources.items():
                if client_key in ['connection_defaults', 'security']:
                    continue
                
                if not self._validate_client_config(client_key, client_config):
                    return False
            
            self.logger.info("Configuration validation successful")
            return True
            
        except Exception as e:
            self.logger.error(f"Configuration validation failed: {str(e)}")
            return False
    
    def _validate_client_config(self, client_key: str, client_config: Dict[str, Any]) -> bool:
        """
        Validate a specific client configuration.
        
        Args:
            client_key: Client identifier
            client_config: Client configuration dictionary
            
        Returns:
            True if valid, False otherwise
        """
        required_keys = ['db_type', 'connection_details']
        connection_required_keys = ['host', 'port', 'username', 'password', 'database']
        
        # Check required keys
        for key in required_keys:
            if key not in client_config:
                self.logger.error(f"Client '{client_key}' missing required key: {key}")
                return False
        
        # Check connection details
        connection_details = client_config.get('connection_details', {})
        for key in connection_required_keys:
            if key not in connection_details:
                self.logger.error(f"Client '{client_key}' missing required connection detail: {key}")
                return False
        
        # Validate database type
        db_type = client_config['db_type']
        supported_types = ['mysql', 'postgresql', 'mssql']
        if db_type not in supported_types:
            self.logger.error(f"Client '{client_key}' has unsupported database type: {db_type}")
            return False
        
        # Validate port number
        try:
            port = int(connection_details['port'])
            if port < 1 or port > 65535:
                self.logger.error(f"Client '{client_key}' has invalid port number: {port}")
                return False
        except (ValueError, TypeError):
            self.logger.error(f"Client '{client_key}' has invalid port: {connection_details['port']}")
            return False
        
        return True
    
    def get_clients_by_database_type(self, db_type: str) -> List[str]:
        """
        Get all client keys that use a specific database type.
        
        Args:
            db_type: Database type ('mysql', 'postgresql', 'mssql')
            
        Returns:
            List of client keys
        """
        matching_clients = []
        client_keys = self.list_available_clients()
        
        for client_key in client_keys:
            try:
                client_config = self.get_client_config(client_key)
                if client_config.get('db_type', '').lower() == db_type.lower():
                    matching_clients.append(client_key)
            except Exception as e:
                self.logger.warning(f"Failed to check client {client_key}: {e}")
                continue
        
        return matching_clients
    
    def test_all_connections(self) -> Dict[str, bool]:
        """
        Test connections for all configured clients.
        
        Returns:
            Dictionary mapping client keys to connection test results
        """
        results = {}
        client_keys = self.list_available_clients()
        
        for client_key in client_keys:
            try:
                connector = self.create_connector_from_config(client_key)
                results[client_key] = connector.health_check()
            except Exception as e:
                self.logger.warning(f"Failed to test connection for {client_key}: {e}")
                results[client_key] = False
        
        return results
