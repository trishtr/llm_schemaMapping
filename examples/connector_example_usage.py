"""
Example Usage of Database Connectors with Configuration Loader

This example demonstrates how to:
1. Load database configurations from database_configs.json
2. Create appropriate connectors based on db_type
3. Test connections and perform basic operations
"""

import sys
import os
import logging

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from connectors.config_loader import ConfigLoader
from connectors import ConnectionError, QueryError


def setup_logging():
    """Setup basic logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def main():
    """Main function demonstrating the database connector usage."""
    print("Database Connector Configuration Example")
    print("=" * 50)
    
    setup_logging()
    
    try:
        # 1. Create config loader
        print("\n1. Loading configuration from database_configs.json...")
        config_loader = ConfigLoader()
        
        # 2. Validate configuration
        if config_loader.validate_config():
            print("✓ Configuration validation successful")
        else:
            print("✗ Configuration validation failed")
            return
        
        # 3. List available clients
        print("\n2. Available database clients:")
        clients = config_loader.list_available_clients()
        for client in clients:
            try:
                info = config_loader.get_client_info(client)
                print(f"  - {client}:")
                print(f"    Name: {info['client_name']}")
                print(f"    Type: {info['db_type']}")
                print(f"    Host: {info['host']}:{info['port']}")
                print(f"    Database: {info['database']}")
                if info.get('schema'):
                    print(f"    Schema: {info['schema']}")
                print()
            except Exception as e:
                print(f"  - {client}: Error getting info - {e}")
        
        # 4. Create connectors for each client
        print("3. Creating database connectors...")
        for client in clients:
            try:
                print(f"\n  Creating connector for {client}...")
                
                # Get client info for display
                info = config_loader.get_client_info(client)
                db_type = info['db_type']
                
                # Create connector based on db_type from config
                connector = config_loader.create_connector_from_config(client)
                print(f"  ✓ Created {connector.__class__.__name__} for {db_type} database")
                
                # Test connection (this will fail without actual databases)
                print(f"  Testing connection to {client}...")
                try:
                    if connector.connect():
                        print(f"  ✓ Successfully connected to {client}")
                        
                        # Get pool status
                        status = connector.get_pool_status()
                        print(f"  Pool Status: {status['status']}")
                        
                        # Test basic query based on database type
                        try:
                            if db_type == 'mysql':
                                result = connector.execute_query("SELECT VERSION() as version")
                                print(f"  MySQL Version: {result[0]['version']}")
                            elif db_type == 'postgresql':
                                result = connector.execute_query("SELECT version() as version")
                                print(f"  PostgreSQL Version: {result[0]['version']}")
                            elif db_type == 'mssql':
                                result = connector.execute_query("SELECT @@VERSION as version")
                                print(f"  MSSQL Version: {result[0]['version']}")
                        except Exception as e:
                            print(f"  Query test failed: {e}")
                        
                        # Disconnect
                        connector.disconnect()
                        print(f"  ✓ Disconnected from {client}")
                        
                    else:
                        print(f"  ✗ Failed to connect to {client}")
                        
                except ConnectionError as e:
                    print(f"  ✗ Connection error for {client}: {e}")
                except Exception as e:
                    print(f"  ✗ Unexpected error for {client}: {e}")
                    
            except Exception as e:
                print(f"  ✗ Failed to create connector for {client}: {e}")
        
        # 5. Demonstrate getting connectors by database type
        print("\n4. Getting connectors by database type...")
        db_types = ['mysql', 'postgresql', 'mssql']
        
        for db_type in db_types:
            connector = config_loader.get_connector_by_database_type(db_type)
            if connector:
                print(f"  ✓ Found {db_type} connector: {connector.__class__.__name__}")
                
                # List clients of this type
                clients_of_type = config_loader.get_clients_by_database_type(db_type)
                print(f"    Available {db_type} clients: {clients_of_type}")
            else:
                print(f"  ✗ No {db_type} connector found in configuration")
        
        # 6. Test all connections
        print("\n5. Testing all connections...")
        connection_results = config_loader.test_all_connections()
        for client, success in connection_results.items():
            status = "✓ Connected" if success else "✗ Failed"
            print(f"  {client}: {status}")
        
        print("\n" + "=" * 50)
        print("Example completed!")
        print("=" * 50)
        
        print(f"\nSummary:")
        print(f"- Configuration loaded from: {config_loader.config_file_path}")
        print(f"- {len(clients)} clients found in configuration")
        print(f"- Supported database types: MySQL, PostgreSQL, MSSQL")
        print(f"- All connectors created successfully based on db_type")
        
        # Show which database types are configured
        configured_types = set()
        for client in clients:
            try:
                info = config_loader.get_client_info(client)
                configured_types.add(info['db_type'])
            except:
                pass
        print(f"- Configured database types: {', '.join(sorted(configured_types))}")
        
    except FileNotFoundError as e:
        print(f"\n✗ Configuration file not found: {e}")
        print("Make sure config/database_configs.json exists")
    except Exception as e:
        print(f"\n✗ Error running example: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 