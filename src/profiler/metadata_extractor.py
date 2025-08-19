"""
Database Metadata Extractor

This module provides a dedicated class for extracting database metadata,
consolidating all the _get_* methods from the schema profiler into a
more organized and reusable structure.
"""

import re
import logging
from typing import Dict, Any, List, Optional, Union, Tuple
from .database_query import DatabaseQuery, MetadataQueryMixin
from .database_dialect import DatabaseDialect
from .schema_models import ColumnProfile


class MetadataExtractor(DatabaseQuery, MetadataQueryMixin):
    """
    Extracts comprehensive metadata from database schemas.
    
    This class consolidates all metadata extraction operations including:
    - Table information
    - Column profiles
    - Primary keys
    - Foreign keys
    - Indexes
    - Sample data
    - Row counts
    - Relationship analysis
    """
    
    def __init__(self, connector, database_name: str, schema_name: str = None, db_type: str = 'unknown'):
        """
        Initialize the metadata extractor.
        
        Args:
            connector: Database connector instance
            database_name: Name of the database
            schema_name: Schema name (for databases that support schemas)
            db_type: Database type (mysql, postgresql, mssql)
        """
        super().__init__(connector, database_name, schema_name, db_type)
        self.dialect = DatabaseDialect(self.db_type)
    
    def get_supported_operations(self) -> List[str]:
        """Get list of supported metadata extraction operations."""
        return [
            'tables_info',
            'column_profiles',
            'primary_keys',
            'foreign_keys',
            'indexes',
            'sample_data',
            'row_count',
            'self_referencing_columns',
            'potential_fk_candidates'
        ]
    
    def get_tables_info(self) -> List[Dict[str, Any]]:
        """
        Get basic information about all tables in the database.
        
        Returns:
            List of dictionaries containing table information
        """
        return self._get_metadata_with_dialect('get_tables_query', operation_name='tables_info')
    
    def get_column_profiles(self, table_name: str) -> List[ColumnProfile]:
        """
        Get detailed column information for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of ColumnProfile objects
        """
        result = self._get_metadata_with_dialect(
            'get_column_info_query', 
            table_name=table_name,
            operation_name='column_profiles'
        )
        
        columns = []
        for row in result:
            column = ColumnProfile(
                name=row['column_name'],
                data_type=row['data_type'],
                is_nullable=self._parse_nullable(row['is_nullable']),
                max_length=row.get('character_maximum_length'),
                default_value=row.get('column_default'),
                column_comment=row.get('column_comment'),
                ordinal_position=row.get('ordinal_position', 0)
            )
            
            # MySQL specific column key information
            if self.db_type == 'mysql' and 'column_key' in row:
                column.is_primary_key = row['column_key'] == 'PRI'
                column.is_unique = row['column_key'] in ['PRI', 'UNI']
            
            columns.append(column)
        
        return columns
    
    def get_primary_keys(self, table_name: str) -> List[str]:
        """
        Get primary key columns for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of primary key column names
        """
        return self._get_metadata_with_dialect(
            'get_primary_keys_query',
            table_name=table_name,
            operation_name='primary_keys',
            extract_key='column_name'
        )
    
    def get_foreign_keys(self, table_name: str) -> List[Dict[str, str]]:
        """
        Get foreign key relationships for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of foreign key relationship dictionaries
        """
        result = self._get_metadata_with_dialect(
            'get_foreign_keys_query',
            table_name=table_name,
            operation_name='foreign_keys'
        )
        
        foreign_keys = []
        for row in result:
            fk = {
                'column_name': row['column_name'],
                'referenced_table': row['referenced_table'],
                'referenced_column': row['referenced_column'],
                'constraint_name': row['constraint_name']
            }
            foreign_keys.append(fk)
        
        return foreign_keys
    
    def get_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get index information for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of index information dictionaries
        """
        result = self._get_metadata_with_dialect(
            'get_indexes_query',
            table_name=table_name,
            operation_name='indexes'
        )
        
        indexes = []
        for row in result:
            index = {
                'index_name': row['index_name'],
                'column_name': row['column_name'],
                'is_unique': self._parse_unique_flag(row)
            }
            indexes.append(index)
        
        return indexes
    
    def get_sample_data(self, table_name: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get sample data from a specific table.
        
        Args:
            table_name: Name of the table
            limit: Number of rows to retrieve
            
        Returns:
            List of sample data rows
        """
        try:
            quoted_table = self.get_quoted_table_name(table_name)
            query = self.dialect.get_sample_query("*", quoted_table, limit)
            
            result = self._execute_query_safe(
                query, 
                params=None,
                operation_name='sample_data',
                table_name=table_name
            )
            
            return result[:limit] if result else []
        except Exception as e:
            self.logger.error(f"Error getting sample data for {table_name}: {e}")
            return []
    
    def get_row_count(self, table_name: str) -> int:
        """
        Get estimated row count for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Estimated row count
        """
        try:
            quoted_table = self.get_quoted_table_name(table_name)
            query = self.dialect.get_count_query(quoted_table)
            
            return self._execute_single_value_query(
                query,
                params=None,
                operation_name='row_count',
                table_name=table_name,
                value_key='row_count',
                default_value=0
            )
        except Exception as e:
            self.logger.error(f"Error getting row count for {table_name}: {e}")
            return 0
    
    def find_self_referencing_columns(self, foreign_keys: List[Dict[str, str]], table_name: str) -> List[str]:
        """
        Find columns that reference the same table (self-referencing).
        
        Args:
            foreign_keys: List of foreign key relationships
            table_name: Name of the table
            
        Returns:
            List of self-referencing column names
        """
        self_referencing = []
        for fk in foreign_keys:
            if fk['referenced_table'] == table_name:
                self_referencing.append(fk['column_name'])
        return self_referencing
    
    def find_potential_fk_candidates(self, columns: List[ColumnProfile]) -> List[Dict[str, Any]]:
        """
        Find columns that could potentially be foreign keys based on naming patterns.
        
        Args:
            columns: List of column profiles
            
        Returns:
            List of potential foreign key candidates
        """
        potential_fks = []
        
        fk_patterns = [
            r'.*_id$',
            r'.*_key$',
            r'.*_code$',
            r'.*_ref$',
            r'.*_fk$'
        ]
        
        for column in columns:
            if not column.is_foreign_key and not column.is_primary_key:
                for pattern in fk_patterns:
                    if re.match(pattern, column.name.lower()):
                        potential_fks.append({
                            'column_name': column.name,
                            'data_type': column.data_type,
                            'reason': f'Matches pattern: {pattern}'
                        })
                        break
        
        return potential_fks
    
    def enrich_column_profiles(self, columns: List[ColumnProfile], primary_keys: List[str],
                              foreign_keys: List[Dict[str, str]], indexes: List[Dict[str, Any]],
                              sample_data: List[Dict[str, Any]]) -> List[ColumnProfile]:
        """
        Enrich column profiles with additional metadata information.
        
        Args:
            columns: List of column profiles to enrich
            primary_keys: List of primary key column names
            foreign_keys: List of foreign key relationships
            indexes: List of index information
            sample_data: Sample data from the table
            
        Returns:
            Enriched list of column profiles
        """
        # Mark primary key columns
        for column in columns:
            if column.name in primary_keys:
                column.is_primary_key = True
        
        # Mark foreign key columns and add references
        for fk in foreign_keys:
            for column in columns:
                if column.name == fk['column_name']:
                    column.is_foreign_key = True
                    column.foreign_key_reference = {
                        'table': fk['referenced_table'],
                        'column': fk['referenced_column'],
                        'constraint': fk['constraint_name']
                    }
        
        # Mark indexed columns
        for index in indexes:
            for column in columns:
                if column.name == index['column_name']:
                    column.is_indexed = True
                    if index.get('is_unique'):
                        column.is_unique = True
        
        # Add sample values
        if sample_data:
            for column in columns:
                column.sample_values = [row.get(column.name) for row in sample_data[:5]]
        
        return columns
    
    def get_complete_table_metadata(self, table_name: str) -> Dict[str, Union[str, int, List[ColumnProfile], List[str], List[Dict[str, Any]]]]:
        """
        Get complete metadata for a single table including all available information.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary containing all metadata for the table with specific types:
            - table_name: str
            - columns: List[ColumnProfile]  
            - primary_keys: List[str]
            - foreign_keys: List[Dict[str, str]]
            - indexes: List[Dict[str, Any]]
            - sample_data: List[Dict[str, Any]]
            - row_count: int
            - self_referencing_columns: List[str]
            - potential_fk_candidates: List[Dict[str, Any]]
        """
        metadata = {
            'table_name': table_name,
            'columns': self.get_column_profiles(table_name),
            'primary_keys': self.get_primary_keys(table_name),
            'foreign_keys': self.get_foreign_keys(table_name),
            'indexes': self.get_indexes(table_name),
            'sample_data': self.get_sample_data(table_name),
            'row_count': self.get_row_count(table_name)
        }
        
        # Add derived information
        metadata['self_referencing_columns'] = self.find_self_referencing_columns(
            metadata['foreign_keys'], table_name
        )
        metadata['potential_fk_candidates'] = self.find_potential_fk_candidates(
            metadata['columns']
        )
        
        # Enrich column profiles
        metadata['columns'] = self.enrich_column_profiles(
            metadata['columns'],
            metadata['primary_keys'],
            metadata['foreign_keys'],
            metadata['indexes'],
            metadata['sample_data']
        )
        
        return metadata
    
    def get_schema_metadata_summary(self) -> Dict[str, Any]:
        """
        Get a summary of metadata extraction capabilities and database information.
        
        Returns:
            Dictionary containing schema metadata summary
        """
        tables_info = self.get_tables_info()
        
        return {
            'database_name': self.database_name,
            'schema_name': self.schema_name,
            'database_type': self.db_type,
            'total_tables': len(tables_info),
            'supported_operations': self.get_supported_operations(),
            'tables': [table['table_name'] for table in tables_info] if tables_info else []
        }
    
    # Helper methods
    def _parse_nullable(self, nullable_value: Any) -> bool:
        """Parse nullable value from database result."""
        if isinstance(nullable_value, str):
            return nullable_value.lower() == 'yes'
        return bool(nullable_value)
    
    def _parse_unique_flag(self, row: Dict[str, Any]) -> bool:
        """Parse unique flag from index information."""
        # Handle different database-specific column names
        if 'non_unique' in row:
            return not row['non_unique']
        elif 'is_unique' in row:
            return bool(row['is_unique'])
        else:
            return False 