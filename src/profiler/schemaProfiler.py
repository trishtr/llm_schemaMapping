"""
Comprehensive Schema Data Profiler

This module provides comprehensive database schema profiling including:
- Table profiling (metadata, row counts, relationships)
- Column profiling (data types, constraints, patterns)
- Schema profiling (overall structure, relationships)
- Sample data extraction (5 rows per table)
- Field pattern recognition (healthcare patterns from field_patterns.json)
- Structural relationships (FK, PK, potential relationships, self-referencing)

The profiler collects essential information for LLM processing without full database statistics.
"""

import json
import re
import logging
from typing import Dict, Any, List, Optional, Tuple, Set
from pathlib import Path
from dataclasses import dataclass, asdict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from .database_dialect import DatabaseDialect
from .schema_models import ColumnProfile, TableProfile, SchemaProfile
from .pattern_recognizer import FieldPatternRecognizer



class SchemaDataProfiler:
    """
    Comprehensive schema data profiler that collects essential information for LLM processing.
    
    Features:
    - Table profiling (metadata, relationships, constraints)
    - Column profiling (types, patterns, sample data)
    - Schema profiling (overall structure)
    - Relationship analysis (FK, PK, potential relationships)
    - Pattern recognition (healthcare and general patterns)
    """
    
    def __init__(self, connector, database_name: str, schema_name: str = None):
        """
        Initialize the schema profiler.
        
        Args:
            connector: Database connector instance
            database_name: Name of the database to profile
            schema_name: Schema name (for databases that support schemas)
        """
        self.connector = connector
        self.database_name = database_name
        self.schema_name = schema_name
        self.db_type = self._detect_database_type()
        self.dialect = DatabaseDialect(self.db_type)
        self.pattern_recognizer = FieldPatternRecognizer()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _detect_database_type(self) -> str:
        """Detect database type from connector"""
        connector_class = self.connector.__class__.__name__
        if 'MySQL' in connector_class:
            return 'mysql'
        elif 'PostgreSQL' in connector_class:
            return 'postgresql'
        elif 'MSSQL' in connector_class:
            return 'mssql'
        else:
            return 'unknown'
    
    def profile_schema(self) -> SchemaProfile:
        """
        Profile the complete database schema (sequential processing).
        
        Returns:
            SchemaProfile object containing all profiled information
        """
        self.logger.info(f"Starting schema profiling for database: {self.database_name}")
        
        schema_profile = SchemaProfile(
            database_name=self.database_name,
            schema_name=self.schema_name,
            database_type=self.db_type
        )
        
        try:
            # Get list of tables
            tables_info = self._get_tables_info()
            schema_profile.total_tables = len(tables_info)
            
            # Profile each table
            for table_info in tables_info:
                table_profile = self._profile_table(table_info['table_name'])
                schema_profile.tables.append(table_profile)
                schema_profile.total_columns += len(table_profile.columns)
            
            # Analyze cross-table relationships
            schema_profile.cross_table_relationships = self._analyze_cross_table_relationships(schema_profile.tables)
            schema_profile.potential_relationships = self._find_potential_relationships(schema_profile.tables)
            
            # Generate pattern summary
            schema_profile.pattern_summary = self._generate_pattern_summary(schema_profile.tables)
            
            self.logger.info(f"Schema profiling completed. Tables: {schema_profile.total_tables}, Columns: {schema_profile.total_columns}")
            
        except Exception as e:
            self.logger.error(f"Error during schema profiling: {e}")
            raise
        
        return schema_profile
    
    def profile_schema_parallel(self, max_workers: int = 4) -> SchemaProfile:
        """
        Profile the complete database schema using parallel processing for large schemas.
        
        Args:
            max_workers: Maximum number of worker threads for parallel processing
            
        Returns:
            SchemaProfile object containing all profiled information
        """
        self.logger.info(f"Starting parallel schema profiling for database: {self.database_name} with {max_workers} workers")
        
        schema_profile = SchemaProfile(
            database_name=self.database_name,
            schema_name=self.schema_name,
            database_type=self.db_type
        )
        
        try:
            # Get list of tables
            tables_info = self._get_tables_info()
            schema_profile.total_tables = len(tables_info)
            
            if not tables_info:
                self.logger.warning("No tables found to profile")
                return schema_profile
            
            # Profile tables in parallel
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all table profiling tasks
                future_to_table = {
                    executor.submit(self._profile_table_safe, table_info['table_name']): table_info['table_name']
                    for table_info in tables_info
                }
                
                # Collect results as they complete
                for future in as_completed(future_to_table):
                    table_name = future_to_table[future]
                    try:
                        table_profile = future.result()
                        if table_profile:
                            schema_profile.tables.append(table_profile)
                            schema_profile.total_columns += len(table_profile.columns)
                            self.logger.debug(f"Completed profiling table: {table_name}")
                        else:
                            self.logger.warning(f"No profile data returned for table: {table_name}")
                    except Exception as e:
                        self.logger.error(f"Error profiling table {table_name}: {e}")
                        # Continue with other tables even if one fails
            
            # Sort tables by name for consistent output
            schema_profile.tables.sort(key=lambda t: t.name)
            
            # Analyze cross-table relationships
            self.logger.info("Analyzing cross-table relationships...")
            schema_profile.cross_table_relationships = self._analyze_cross_table_relationships(schema_profile.tables)
            schema_profile.potential_relationships = self._find_potential_relationships(schema_profile.tables)
            
            # Generate pattern summary
            self.logger.info("Generating pattern summary...")
            schema_profile.pattern_summary = self._generate_pattern_summary(schema_profile.tables)
            
            self.logger.info(f"Parallel schema profiling completed. Tables: {schema_profile.total_tables}, Columns: {schema_profile.total_columns}")
            
        except Exception as e:
            self.logger.error(f"Error during parallel schema profiling: {e}")
            raise
        
        return schema_profile
    
    def profile_schema_adaptive(self, parallel_threshold: int = 10, max_workers: int = 4) -> SchemaProfile:
        """
        Profile the complete database schema using adaptive processing.
        Automatically chooses between sequential and parallel processing based on schema size.
        
        Args:
            parallel_threshold: Minimum number of tables to trigger parallel processing
            max_workers: Maximum number of worker threads for parallel processing
            
        Returns:
            SchemaProfile object containing all profiled information
        """
        # Get table count first to decide on processing strategy
        tables_info = self._get_tables_info()
        table_count = len(tables_info)
        
        self.logger.info(f"Found {table_count} tables in database: {self.database_name}")
        
        if table_count >= parallel_threshold:
            self.logger.info(f"Using parallel processing ({table_count} tables >= {parallel_threshold} threshold)")
            return self.profile_schema_parallel(max_workers=max_workers)
        else:
            self.logger.info(f"Using sequential processing ({table_count} tables < {parallel_threshold} threshold)")
            return self.profile_schema()
    
    def _get_tables_info(self) -> List[Dict[str, Any]]:
        """Get basic information about all tables"""
        query = self.dialect.get_tables_query()
        
        if self.db_type == 'mssql':
            params = [self.database_name]
        elif self.db_type == 'postgresql':
            params = [self.schema_name or 'public', self.schema_name or 'public']
        else:  # mysql
            params = [self.schema_name or self.database_name]
        
        try:
            result = self.connector.execute_query(query, params)
            return result if result else []
        except Exception as e:
            self.logger.error(f"Error getting tables info: {e}")
            return []
    
    def _profile_table(self, table_name: str) -> TableProfile:
        """Profile a single table comprehensively"""
        self.logger.debug(f"Profiling table: {table_name}")
        
        table_profile = TableProfile(name=table_name, schema=self.schema_name)
        
        try:
            # Get column information
            table_profile.columns = self._get_column_profiles(table_name)
            
            # Get primary keys
            table_profile.primary_keys = self._get_primary_keys(table_name)
            
            # Get foreign keys
            table_profile.foreign_keys = self._get_foreign_keys(table_name)
            
            # Get indexes
            table_profile.indexes = self._get_indexes(table_name)
            
            # Get sample data
            table_profile.sample_data = self._get_sample_data(table_name)
            
            # Get row count estimate
            table_profile.estimated_row_count = self._get_row_count(table_name)
            
            # Find self-referencing columns
            table_profile.self_referencing_columns = self._find_self_referencing_columns(table_profile.foreign_keys, table_name)
            
            # Find potential foreign key candidates
            table_profile.potential_fk_candidates = self._find_potential_fk_candidates(table_profile.columns)
            
            # Update column profiles with additional information
            self._enrich_column_profiles(table_profile)
            
        except Exception as e:
            self.logger.error(f"Error profiling table {table_name}: {e}")
        
        return table_profile
    
    def _profile_table_safe(self, table_name: str) -> Optional[TableProfile]:
        """
        Thread-safe wrapper for table profiling used in parallel processing.
        
        Args:
            table_name: Name of the table to profile
            
        Returns:
            TableProfile object or None if profiling fails
        """
        try:
            return self._profile_table(table_name)
        except Exception as e:
            self.logger.error(f"Thread-safe table profiling failed for {table_name}: {e}")
            return None
    
    def _get_column_profiles(self, table_name: str) -> List[ColumnProfile]:
        """Get detailed column information"""
        query = self.dialect.get_column_info_query()
        
        if self.db_type == 'mssql':
            params = [self.database_name, table_name]
        else:
            params = [self.schema_name or self.database_name, table_name]
        
        try:
            result = self.connector.execute_query(query, params)
            columns = []
            
            for row in result:
                column = ColumnProfile(
                    name=row['column_name'],
                    data_type=row['data_type'],
                    is_nullable=row['is_nullable'].lower() == 'yes' if isinstance(row['is_nullable'], str) else bool(row['is_nullable']),
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
        except Exception as e:
            self.logger.error(f"Error getting column profiles for {table_name}: {e}")
            return []
    
    def _get_primary_keys(self, table_name: str) -> List[str]:
        """Get primary key columns"""
        query = self.dialect.get_primary_keys_query()
        
        if self.db_type == 'mssql':
            params = [table_name]
        else:
            params = [self.schema_name or self.database_name, table_name]
        
        try:
            result = self.connector.execute_query(query, params)
            return [row['column_name'] for row in result]
        except Exception as e:
            self.logger.error(f"Error getting primary keys for {table_name}: {e}")
            return []
    
    def _get_foreign_keys(self, table_name: str) -> List[Dict[str, str]]:
        """Get foreign key relationships"""
        query = self.dialect.get_foreign_keys_query()
        
        if self.db_type == 'mssql':
            params = [table_name]
        else:
            params = [self.schema_name or self.database_name, table_name]
        
        try:
            result = self.connector.execute_query(query, params)
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
        except Exception as e:
            self.logger.error(f"Error getting foreign keys for {table_name}: {e}")
            return []
    
    def _get_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """Get index information"""
        query = self.dialect.get_indexes_query()
        
        if self.db_type == 'mssql':
            params = [table_name]
        else:
            params = [self.schema_name or self.database_name, table_name]
        
        try:
            result = self.connector.execute_query(query, params)
            indexes = []
            
            for row in result:
                index = {
                    'index_name': row['index_name'],
                    'column_name': row['column_name'],
                    'is_unique': not row.get('non_unique', True) if 'non_unique' in row else row.get('is_unique', False)
                }
                indexes.append(index)
            
            return indexes
        except Exception as e:
            self.logger.error(f"Error getting indexes for {table_name}: {e}")
            return []
    
    def _get_sample_data(self, table_name: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Get sample data from table"""
        try:
            quoted_table = self.dialect.quote_identifier(table_name)
            if self.schema_name and self.db_type != 'mysql':
                quoted_table = f"{self.dialect.quote_identifier(self.schema_name)}.{quoted_table}"
            
            query = self.dialect.get_sample_query("*", quoted_table, limit)
            result = self.connector.execute_query(query)
            
            return result[:limit] if result else []
        except Exception as e:
            self.logger.error(f"Error getting sample data for {table_name}: {e}")
            return []
    
    def _get_row_count(self, table_name: str) -> int:
        """Get estimated row count"""
        try:
            quoted_table = self.dialect.quote_identifier(table_name)
            if self.schema_name and self.db_type != 'mysql':
                quoted_table = f"{self.dialect.quote_identifier(self.schema_name)}.{quoted_table}"
            
            query = self.dialect.get_count_query(quoted_table)
            result = self.connector.execute_query(query)
            
            return result[0]['row_count'] if result else 0
        except Exception as e:
            self.logger.error(f"Error getting row count for {table_name}: {e}")
            return 0
    
    def _find_self_referencing_columns(self, foreign_keys: List[Dict[str, str]], table_name: str) -> List[str]:
        """Find columns that reference the same table (self-referencing)"""
        self_referencing = []
        for fk in foreign_keys:
            if fk['referenced_table'] == table_name:
                self_referencing.append(fk['column_name'])
        return self_referencing
    
    def _find_potential_fk_candidates(self, columns: List[ColumnProfile]) -> List[Dict[str, Any]]:
        """Find columns that could potentially be foreign keys based on naming patterns"""
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
    
    def _enrich_column_profiles(self, table_profile: TableProfile):
        """Enrich column profiles with additional information"""
        # Mark primary key columns
        for column in table_profile.columns:
            if column.name in table_profile.primary_keys:
                column.is_primary_key = True
        
        # Mark foreign key columns and add references
        for fk in table_profile.foreign_keys:
            for column in table_profile.columns:
                if column.name == fk['column_name']:
                    column.is_foreign_key = True
                    column.foreign_key_reference = {
                        'table': fk['referenced_table'],
                        'column': fk['referenced_column'],
                        'constraint': fk['constraint_name']
                    }
        
        # Mark indexed columns
        for index in table_profile.indexes:
            for column in table_profile.columns:
                if column.name == index['column_name']:
                    column.is_indexed = True
                    if index.get('is_unique'):
                        column.is_unique = True
        
        # Add sample values and detect patterns
        if table_profile.sample_data:
            for column in table_profile.columns:
                # Extract sample values for this column
                column.sample_values = [row.get(column.name) for row in table_profile.sample_data[:5]]
                
                # Detect patterns
                column.detected_patterns = self.pattern_recognizer.detect_patterns(
                    column.sample_values, field_name=column.name
                )
    
    def _analyze_cross_table_relationships(self, tables: List[TableProfile]) -> List[Dict[str, Any]]:
        """Analyze relationships between tables"""
        relationships = []
        
        for table in tables:
            for fk in table.foreign_keys:
                relationship = {
                    'type': 'foreign_key',
                    'from_table': table.name,
                    'from_column': fk['column_name'],
                    'to_table': fk['referenced_table'],
                    'to_column': fk['referenced_column'],
                    'constraint_name': fk['constraint_name']
                }
                relationships.append(relationship)
        
        return relationships
    
    def _find_potential_relationships(self, tables: List[TableProfile]) -> List[Dict[str, Any]]:
        """Find potential relationships based on column names and types"""
        potential_relationships = []
        
        # Create a mapping of table names to their primary key columns
        table_pk_map = {}
        for table in tables:
            if table.primary_keys:
                table_pk_map[table.name] = table.primary_keys[0]  # Use first PK if multiple
        
        # Look for potential relationships
        for table in tables:
            for column in table.columns:
                if not column.is_foreign_key and not column.is_primary_key:
                    # Check if column name suggests a reference to another table
                    for other_table_name, pk_column in table_pk_map.items():
                        if other_table_name != table.name:
                            # Check various naming patterns
                            potential_patterns = [
                                f"{other_table_name}_id",
                                f"{other_table_name}_{pk_column}",
                                f"{other_table_name}_key",
                                pk_column if column.name.lower() == pk_column.lower() else None
                            ]
                            
                            if column.name.lower() in [p.lower() for p in potential_patterns if p]:
                                potential_relationships.append({
                                    'type': 'potential_foreign_key',
                                    'from_table': table.name,
                                    'from_column': column.name,
                                    'to_table': other_table_name,
                                    'to_column': pk_column,
                                    'confidence': 'medium',
                                    'reason': 'Column name pattern match'
                                })
        
        return potential_relationships
    
    def _generate_pattern_summary(self, tables: List[TableProfile]) -> Dict[str, int]:
        """Generate summary of detected patterns"""
        pattern_counts = {}
        
        for table in tables:
            for column in table.columns:
                for pattern in column.detected_patterns:
                    pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1
        
        return pattern_counts
    
    def export_profile(self, schema_profile: SchemaProfile, output_path: str = None) -> Dict[str, Any]:
        """
        Export schema profile to dictionary format suitable for LLM processing.
        
        Args:
            schema_profile: SchemaProfile object to export
            output_path: Optional file path to save JSON output
            
        Returns:
            Dictionary representation of the schema profile
        """
        profile_dict = asdict(schema_profile)
        
        if output_path:
            try:
                with open(output_path, 'w') as f:
                    json.dump(profile_dict, f, indent=2, default=str)
                self.logger.info(f"Schema profile exported to: {output_path}")
            except Exception as e:
                self.logger.error(f"Error exporting profile to {output_path}: {e}")
        
        return profile_dict