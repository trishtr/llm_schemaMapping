"""
Core Schema Profiler

This module contains the core schema profiling logic, focused purely on
extracting and analyzing database schema information without incremental
complexity or processing strategy concerns.
"""

import logging
from typing import Dict, Any, List, Optional

from .interfaces import SchemaProfiler, ProfilerConfig
from .schema_models import SchemaProfile, TableProfile
from .metadata_extractor import MetadataExtractor
from .pattern_recognizer import FieldPatternRecognizer
from .database_dialect import DatabaseDialect


class CoreSchemaProfiler(SchemaProfiler):
    """
    Core schema profiler focused on pure profiling logic.
    
    This class handles the essential schema profiling operations without
    concerns about processing strategies, incremental updates, or state management.
    Those concerns are handled by other components in the architecture.
    """
    
    def __init__(self, connector, metadata_extractor: Optional[MetadataExtractor] = None,
                 pattern_recognizer: Optional[FieldPatternRecognizer] = None):
        """
        Initialize the core profiler with its dependencies.
        
        Args:
            connector: Database connector instance
            metadata_extractor: Optional metadata extractor (will create if None)
            pattern_recognizer: Optional pattern recognizer (will create if None)
        """
        self.connector = connector
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Detect database type
        self.db_type = self._detect_database_type()
        self.dialect = DatabaseDialect(self.db_type)
        
        # Initialize or use provided dependencies
        self.metadata_extractor = metadata_extractor or MetadataExtractor(
            connector, "", None, self.db_type
        )
        self.pattern_recognizer = pattern_recognizer or FieldPatternRecognizer()
    
    def profile_schema(self, config: ProfilerConfig) -> SchemaProfile:
        """
        Profile the complete database schema.
        
        Args:
            config: Profiling configuration
            
        Returns:
            Complete schema profile
        """
        self.logger.info(f"Starting core schema profiling for database: {config.database_name}")
        
        # Initialize schema profile
        schema_profile = SchemaProfile(
            database_name=config.database_name,
            schema_name=config.schema_name,
            database_type=self.db_type
        )
        
        try:
            # Update metadata extractor with current config
            self._update_metadata_extractor_config(config)
            
            # Get list of tables
            tables_info = self.metadata_extractor.get_tables_info()
            schema_profile.total_tables = len(tables_info)
            
            if not tables_info:
                self.logger.warning("No tables found to profile")
                return schema_profile
            
            # Profile each table (processing strategy handled externally)
            table_profiles = []
            for table_info in tables_info:
                table_profile = self.profile_table(table_info['table_name'], config)
                table_profiles.append(table_profile)
                schema_profile.total_columns += len(table_profile.columns)
            
            schema_profile.tables = table_profiles
            
            # Post-processing analysis
            self._analyze_schema_relationships(schema_profile, config)
            
            self.logger.info(f"Core profiling completed. Tables: {schema_profile.total_tables}, Columns: {schema_profile.total_columns}")
            
        except Exception as e:
            self.logger.error(f"Error during core schema profiling: {e}")
            raise
        
        return schema_profile
    
    def profile_table(self, table_name: str, config: ProfilerConfig) -> TableProfile:
        """
        Profile a single table comprehensively.
        
        Args:
            table_name: Name of the table to profile
            config: Profiling configuration
            
        Returns:
            Complete table profile
        """
        self.logger.debug(f"Profiling table: {table_name}")
        
        try:
            # Get complete metadata using the metadata extractor
            metadata = self.metadata_extractor.get_complete_table_metadata(table_name)
            
            # Create table profile from metadata
            table_profile = TableProfile(
                name=table_name,
                schema=config.schema_name,
                columns=metadata['columns'],
                primary_keys=metadata['primary_keys'],
                foreign_keys=metadata['foreign_keys'],
                indexes=metadata['indexes'],
                sample_data=metadata['sample_data'] if config.include_sample_data else [],
                estimated_row_count=metadata['row_count'],
                self_referencing_columns=metadata['self_referencing_columns'],
                potential_fk_candidates=metadata['potential_fk_candidates']
            )
            
            # Add pattern detection if enabled
            if config.pattern_recognition_enabled:
                self._add_pattern_detection(table_profile, config)
            
            return table_profile
            
        except Exception as e:
            self.logger.error(f"Error profiling table {table_name}: {e}")
            # Return minimal profile rather than failing completely
            return TableProfile(
                name=table_name,
                schema=config.schema_name,
                columns=[],
                primary_keys=[],
                foreign_keys=[],
                indexes=[],
                sample_data=[],
                estimated_row_count=0,
                self_referencing_columns=[],
                potential_fk_candidates=[]
            )
    
    def profile_tables(self, table_names: List[str], config: ProfilerConfig) -> List[TableProfile]:
        """
        Profile multiple tables.
        
        Args:
            table_names: List of table names to profile
            config: Profiling configuration
            
        Returns:
            List of table profiles
        """
        profiles = []
        for table_name in table_names:
            profile = self.profile_table(table_name, config)
            profiles.append(profile)
        
        return profiles
    
    def get_tables_info(self) -> List[Dict[str, Any]]:
        """Get basic information about all tables."""
        return self.metadata_extractor.get_tables_info()
    
    def _detect_database_type(self) -> str:
        """Detect database type from connector."""
        connector_class = self.connector.__class__.__name__
        if 'MySQL' in connector_class:
            return 'mysql'
        elif 'PostgreSQL' in connector_class:
            return 'postgresql'
        elif 'MSSQL' in connector_class:
            return 'mssql'
        else:
            return 'unknown'
    
    def _update_metadata_extractor_config(self, config: ProfilerConfig) -> None:
        """Update metadata extractor with current configuration."""
        # Update the metadata extractor's database and schema names
        self.metadata_extractor.database_name = config.database_name
        self.metadata_extractor.schema_name = config.schema_name
    
    def _add_pattern_detection(self, table_profile: TableProfile, config: ProfilerConfig) -> None:
        """Add pattern detection to column profiles."""
        if not table_profile.sample_data:
            return
        
        for column in table_profile.columns:
            if column.sample_values:
                try:
                    # Detect patterns using the pattern recognizer
                    column.detected_patterns = self.pattern_recognizer.detect_patterns(
                        column.sample_values, field_name=column.name
                    )
                except Exception as e:
                    self.logger.warning(f"Pattern detection failed for column {column.name}: {e}")
                    column.detected_patterns = []
    
    def _analyze_schema_relationships(self, schema_profile: SchemaProfile, config: ProfilerConfig) -> None:
        """Analyze cross-table relationships and generate summaries."""
        if not config.validate_relationships:
            return
        
        try:
            # Analyze cross-table relationships
            schema_profile.cross_table_relationships = self._analyze_cross_table_relationships(schema_profile.tables)
            
            # Find potential relationships
            schema_profile.potential_relationships = self._find_potential_relationships(schema_profile.tables)
            
            # Generate pattern summary
            if config.pattern_recognition_enabled:
                schema_profile.pattern_summary = self._generate_pattern_summary(schema_profile.tables)
            
        except Exception as e:
            self.logger.error(f"Error analyzing schema relationships: {e}")
            # Set empty relationships rather than failing
            schema_profile.cross_table_relationships = []
            schema_profile.potential_relationships = []
            schema_profile.pattern_summary = {}
    
    def _analyze_cross_table_relationships(self, tables: List[TableProfile]) -> List[Dict[str, Any]]:
        """Analyze relationships between tables."""
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
        """Find potential relationships based on column names and types."""
        potential_relationships = []
        
        # Create a map of table names to their primary key columns
        pk_map = {}
        for table in tables:
            if table.primary_keys:
                pk_map[table.name] = table.primary_keys[0]  # Use first PK for simplicity
        
        # Look for potential foreign keys
        for table in tables:
            for column in table.columns:
                if not column.is_foreign_key and not column.is_primary_key:
                    # Check if column name suggests it's a foreign key
                    for target_table, pk_column in pk_map.items():
                        if target_table != table.name:
                            # Simple heuristic: column_name ends with target_table name or pk_column
                            if (column.name.lower().endswith(f"_{target_table.lower()}_id") or
                                column.name.lower().endswith(f"_{pk_column.lower()}") or
                                column.name.lower() == f"{target_table.lower()}_id"):
                                
                                potential_relationships.append({
                                    'type': 'potential_foreign_key',
                                    'from_table': table.name,
                                    'from_column': column.name,
                                    'to_table': target_table,
                                    'to_column': pk_column,
                                    'reason': 'Column name pattern suggests relationship',
                                    'confidence': 'medium'
                                })
        
        return potential_relationships
    
    def _generate_pattern_summary(self, tables: List[TableProfile]) -> Dict[str, int]:
        """Generate a summary of detected patterns across all tables."""
        pattern_counts = {}
        
        for table in tables:
            for column in table.columns:
                for pattern in column.detected_patterns:
                    pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1
        
        return pattern_counts
    
    def export_profile(self, schema_profile: SchemaProfile, output_path: str) -> Dict[str, Any]:
        """
        Export schema profile to file.
        
        Args:
            schema_profile: Schema profile to export
            output_path: Output file path
            
        Returns:
            Dictionary representation of the profile
        """
        import json
        from dataclasses import asdict
        from pathlib import Path
        
        try:
            # Convert to dictionary
            profile_dict = asdict(schema_profile)
            
            # Ensure directory exists
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Export to JSON
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(profile_dict, f, indent=2, ensure_ascii=False, default=str)
            
            self.logger.info(f"Schema profile exported to {output_path}")
            return profile_dict
            
        except Exception as e:
            self.logger.error(f"Error exporting profile: {e}")
            raise 