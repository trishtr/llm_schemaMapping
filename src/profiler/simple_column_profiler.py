#!/usr/bin/env python3
"""
Simple Column Profiler

A focused profiler that analyzes individual database columns without the complexity
of full schema profiling. Designed for targeted column analysis and LLM processing.

Key Features:
- Column metadata extraction
- Data type analysis
- Null/unique/key detection
- Pattern recognition
- Sample data collection
- Simple, clean output format
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Union
import logging
from datetime import datetime

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from connectors.base_connector import BaseConnector
from .simple_pattern_recognizer import SimplePatternRecognizer
from .database_query import DatabaseQuery


@dataclass
class ColumnAnalysis:
    """Simple column analysis result."""
    
    # Basic column information
    column_name: str
    table_name: str
    schema_name: Optional[str] = None
    
    # Data type information
    data_type: str
    max_length: Optional[int] = None
    numeric_precision: Optional[int] = None
    numeric_scale: Optional[int] = None
    
    # Column properties
    is_nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_unique: bool = False
    is_indexed: bool = False
    
    # Position and metadata
    ordinal_position: int = 0
    default_value: Optional[str] = None
    column_comment: Optional[str] = None
    
    # Data analysis
    estimated_row_count: int = 0
    null_count: int = 0
    unique_count: int = 0
    null_percentage: float = 0.0
    unique_percentage: float = 0.0
    
    # Sample data and patterns
    sample_values: List[Any] = field(default_factory=list)
    detected_patterns: List[str] = field(default_factory=list)
    
    # Foreign key information
    foreign_key_reference: Optional[Dict[str, str]] = None
    
    # Analysis metadata
    analysis_timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class ColumnProfilingResult:
    """Result container for column profiling operation."""
    
    database_name: str
    table_name: str
    schema_name: Optional[str] = None
    database_type: str = "unknown"
    
    # Column analyses
    columns: List[ColumnAnalysis] = field(default_factory=list)
    
    # Summary statistics
    total_columns: int = 0
    total_rows_analyzed: int = 0
    analysis_timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    # Analysis configuration
    sample_size: int = 5
    pattern_detection_enabled: bool = True


class SimpleColumnProfiler(DatabaseQuery):
    """Simple column-focused profiler."""
    
    def __init__(self, connector: BaseConnector, sample_size: int = 5):
        super().__init__(connector)
        self.sample_size = sample_size
        self.pattern_recognizer = SimplePatternRecognizer()
        self.logger = logging.getLogger(__name__)
    
    def profile_table_columns(self, table_name: str, schema_name: Optional[str] = None) -> ColumnProfilingResult:
        """Profile all columns in a specific table."""
        
        try:
            self.logger.info(f"Starting column profiling for table: {table_name}")
            
            # Get column metadata
            columns_metadata = self._get_columns_metadata(table_name, schema_name)
            
            if not columns_metadata:
                self.logger.warning(f"No columns found for table: {table_name}")
                return ColumnProfilingResult(
                    database_name=self.connector.connection_config.get('database', 'unknown'),
                    table_name=table_name,
                    schema_name=schema_name,
                    database_type=self.connector.db_type
                )
            
            # Analyze each column
            column_analyses = []
            total_rows = 0
            
            for col_meta in columns_metadata:
                column_analysis = self._analyze_column(col_meta, table_name, schema_name)
                column_analyses.append(column_analysis)
                total_rows = max(total_rows, column_analysis.estimated_row_count)
            
            # Create result
            result = ColumnProfilingResult(
                database_name=self.connector.connection_config.get('database', 'unknown'),
                table_name=table_name,
                schema_name=schema_name,
                database_type=self.connector.db_type,
                columns=column_analyses,
                total_columns=len(column_analyses),
                total_rows_analyzed=total_rows,
                sample_size=self.sample_size
            )
            
            self.logger.info(f"Completed column profiling for table: {table_name} ({len(column_analyses)} columns)")
            return result
            
        except Exception as e:
            self.logger.error(f"Error profiling columns for table {table_name}: {str(e)}")
            raise
    
    def profile_single_column(self, table_name: str, column_name: str, 
                             schema_name: Optional[str] = None) -> ColumnAnalysis:
        """Profile a single specific column."""
        
        try:
            self.logger.info(f"Starting single column profiling: {table_name}.{column_name}")
            
            # Get column metadata
            columns_metadata = self._get_columns_metadata(table_name, schema_name)
            
            # Find the specific column
            col_meta = None
            for meta in columns_metadata:
                if meta['column_name'].lower() == column_name.lower():
                    col_meta = meta
                    break
            
            if not col_meta:
                raise ValueError(f"Column '{column_name}' not found in table '{table_name}'")
            
            # Analyze the column
            column_analysis = self._analyze_column(col_meta, table_name, schema_name)
            
            self.logger.info(f"Completed single column profiling: {table_name}.{column_name}")
            return column_analysis
            
        except Exception as e:
            self.logger.error(f"Error profiling column {table_name}.{column_name}: {str(e)}")
            raise
    
    def _analyze_column(self, col_meta: Dict[str, Any], table_name: str, 
                       schema_name: Optional[str]) -> ColumnAnalysis:
        """Analyze a single column based on its metadata."""
        
        column_name = col_meta['column_name']
        
        # Create basic column analysis
        analysis = ColumnAnalysis(
            column_name=column_name,
            table_name=table_name,
            schema_name=schema_name,
            data_type=col_meta.get('data_type', 'unknown'),
            max_length=col_meta.get('character_maximum_length'),
            numeric_precision=col_meta.get('numeric_precision'),
            numeric_scale=col_meta.get('numeric_scale'),
            is_nullable=self._parse_nullable(col_meta.get('is_nullable', 'YES')),
            ordinal_position=col_meta.get('ordinal_position', 0),
            default_value=col_meta.get('column_default'),
            column_comment=col_meta.get('column_comment')
        )
        
        # Get table row count
        analysis.estimated_row_count = self._get_table_row_count(table_name, schema_name)
        
        # Analyze column data
        self._analyze_column_data(analysis, table_name, schema_name)
        
        # Detect key properties
        self._detect_key_properties(analysis, table_name, schema_name)
        
        # Detect patterns
        if analysis.sample_values:
            analysis.detected_patterns = self.pattern_recognizer.detect_patterns(
                analysis.sample_values, 
                field_name=column_name
            )
        
        return analysis
    
    def _analyze_column_data(self, analysis: ColumnAnalysis, table_name: str, 
                           schema_name: Optional[str]) -> None:
        """Analyze column data statistics and collect samples."""
        
        full_table_name = self._get_full_table_name(table_name, schema_name)
        column_name = analysis.column_name
        
        try:
            # Get column statistics
            stats_query = f"""
            SELECT 
                COUNT(*) as total_count,
                COUNT({column_name}) as non_null_count,
                COUNT(DISTINCT {column_name}) as unique_count
            FROM {full_table_name}
            """
            
            stats_result = self.execute_query(stats_query)
            if stats_result:
                row = stats_result[0]
                total_count = row.get('total_count', 0)
                non_null_count = row.get('non_null_count', 0)
                unique_count = row.get('unique_count', 0)
                
                analysis.null_count = total_count - non_null_count
                analysis.unique_count = unique_count
                
                if total_count > 0:
                    analysis.null_percentage = (analysis.null_count / total_count) * 100
                    analysis.unique_percentage = (unique_count / total_count) * 100
                    
                # Determine if column is effectively unique
                if total_count > 0 and unique_count == non_null_count and analysis.null_count <= 1:
                    analysis.is_unique = True
            
            # Get sample values
            sample_query = f"""
            SELECT DISTINCT {column_name}
            FROM {full_table_name}
            WHERE {column_name} IS NOT NULL
            LIMIT {self.sample_size}
            """
            
            sample_result = self.execute_query(sample_query)
            if sample_result:
                analysis.sample_values = [row[column_name] for row in sample_result]
                
        except Exception as e:
            self.logger.warning(f"Error analyzing column data for {column_name}: {str(e)}")
    
    def _detect_key_properties(self, analysis: ColumnAnalysis, table_name: str, 
                             schema_name: Optional[str]) -> None:
        """Detect primary key, foreign key, and index properties."""
        
        try:
            # Check for primary key
            pk_query = self._get_primary_key_query(table_name, schema_name)
            pk_result = self.execute_query(pk_query)
            
            if pk_result:
                pk_columns = [row.get('column_name', '').lower() for row in pk_result]
                analysis.is_primary_key = analysis.column_name.lower() in pk_columns
            
            # Check for foreign keys
            fk_query = self._get_foreign_key_query(table_name, schema_name)
            fk_result = self.execute_query(fk_query)
            
            if fk_result:
                for row in fk_result:
                    if row.get('column_name', '').lower() == analysis.column_name.lower():
                        analysis.is_foreign_key = True
                        analysis.foreign_key_reference = {
                            'referenced_table': row.get('referenced_table_name'),
                            'referenced_column': row.get('referenced_column_name'),
                            'constraint_name': row.get('constraint_name')
                        }
                        break
            
            # Check for indexes
            idx_query = self._get_index_query(table_name, schema_name)
            idx_result = self.execute_query(idx_query)
            
            if idx_result:
                indexed_columns = [row.get('column_name', '').lower() for row in idx_result]
                analysis.is_indexed = analysis.column_name.lower() in indexed_columns
                
        except Exception as e:
            self.logger.warning(f"Error detecting key properties for {analysis.column_name}: {str(e)}")
    
    def _get_columns_metadata(self, table_name: str, schema_name: Optional[str]) -> List[Dict[str, Any]]:
        """Get column metadata for a table."""
        
        from .database_dialect import DatabaseDialect
        dialect = DatabaseDialect(self.connector.db_type)
        
        query = dialect.get_columns_query()
        params = {'table_name': table_name}
        
        if schema_name:
            params['schema_name'] = schema_name
        
        return self.execute_query(query, params) or []
    
    def _get_table_row_count(self, table_name: str, schema_name: Optional[str]) -> int:
        """Get estimated row count for a table."""
        
        try:
            full_table_name = self._get_full_table_name(table_name, schema_name)
            query = f"SELECT COUNT(*) as row_count FROM {full_table_name}"
            
            result = self.execute_query(query)
            if result:
                return result[0].get('row_count', 0)
                
        except Exception as e:
            self.logger.warning(f"Error getting row count for {table_name}: {str(e)}")
        
        return 0
    
    def _get_primary_key_query(self, table_name: str, schema_name: Optional[str]) -> str:
        """Get database-specific primary key query."""
        
        from .database_dialect import DatabaseDialect
        dialect = DatabaseDialect(self.connector.db_type)
        return dialect.get_primary_keys_query()
    
    def _get_foreign_key_query(self, table_name: str, schema_name: Optional[str]) -> str:
        """Get database-specific foreign key query."""
        
        from .database_dialect import DatabaseDialect
        dialect = DatabaseDialect(self.connector.db_type)
        return dialect.get_foreign_keys_query()
    
    def _get_index_query(self, table_name: str, schema_name: Optional[str]) -> str:
        """Get database-specific index query."""
        
        from .database_dialect import DatabaseDialect
        dialect = DatabaseDialect(self.connector.db_type)
        return dialect.get_indexes_query()
    
    def _get_full_table_name(self, table_name: str, schema_name: Optional[str]) -> str:
        """Get properly formatted full table name."""
        
        if schema_name:
            return f"{schema_name}.{table_name}"
        return table_name
    
    def _parse_nullable(self, nullable_value: Any) -> bool:
        """Parse nullable value from database metadata."""
        
        if isinstance(nullable_value, bool):
            return nullable_value
        
        if isinstance(nullable_value, str):
            return nullable_value.upper() in ['YES', 'TRUE', '1']
        
        return bool(nullable_value) 