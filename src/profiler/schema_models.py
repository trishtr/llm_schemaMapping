"""
Schema Data Models

This module contains dataclasses that represent the structure of database schema profiling data.
These models are used to organize and store schema information in a structured format suitable
for LLM processing and analysis.

Models:
- ColumnProfile: Information about a single database column
- TableProfile: Information about a single database table
- SchemaProfile: Complete database schema information
"""

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from datetime import datetime


@dataclass
class ColumnProfile:
    """
    Profile information for a single database column.
    
    Contains metadata, constraints, sample data, and detected patterns for a column.
    """
    name: str
    data_type: str
    is_nullable: bool
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_unique: bool = False
    is_indexed: bool = False
    max_length: Optional[int] = None
    default_value: Optional[str] = None
    column_comment: Optional[str] = None
    ordinal_position: int = 0
    detected_patterns: List[str] = field(default_factory=list)
    sample_values: List[Any] = field(default_factory=list)
    foreign_key_reference: Optional[Dict[str, str]] = None
    
    def __post_init__(self):
        """Ensure lists are initialized properly."""
        if self.detected_patterns is None:
            self.detected_patterns = []
        if self.sample_values is None:
            self.sample_values = []
    
    def has_pattern(self, pattern: str) -> bool:
        """Check if column has a specific detected pattern."""
        return pattern in self.detected_patterns
    
    def is_key_column(self) -> bool:
        """Check if column is any type of key (PK or FK)."""
        return self.is_primary_key or self.is_foreign_key
    
    def get_constraints(self) -> List[str]:
        """Get list of constraints applied to this column."""
        constraints = []
        if self.is_primary_key:
            constraints.append("PRIMARY KEY")
        if self.is_foreign_key:
            constraints.append("FOREIGN KEY")
        if self.is_unique:
            constraints.append("UNIQUE")
        if not self.is_nullable:
            constraints.append("NOT NULL")
        if self.is_indexed:
            constraints.append("INDEXED")
        return constraints


@dataclass
class TableProfile:
    """
    Profile information for a single database table.
    
    Contains table metadata, columns, relationships, and structural analysis.
    """
    name: str
    schema: Optional[str] = None
    table_type: str = "BASE TABLE"
    table_comment: Optional[str] = None
    estimated_row_count: int = 0
    columns: List[ColumnProfile] = field(default_factory=list)
    primary_keys: List[str] = field(default_factory=list)
    foreign_keys: List[Dict[str, str]] = field(default_factory=list)
    indexes: List[Dict[str, Any]] = field(default_factory=list)
    sample_data: List[Dict[str, Any]] = field(default_factory=list)
    self_referencing_columns: List[str] = field(default_factory=list)
    potential_fk_candidates: List[Dict[str, Any]] = field(default_factory=list)
    
    def __post_init__(self):
        """Ensure lists are initialized properly."""
        if self.columns is None:
            self.columns = []
        if self.primary_keys is None:
            self.primary_keys = []
        if self.foreign_keys is None:
            self.foreign_keys = []
        if self.indexes is None:
            self.indexes = []
        if self.sample_data is None:
            self.sample_data = []
        if self.self_referencing_columns is None:
            self.self_referencing_columns = []
        if self.potential_fk_candidates is None:
            self.potential_fk_candidates = []
    
    def get_column_by_name(self, column_name: str) -> Optional[ColumnProfile]:
        """Get a column profile by name."""
        for column in self.columns:
            if column.name == column_name:
                return column
        return None
    
    def get_primary_key_columns(self) -> List[ColumnProfile]:
        """Get column profiles for primary key columns."""
        return [col for col in self.columns if col.is_primary_key]
    
    def get_foreign_key_columns(self) -> List[ColumnProfile]:
        """Get column profiles for foreign key columns."""
        return [col for col in self.columns if col.is_foreign_key]
    
    def get_indexed_columns(self) -> List[ColumnProfile]:
        """Get column profiles for indexed columns."""
        return [col for col in self.columns if col.is_indexed]
    
    def has_foreign_keys(self) -> bool:
        """Check if table has any foreign key relationships."""
        return len(self.foreign_keys) > 0
    
    def has_self_references(self) -> bool:
        """Check if table has self-referencing relationships."""
        return len(self.self_referencing_columns) > 0
    
    def get_column_count(self) -> int:
        """Get total number of columns in the table."""
        return len(self.columns)
    
    def get_relationships_summary(self) -> Dict[str, Any]:
        """Get a summary of table relationships."""
        return {
            "foreign_keys_count": len(self.foreign_keys),
            "self_referencing_count": len(self.self_referencing_columns),
            "potential_fk_candidates_count": len(self.potential_fk_candidates),
            "references_other_tables": len(set(fk['referenced_table'] for fk in self.foreign_keys)),
            "is_self_referencing": self.has_self_references()
        }
    
    def get_full_name(self) -> str:
        """Get fully qualified table name including schema if available."""
        if self.schema:
            return f"{self.schema}.{self.name}"
        return self.name


@dataclass
class SchemaProfile:
    """
    Complete database schema profile information.
    
    Contains all tables, relationships, and schema-level analysis results.
    """
    database_name: str
    schema_name: Optional[str] = None
    database_type: str = "unknown"
    profiling_timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    total_tables: int = 0
    total_columns: int = 0
    tables: List[TableProfile] = field(default_factory=list)
    cross_table_relationships: List[Dict[str, Any]] = field(default_factory=list)
    potential_relationships: List[Dict[str, Any]] = field(default_factory=list)
    pattern_summary: Dict[str, int] = field(default_factory=dict)
    
    def __post_init__(self):
        """Ensure lists and dictionaries are initialized properly."""
        if self.tables is None:
            self.tables = []
        if self.cross_table_relationships is None:
            self.cross_table_relationships = []
        if self.potential_relationships is None:
            self.potential_relationships = []
        if self.pattern_summary is None:
            self.pattern_summary = {}
    
    def get_table_by_name(self, table_name: str) -> Optional[TableProfile]:
        """Get a table profile by name."""
        for table in self.tables:
            if table.name == table_name:
                return table
        return None
    
    def get_tables_by_type(self, table_type: str = "BASE TABLE") -> List[TableProfile]:
        """Get tables filtered by type."""
        return [table for table in self.tables if table.table_type == table_type]
    
    def get_tables_with_foreign_keys(self) -> List[TableProfile]:
        """Get tables that have foreign key relationships."""
        return [table for table in self.tables if table.has_foreign_keys()]
    
    def get_self_referencing_tables(self) -> List[TableProfile]:
        """Get tables that have self-referencing relationships."""
        return [table for table in self.tables if table.has_self_references()]
    
    def get_relationship_count(self) -> int:
        """Get total number of explicit foreign key relationships."""
        return len(self.cross_table_relationships)
    
    def get_potential_relationship_count(self) -> int:
        """Get total number of potential relationships identified."""
        return len(self.potential_relationships)
    
    def get_tables_by_column_count(self, min_columns: int = None, max_columns: int = None) -> List[TableProfile]:
        """Get tables filtered by column count range."""
        filtered_tables = []
        for table in self.tables:
            column_count = table.get_column_count()
            if min_columns is not None and column_count < min_columns:
                continue
            if max_columns is not None and column_count > max_columns:
                continue
            filtered_tables.append(table)
        return filtered_tables
    
    def get_pattern_statistics(self) -> Dict[str, Any]:
        """Get statistics about detected patterns."""
        if not self.pattern_summary:
            return {"total_patterns": 0, "unique_patterns": 0, "most_common": None}
        
        total_patterns = sum(self.pattern_summary.values())
        unique_patterns = len(self.pattern_summary)
        most_common = max(self.pattern_summary.items(), key=lambda x: x[1]) if self.pattern_summary else None
        
        return {
            "total_patterns": total_patterns,
            "unique_patterns": unique_patterns,
            "most_common": most_common,
            "pattern_distribution": self.pattern_summary
        }
    
    def get_schema_summary(self) -> Dict[str, Any]:
        """Get a comprehensive summary of the schema."""
        return {
            "database_info": {
                "name": self.database_name,
                "schema": self.schema_name,
                "type": self.database_type,
                "profiled_at": self.profiling_timestamp
            },
            "structure": {
                "total_tables": self.total_tables,
                "total_columns": self.total_columns,
                "avg_columns_per_table": round(self.total_columns / self.total_tables, 2) if self.total_tables > 0 else 0
            },
            "relationships": {
                "explicit_foreign_keys": len(self.cross_table_relationships),
                "potential_relationships": len(self.potential_relationships),
                "self_referencing_tables": len(self.get_self_referencing_tables())
            },
            "patterns": self.get_pattern_statistics()
        }
    
    def get_table_names(self) -> List[str]:
        """Get list of all table names in the schema."""
        return [table.name for table in self.tables]
    
    def has_relationships(self) -> bool:
        """Check if schema has any explicit relationships."""
        return len(self.cross_table_relationships) > 0 