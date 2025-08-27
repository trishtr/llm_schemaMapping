#!/usr/bin/env python3
"""
Schema Chunking Strategies for LLM Processing

This script demonstrates key strategies to handle large database schemas
that exceed LLM token limits for schema mapping tasks.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from profiler.schema_models import ColumnProfile, TableProfile, SchemaProfile
from dataclasses import asdict
import json
from typing import List, Dict, Any
from enum import Enum


class DetailLevel(Enum):
    """Different levels of schema detail for LLM processing."""
    MINIMAL = "minimal"          # Just table/column names and types
    ESSENTIAL = "essential"      # + primary keys, foreign keys, basic patterns
    STANDARD = "standard"        # + indexes, constraints, sample data (limited)
    COMPREHENSIVE = "comprehensive"  # Full detail (original format)


class SchemaProcessor:
    """Processes schemas for LLM consumption with various strategies."""
    
    def __init__(self, max_tokens_per_chunk: int = 8000):
        self.max_tokens_per_chunk = max_tokens_per_chunk
        
    def estimate_tokens(self, text: str) -> int:
        """Rough token estimation (1 token ≈ 4 characters for English)."""
        return len(text) // 4
    
    def estimate_json_tokens(self, data: Dict[str, Any]) -> int:
        """Estimate tokens for JSON data."""
        json_str = json.dumps(data, separators=(',', ':'))
        return self.estimate_tokens(json_str)

    def chunk_by_tables(self, schema: SchemaProfile, tables_per_chunk: int = 5) -> List[Dict[str, Any]]:
        """Strategy 1: Chunk schema by grouping tables."""
        chunks = []
        tables = schema.tables
        
        for i in range(0, len(tables), tables_per_chunk):
            chunk_tables = tables[i:i + tables_per_chunk]
            
            chunk = {
                "database_name": schema.database_name,
                "schema_name": schema.schema_name,
                "database_type": schema.database_type,
                "chunk_info": {
                    "chunk_number": i // tables_per_chunk + 1,
                    "total_chunks": (len(tables) + tables_per_chunk - 1) // tables_per_chunk,
                    "tables_in_chunk": len(chunk_tables),
                    "table_names": [t.name for t in chunk_tables]
                },
                "tables": [asdict(table) for table in chunk_tables]
            }
            
            # Include only relevant relationships for this chunk
            chunk_table_names = set(t.name for t in chunk_tables)
            chunk["cross_table_relationships"] = [
                rel for rel in schema.cross_table_relationships
                if rel.get("from_table") in chunk_table_names or rel.get("to_table") in chunk_table_names
            ]
            
            chunks.append(chunk)
        
        return chunks

    def compress_schema(self, schema: SchemaProfile, detail_level: DetailLevel) -> Dict[str, Any]:
        """Strategy 2: Compress schema by reducing detail level."""
        
        if detail_level == DetailLevel.MINIMAL:
            return {
                "database_name": schema.database_name,
                "database_type": schema.database_type,
                "total_tables": schema.total_tables,
                "total_columns": schema.total_columns,
                "tables": [
                    {
                        "name": table.name,
                        "estimated_row_count": table.estimated_row_count,
                        "total_columns": table.total_columns,
                        "columns": [
                            {
                                "name": col.name,
                                "data_type": col.data_type,
                                "is_nullable": col.is_nullable,
                                "ordinal_position": col.ordinal_position
                            } for col in table.columns
                        ]
                    } for table in schema.tables
                ]
            }
        
        elif detail_level == DetailLevel.ESSENTIAL:
            return {
                "database_name": schema.database_name,
                "database_type": schema.database_type,
                "total_tables": schema.total_tables,
                "total_columns": schema.total_columns,
                "tables": [
                    {
                        "name": table.name,
                        "estimated_row_count": table.estimated_row_count,
                        "total_columns": table.total_columns,
                        "columns": [
                            {
                                "name": col.name,
                                "data_type": col.data_type,
                                "is_nullable": col.is_nullable,
                                "is_primary_key": col.is_primary_key,
                                "is_foreign_key": col.is_foreign_key,
                                "ordinal_position": col.ordinal_position,
                                "foreign_key_reference": col.foreign_key_reference,
                                "detected_patterns": col.detected_patterns[:2] if col.detected_patterns else []
                            } for col in table.columns
                        ],
                        "primary_keys": table.primary_keys,
                        "foreign_keys": table.foreign_keys
                    } for table in schema.tables
                ],
                "cross_table_relationships": schema.cross_table_relationships
            }
        
        else:  # STANDARD or COMPREHENSIVE
            return asdict(schema)

    def filter_relevant_tables(self, schema: SchemaProfile, 
                              table_patterns: List[str] = None,
                              min_row_count: int = 0,
                              exclude_system_tables: bool = True) -> SchemaProfile:
        """Strategy 3: Filter schema to include only relevant tables."""
        
        filtered_tables = []
        
        for table in schema.tables:
            # Skip system tables if requested
            if exclude_system_tables and self._is_system_table(table.name):
                continue
            
            # Filter by row count
            if table.estimated_row_count < min_row_count:
                continue
            
            # Filter by table name patterns
            if table_patterns and not any(pattern.lower() in table.name.lower() for pattern in table_patterns):
                continue
            
            filtered_tables.append(table)
        
        # Create filtered schema
        return SchemaProfile(
            database_name=schema.database_name,
            schema_name=schema.schema_name,
            database_type=schema.database_type,
            profiling_timestamp=schema.profiling_timestamp,
            total_tables=len(filtered_tables),
            total_columns=sum(len(t.columns) for t in filtered_tables),
            tables=filtered_tables,
            cross_table_relationships=[
                rel for rel in schema.cross_table_relationships
                if any(rel.get("from_table") == t.name for t in filtered_tables) or
                   any(rel.get("to_table") == t.name for t in filtered_tables)
            ],
            potential_relationships=[
                rel for rel in schema.potential_relationships
                if any(rel.get("from_table") == t.name for t in filtered_tables) or
                   any(rel.get("to_table") == t.name for t in filtered_tables)
            ],
            pattern_summary=schema.pattern_summary
        )

    def _is_system_table(self, table_name: str) -> bool:
        """Check if table is a system/metadata table."""
        system_patterns = [
            'information_schema', 'sys_', 'system_', 'pg_', 'mysql_',
            'performance_schema', '__', 'temp_', 'tmp_', 'log_', 'audit_'
        ]
        return any(pattern in table_name.lower() for pattern in system_patterns)

    def create_hierarchical_summary(self, schema: SchemaProfile) -> Dict[str, Any]:
        """Strategy 4: Create hierarchical summary for multi-pass processing."""
        
        return {
            "database_overview": {
                "database_name": schema.database_name,
                "database_type": schema.database_type,
                "total_tables": schema.total_tables,
                "total_columns": schema.total_columns,
                "profiling_timestamp": schema.profiling_timestamp
            },
            "table_summary": [
                {
                    "name": table.name,
                    "row_count": table.estimated_row_count,
                    "column_count": table.total_columns,
                    "has_primary_key": len(table.primary_keys) > 0,
                    "has_foreign_keys": len(table.foreign_keys) > 0,
                    "detected_patterns_count": sum(len(col.detected_patterns) for col in table.columns),
                    "business_domain": self._infer_business_domain(table.name)
                } for table in schema.tables
            ],
            "relationship_summary": {
                "total_foreign_keys": len(schema.cross_table_relationships),
                "total_potential_relationships": len(schema.potential_relationships),
                "self_referencing_tables": len([t for t in schema.tables if t.self_referencing_columns])
            },
            "pattern_summary": schema.pattern_summary
        }

    def _infer_business_domain(self, table_name: str) -> str:
        """Infer business domain from table name."""
        domain_keywords = {
            "user_management": ["user", "account", "profile", "auth", "login"],
            "healthcare": ["patient", "provider", "appointment", "medical", "diagnosis"],
            "financial": ["account", "transaction", "payment", "billing", "finance"],
            "content": ["content", "article", "post", "page", "media"],
            "system": ["config", "setting", "system", "admin", "meta"]
        }
        
        table_lower = table_name.lower()
        for domain, keywords in domain_keywords.items():
            if any(keyword in table_lower for keyword in keywords):
                return domain
        
        return "general"


def create_sample_schema() -> SchemaProfile:
    """Create a sample schema for demonstration."""
    
    # Create sample tables with proper ColumnProfile initialization
    patients_table = TableProfile(
        name="patients",
        schema="public",
        estimated_row_count=50000,
        total_columns=4,
        columns=[
            ColumnProfile(
                name="patient_id",
                data_type="bigint",
                is_nullable=False,
                is_primary_key=True,
                ordinal_position=1,
                sample_values=[100001, 100002, 100003, 100004, 100005]
            ),
            ColumnProfile(
                name="provider_id",
                data_type="int",
                is_nullable=True,
                is_foreign_key=True,
                ordinal_position=2,
                sample_values=[1001, 1002, 1003, 1001, 1004],
                foreign_key_reference={
                    "referenced_table": "providers",
                    "referenced_column": "provider_id",
                    "constraint_name": "fk_patients_provider"
                }
            ),
            ColumnProfile(
                name="email",
                data_type="varchar",
                is_nullable=True,
                ordinal_position=3,
                detected_patterns=["email_address"],
                sample_values=["john@example.com", "jane@test.org", "bob@clinic.net"]
            ),
            ColumnProfile(
                name="phone",
                data_type="varchar",
                is_nullable=True,
                ordinal_position=4,
                detected_patterns=["phone_number"],
                sample_values=["555-123-4567", "555-987-6543", "555-456-7890"]
            )
        ],
        primary_keys=["patient_id"],
        foreign_keys=[{
            "column_name": "provider_id",
            "referenced_table": "providers",
            "referenced_column": "provider_id",
            "constraint_name": "fk_patients_provider"
        }]
    )
    
    providers_table = TableProfile(
        name="providers",
        schema="public",
        estimated_row_count=1500,
        total_columns=3,
        columns=[
            ColumnProfile(
                name="provider_id",
                data_type="int",
                is_nullable=False,
                is_primary_key=True,
                ordinal_position=1,
                sample_values=[1001, 1002, 1003, 1004, 1005]
            ),
            ColumnProfile(
                name="npi",
                data_type="varchar",
                is_nullable=False,
                is_unique=True,
                ordinal_position=2,
                detected_patterns=["npi_identifier"],
                sample_values=["1234567890", "9876543210", "5555666677"]
            ),
            ColumnProfile(
                name="department_id",
                data_type="int",
                is_nullable=True,
                ordinal_position=3,
                sample_values=[101, 102, 103, 101, 104]
            )
        ],
        primary_keys=["provider_id"]
    )
    
    users_table = TableProfile(
        name="users",
        schema="public",
        estimated_row_count=25000,
        total_columns=3,
        columns=[
            ColumnProfile(
                name="user_id",
                data_type="int",
                is_nullable=False,
                is_primary_key=True,
                ordinal_position=1,
                sample_values=[2001, 2002, 2003, 2004, 2005]
            ),
            ColumnProfile(
                name="username",
                data_type="varchar",
                is_nullable=False,
                is_unique=True,
                ordinal_position=2,
                sample_values=["john_doe", "jane_smith", "bob_wilson"]
            ),
            ColumnProfile(
                name="email",
                data_type="varchar",
                is_nullable=True,
                ordinal_position=3,
                detected_patterns=["email_address"],
                sample_values=["john@example.com", "jane@test.org", "bob@clinic.net"]
            )
        ],
        primary_keys=["user_id"]
    )
    
    # System table (should be filtered out)
    sys_config_table = TableProfile(
        name="sys_config",
        schema="public",
        estimated_row_count=500,
        total_columns=3,
        columns=[
            ColumnProfile(
                name="config_id",
                data_type="int",
                is_nullable=False,
                is_primary_key=True,
                ordinal_position=1,
                sample_values=[1, 2, 3, 4, 5]
            ),
            ColumnProfile(
                name="config_key",
                data_type="varchar",
                is_nullable=False,
                ordinal_position=2,
                sample_values=["app_name", "version", "debug_mode"]
            ),
            ColumnProfile(
                name="config_value",
                data_type="text",
                is_nullable=True,
                ordinal_position=3,
                sample_values=["MyApp", "1.0.0", "false"]
            )
        ],
        primary_keys=["config_id"]
    )
    
    return SchemaProfile(
        database_name="enterprise_healthcare_db",
        schema_name="public",
        database_type="postgresql",
        profiling_timestamp="2025-08-21T17:15:00",
        total_tables=4,
        total_columns=13,
        tables=[patients_table, providers_table, users_table, sys_config_table],
        cross_table_relationships=[
            {
                "type": "foreign_key",
                "from_table": "patients",
                "from_column": "provider_id",
                "to_table": "providers",
                "to_column": "provider_id",
                "constraint_name": "fk_patients_provider"
            }
        ],
        potential_relationships=[
            {
                "type": "potential_foreign_key",
                "from_table": "providers",
                "from_column": "department_id",
                "to_table": "departments",
                "to_column": "dept_id",
                "confidence": "high",
                "reason": "Column name pattern match"
            }
        ],
        pattern_summary={
            "email_address": 2,
            "phone_number": 1,
            "npi_identifier": 1
        }
    )


def demonstrate_strategies():
    """Demonstrate schema processing strategies."""
    
    print("=" * 80)
    print("SCHEMA PROCESSING STRATEGIES FOR LLM CONSUMPTION")
    print("=" * 80)
    print()
    
    # Create sample schema
    schema = create_sample_schema()
    processor = SchemaProcessor(max_tokens_per_chunk=6000)
    
    print(f"📊 ORIGINAL SCHEMA: {schema.database_name}")
    print(f"   Total Tables: {schema.total_tables}")
    print(f"   Total Columns: {schema.total_columns}")
    
    original_tokens = processor.estimate_json_tokens(asdict(schema))
    print(f"   Estimated Tokens: {original_tokens:,}")
    print()
    
    # Strategy 1: Table-based chunking
    print("🔧 STRATEGY 1: TABLE-BASED CHUNKING")
    table_chunks = processor.chunk_by_tables(schema, tables_per_chunk=2)
    print(f"   Created {len(table_chunks)} chunks")
    for i, chunk in enumerate(table_chunks, 1):
        tokens = processor.estimate_json_tokens(chunk)
        print(f"   Chunk {i}: {len(chunk['tables'])} tables, ~{tokens:,} tokens")
        print(f"      Tables: {', '.join(chunk['chunk_info']['table_names'])}")
    print()
    
    # Strategy 2: Compression by detail level
    print("🔧 STRATEGY 2: DETAIL LEVEL COMPRESSION")
    for level in [DetailLevel.MINIMAL, DetailLevel.ESSENTIAL, DetailLevel.STANDARD]:
        compressed = processor.compress_schema(schema, level)
        tokens = processor.estimate_json_tokens(compressed)
        reduction = (1 - tokens / original_tokens) * 100
        print(f"   {level.value.upper()}: ~{tokens:,} tokens ({reduction:.1f}% reduction)")
    print()
    
    # Strategy 3: Relevance filtering
    print("🔧 STRATEGY 3: RELEVANCE FILTERING")
    
    # Filter by table patterns
    filtered_schema = processor.filter_relevant_tables(
        schema,
        table_patterns=["patient", "provider"],
        exclude_system_tables=True
    )
    filtered_tokens = processor.estimate_json_tokens(asdict(filtered_schema))
    reduction = (1 - filtered_tokens / original_tokens) * 100
    print(f"   Healthcare tables only: {filtered_schema.total_tables} tables")
    print(f"   Tokens: ~{filtered_tokens:,} ({reduction:.1f}% reduction)")
    
    # Filter by row count
    large_tables_schema = processor.filter_relevant_tables(
        schema,
        min_row_count=10000,
        exclude_system_tables=True
    )
    large_tokens = processor.estimate_json_tokens(asdict(large_tables_schema))
    reduction = (1 - large_tokens / original_tokens) * 100
    print(f"   Large tables only (>10K rows): {large_tables_schema.total_tables} tables")
    print(f"   Tokens: ~{large_tokens:,} ({reduction:.1f}% reduction)")
    print()
    
    # Strategy 4: Hierarchical summary
    print("🔧 STRATEGY 4: HIERARCHICAL SUMMARY")
    summary = processor.create_hierarchical_summary(schema)
    summary_tokens = processor.estimate_json_tokens(summary)
    reduction = (1 - summary_tokens / original_tokens) * 100
    print(f"   High-level overview: ~{summary_tokens:,} tokens ({reduction:.1f}% reduction)")
    print(f"   Use for initial analysis before detailed processing")
    print()
    
    # Export examples
    print("💾 EXPORTING STRATEGIES EXAMPLES:")
    
    # Export minimal version
    minimal = processor.compress_schema(schema, DetailLevel.MINIMAL)
    with open("schema_minimal_example.json", "w") as f:
        json.dump(minimal, f, indent=2, default=str)
    print(f"   schema_minimal_example.json: {Path('schema_minimal_example.json').stat().st_size:,} bytes")
    
    # Export first chunk
    with open("schema_chunk_example.json", "w") as f:
        json.dump(table_chunks[0], f, indent=2, default=str)
    print(f"   schema_chunk_example.json: {Path('schema_chunk_example.json').stat().st_size:,} bytes")
    
    # Export filtered schema
    with open("schema_filtered_example.json", "w") as f:
        json.dump(asdict(filtered_schema), f, indent=2, default=str)
    print(f"   schema_filtered_example.json: {Path('schema_filtered_example.json').stat().st_size:,} bytes")
    
    # Export hierarchical summary
    with open("schema_summary_example.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"   schema_summary_example.json: {Path('schema_summary_example.json').stat().st_size:,} bytes")
    
    print()
    print("🎯 RECOMMENDED APPROACH FOR LLM SCHEMA MAPPING:")
    print()
    print("   1. 📋 START WITH HIERARCHICAL SUMMARY")
    print("      - Get high-level understanding of schema structure")
    print("      - Identify key business domains and relationships")
    print("      - Plan detailed analysis strategy")
    print()
    print("   2. 🎯 APPLY RELEVANCE FILTERING")
    print("      - Focus on tables relevant to mapping task")
    print("      - Exclude system/metadata tables")
    print("      - Filter by business domain or table patterns")
    print()
    print("   3. 📦 USE APPROPRIATE DETAIL LEVEL")
    print("      - MINIMAL: For initial structure understanding")
    print("      - ESSENTIAL: For relationship mapping")
    print("      - STANDARD: For detailed field mapping")
    print()
    print("   4. 🔄 CHUNK IF STILL TOO LARGE")
    print("      - Split by business domains or table groups")
    print("      - Process chunks sequentially")
    print("      - Maintain relationship context across chunks")
    print()
    print("   5. 🔗 COMBINE STRATEGIES")
    print("      - Filter → Compress → Chunk")
    print("      - Use different strategies for different mapping phases")
    print("      - Adapt based on LLM token limits and capabilities")
    
    print()
    print("=" * 80)
    print("SCHEMA PROCESSING STRATEGIES DEMONSTRATION COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    demonstrate_strategies() 