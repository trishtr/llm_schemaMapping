#!/usr/bin/env python3
"""
Schema Chunking Strategies for LLM Processing

This script demonstrates various strategies to handle large database schemas
that exceed LLM token limits for schema mapping tasks.

Key Strategies:
1. Schema Chunking - Split by tables/relationships
2. Compression - Remove redundant/verbose data
3. Filtering - Focus on relevant tables/columns
4. Hierarchical Processing - Multi-level approach
5. Adaptive Detail Levels - Context-aware detail reduction
6. Token-aware Processing - Estimate and manage token usage
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from profiler.schema_models import ColumnProfile, TableProfile, SchemaProfile
from dataclasses import asdict
import json
import re
from typing import List, Dict, Any, Optional, Tuple
from enum import Enum


class DetailLevel(Enum):
    """Different levels of schema detail for LLM processing."""
    MINIMAL = "minimal"          # Just table/column names and types
    ESSENTIAL = "essential"      # + primary keys, foreign keys, basic patterns
    STANDARD = "standard"        # + indexes, constraints, sample data (limited)
    COMPREHENSIVE = "comprehensive"  # Full detail (original format)


class SchemaChunkingStrategy:
    """Strategies for chunking large schemas for LLM processing."""
    
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
        
        # Create base schema info (without tables)
        base_schema = {
            "database_name": schema.database_name,
            "schema_name": schema.schema_name,
            "database_type": schema.database_type,
            "profiling_timestamp": schema.profiling_timestamp,
            "total_tables": schema.total_tables,
            "total_columns": schema.total_columns,
            "chunk_info": {}
        }
        
        # Split tables into chunks
        tables = schema.tables
        for i in range(0, len(tables), tables_per_chunk):
            chunk_tables = tables[i:i + tables_per_chunk]
            
            chunk = base_schema.copy()
            chunk["tables"] = [asdict(table) for table in chunk_tables]
            chunk["chunk_info"] = {
                "chunk_number": i // tables_per_chunk + 1,
                "total_chunks": (len(tables) + tables_per_chunk - 1) // tables_per_chunk,
                "tables_in_chunk": len(chunk_tables),
                "table_names": [t.name for t in chunk_tables]
            }
            
            # Include only relevant relationships for this chunk
            chunk_table_names = set(t.name for t in chunk_tables)
            chunk["cross_table_relationships"] = [
                rel for rel in schema.cross_table_relationships
                if rel.get("from_table") in chunk_table_names or rel.get("to_table") in chunk_table_names
            ]
            
            chunk["potential_relationships"] = [
                rel for rel in schema.potential_relationships
                if rel.get("from_table") in chunk_table_names or rel.get("to_table") in chunk_table_names
            ]
            
            chunks.append(chunk)
        
        return chunks

    def chunk_by_domain(self, schema: SchemaProfile, domain_keywords: Dict[str, List[str]]) -> List[Dict[str, Any]]:
        """Strategy 2: Chunk schema by business domain (e.g., users, orders, products)."""
        chunks = []
        domain_tables = {domain: [] for domain in domain_keywords}
        unassigned_tables = []
        
        # Classify tables by domain
        for table in schema.tables:
            assigned = False
            for domain, keywords in domain_keywords.items():
                if any(keyword.lower() in table.name.lower() for keyword in keywords):
                    domain_tables[domain].append(table)
                    assigned = True
                    break
            
            if not assigned:
                unassigned_tables.append(table)
        
        # Create domain-based chunks
        for domain, tables in domain_tables.items():
            if not tables:
                continue
                
            chunk = {
                "database_name": schema.database_name,
                "schema_name": schema.schema_name,
                "database_type": schema.database_type,
                "domain": domain,
                "tables": [asdict(table) for table in tables],
                "domain_info": {
                    "domain_name": domain,
                    "keywords_matched": domain_keywords[domain],
                    "tables_count": len(tables),
                    "table_names": [t.name for t in tables]
                }
            }
            
            # Include relevant relationships
            chunk_table_names = set(t.name for t in tables)
            chunk["cross_table_relationships"] = [
                rel for rel in schema.cross_table_relationships
                if rel.get("from_table") in chunk_table_names or rel.get("to_table") in chunk_table_names
            ]
            
            chunks.append(chunk)
        
        # Handle unassigned tables
        if unassigned_tables:
            chunk = {
                "database_name": schema.database_name,
                "schema_name": schema.schema_name,
                "database_type": schema.database_type,
                "domain": "unassigned",
                "tables": [asdict(table) for table in unassigned_tables],
                "domain_info": {
                    "domain_name": "unassigned",
                    "tables_count": len(unassigned_tables),
                    "table_names": [t.name for t in unassigned_tables]
                }
            }
            chunks.append(chunk)
        
        return chunks

    def compress_schema(self, schema: SchemaProfile, detail_level: DetailLevel) -> Dict[str, Any]:
        """Strategy 3: Compress schema by reducing detail level."""
        
        if detail_level == DetailLevel.MINIMAL:
            return self._create_minimal_schema(schema)
        elif detail_level == DetailLevel.ESSENTIAL:
            return self._create_essential_schema(schema)
        elif detail_level == DetailLevel.STANDARD:
            return self._create_standard_schema(schema)
        else:
            return asdict(schema)  # Comprehensive - no compression

    def _create_minimal_schema(self, schema: SchemaProfile) -> Dict[str, Any]:
        """Create minimal schema with just names and types."""
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

    def _create_essential_schema(self, schema: SchemaProfile) -> Dict[str, Any]:
        """Create essential schema with keys and basic relationships."""
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
                            "detected_patterns": col.detected_patterns[:2] if col.detected_patterns else []  # Limit patterns
                        } for col in table.columns
                    ],
                    "primary_keys": table.primary_keys,
                    "foreign_keys": table.foreign_keys
                } for table in schema.tables
            ],
            "cross_table_relationships": schema.cross_table_relationships
        }

    def _create_standard_schema(self, schema: SchemaProfile) -> Dict[str, Any]:
        """Create standard schema with limited sample data."""
        return {
            "database_name": schema.database_name,
            "schema_name": schema.schema_name,
            "database_type": schema.database_type,
            "profiling_timestamp": schema.profiling_timestamp,
            "total_tables": schema.total_tables,
            "total_columns": schema.total_columns,
            "tables": [
                {
                    "name": table.name,
                    "schema": table.schema,
                    "table_type": table.table_type,
                    "estimated_row_count": table.estimated_row_count,
                    "total_columns": table.total_columns,
                    "columns": [
                        {
                            "name": col.name,
                            "data_type": col.data_type,
                            "is_nullable": col.is_nullable,
                            "is_primary_key": col.is_primary_key,
                            "is_foreign_key": col.is_foreign_key,
                            "is_unique": col.is_unique,
                            "max_length": col.max_length,
                            "ordinal_position": col.ordinal_position,
                            "detected_patterns": col.detected_patterns,
                            "sample_values": col.sample_values[:3] if col.sample_values else [],  # Limit samples
                            "foreign_key_reference": col.foreign_key_reference
                        } for col in table.columns
                    ],
                    "primary_keys": table.primary_keys,
                    "foreign_keys": table.foreign_keys,
                    "indexes": table.indexes[:5] if table.indexes else [],  # Limit indexes
                    "sample_data": table.sample_data[:2] if table.sample_data else []  # Limit sample rows
                } for table in schema.tables
            ],
            "cross_table_relationships": schema.cross_table_relationships,
            "potential_relationships": schema.potential_relationships,
            "pattern_summary": schema.pattern_summary
        }

    def filter_relevant_tables(self, schema: SchemaProfile, 
                              table_patterns: List[str] = None,
                              column_patterns: List[str] = None,
                              min_row_count: int = 0,
                              exclude_system_tables: bool = True) -> SchemaProfile:
        """Strategy 4: Filter schema to include only relevant tables."""
        
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
            
            # Filter by column patterns (if table has relevant columns)
            if column_patterns:
                has_relevant_columns = any(
                    any(pattern.lower() in col.name.lower() for pattern in column_patterns)
                    for col in table.columns
                )
                if not has_relevant_columns:
                    continue
            
            filtered_tables.append(table)
        
        # Create filtered schema
        filtered_schema = SchemaProfile(
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
        
        return filtered_schema

    def _is_system_table(self, table_name: str) -> bool:
        """Check if table is a system/metadata table."""
        system_patterns = [
            'information_schema', 'sys_', 'system_', 'pg_', 'mysql_',
            'performance_schema', '__', 'temp_', 'tmp_', 'log_', 'audit_'
        ]
        return any(pattern in table_name.lower() for pattern in system_patterns)

    def create_hierarchical_summary(self, schema: SchemaProfile) -> Dict[str, Any]:
        """Strategy 5: Create hierarchical summary for multi-pass processing."""
        
        # Level 1: High-level overview
        overview = {
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
        
        return overview

    def _infer_business_domain(self, table_name: str) -> str:
        """Infer business domain from table name."""
        domain_keywords = {
            "user_management": ["user", "account", "profile", "auth", "login"],
            "product_catalog": ["product", "item", "catalog", "inventory", "sku"],
            "order_management": ["order", "purchase", "transaction", "payment", "invoice"],
            "content_management": ["content", "article", "post", "page", "media"],
            "analytics": ["log", "event", "metric", "stat", "report"],
            "healthcare": ["patient", "provider", "appointment", "medical", "diagnosis"],
            "financial": ["account", "transaction", "payment", "billing", "finance"],
            "system": ["config", "setting", "system", "admin", "meta"]
        }
        
        table_lower = table_name.lower()
        for domain, keywords in domain_keywords.items():
            if any(keyword in table_lower for keyword in keywords):
                return domain
        
        return "general"

    def create_token_aware_chunks(self, schema: SchemaProfile, max_tokens: int = 8000) -> List[Dict[str, Any]]:
        """Strategy 6: Create chunks based on estimated token usage."""
        chunks = []
        current_chunk = {
            "database_name": schema.database_name,
            "schema_name": schema.schema_name,
            "database_type": schema.database_type,
            "tables": [],
            "cross_table_relationships": [],
            "potential_relationships": []
        }
        
        current_tokens = self.estimate_json_tokens(current_chunk)
        
        for table in schema.tables:
            table_dict = asdict(table)
            table_tokens = self.estimate_json_tokens(table_dict)
            
            # If adding this table would exceed limit, start new chunk
            if current_tokens + table_tokens > max_tokens and current_chunk["tables"]:
                chunks.append(current_chunk.copy())
                
                # Start new chunk
                current_chunk = {
                    "database_name": schema.database_name,
                    "schema_name": schema.schema_name,
                    "database_type": schema.database_type,
                    "tables": [table_dict],
                    "cross_table_relationships": [],
                    "potential_relationships": []
                }
                current_tokens = self.estimate_json_tokens(current_chunk)
            else:
                current_chunk["tables"].append(table_dict)
                current_tokens += table_tokens
        
        # Add final chunk if it has tables
        if current_chunk["tables"]:
            chunks.append(current_chunk)
        
        # Add relevant relationships to each chunk
        for chunk in chunks:
            chunk_table_names = set(t["name"] for t in chunk["tables"])
            
            chunk["cross_table_relationships"] = [
                rel for rel in schema.cross_table_relationships
                if rel.get("from_table") in chunk_table_names or rel.get("to_table") in chunk_table_names
            ]
            
            chunk["potential_relationships"] = [
                rel for rel in schema.potential_relationships
                if rel.get("from_table") in chunk_table_names or rel.get("to_table") in chunk_table_names
            ]
        
        return chunks


def demonstrate_chunking_strategies():
    """Demonstrate various chunking strategies."""
    
    print("=" * 80)
    print("SCHEMA CHUNKING STRATEGIES FOR LLM PROCESSING")
    print("=" * 80)
    print()
    
    # Create a sample large schema (simulated)
    large_schema = create_sample_large_schema()
    
    chunker = SchemaChunkingStrategy(max_tokens_per_chunk=8000)
    
    print(f"📊 ORIGINAL SCHEMA: {large_schema.database_name}")
    print(f"   Total Tables: {large_schema.total_tables}")
    print(f"   Total Columns: {large_schema.total_columns}")
    
    original_tokens = chunker.estimate_json_tokens(asdict(large_schema))
    print(f"   Estimated Tokens: {original_tokens:,}")
    print()
    
    # Strategy 1: Chunk by tables
    print("🔧 STRATEGY 1: CHUNK BY TABLES")
    table_chunks = chunker.chunk_by_tables(large_schema, tables_per_chunk=3)
    print(f"   Created {len(table_chunks)} chunks")
    for i, chunk in enumerate(table_chunks, 1):
        tokens = chunker.estimate_json_tokens(chunk)
        print(f"   Chunk {i}: {len(chunk['tables'])} tables, ~{tokens:,} tokens")
    print()
    
    # Strategy 2: Chunk by domain
    print("🔧 STRATEGY 2: CHUNK BY BUSINESS DOMAIN")
    domain_keywords = {
        "healthcare": ["patient", "provider", "medical", "clinic"],
        "user_management": ["user", "account", "profile", "auth"],
        "content": ["content", "article", "post", "media"]
    }
    domain_chunks = chunker.chunk_by_domain(large_schema, domain_keywords)
    print(f"   Created {len(domain_chunks)} domain chunks")
    for chunk in domain_chunks:
        tokens = chunker.estimate_json_tokens(chunk)
        domain = chunk.get("domain", "unknown")
        print(f"   Domain '{domain}': {len(chunk['tables'])} tables, ~{tokens:,} tokens")
    print()
    
    # Strategy 3: Compression by detail level
    print("🔧 STRATEGY 3: COMPRESSION BY DETAIL LEVEL")
    for level in DetailLevel:
        compressed = chunker.compress_schema(large_schema, level)
        tokens = chunker.estimate_json_tokens(compressed)
        reduction = (1 - tokens / original_tokens) * 100
        print(f"   {level.value.upper()}: ~{tokens:,} tokens ({reduction:.1f}% reduction)")
    print()
    
    # Strategy 4: Filtering
    print("🔧 STRATEGY 4: RELEVANCE FILTERING")
    filtered_schema = chunker.filter_relevant_tables(
        large_schema,
        table_patterns=["patient", "provider", "user"],
        min_row_count=1000,
        exclude_system_tables=True
    )
    filtered_tokens = chunker.estimate_json_tokens(asdict(filtered_schema))
    reduction = (1 - filtered_tokens / original_tokens) * 100
    print(f"   Filtered to {filtered_schema.total_tables} relevant tables")
    print(f"   Tokens: ~{filtered_tokens:,} ({reduction:.1f}% reduction)")
    print()
    
    # Strategy 5: Hierarchical summary
    print("🔧 STRATEGY 5: HIERARCHICAL SUMMARY")
    summary = chunker.create_hierarchical_summary(large_schema)
    summary_tokens = chunker.estimate_json_tokens(summary)
    reduction = (1 - summary_tokens / original_tokens) * 100
    print(f"   High-level overview: ~{summary_tokens:,} tokens ({reduction:.1f}% reduction)")
    print(f"   Can be used for initial analysis before detailed processing")
    print()
    
    # Strategy 6: Token-aware chunking
    print("🔧 STRATEGY 6: TOKEN-AWARE CHUNKING")
    token_chunks = chunker.create_token_aware_chunks(large_schema, max_tokens=6000)
    print(f"   Created {len(token_chunks)} token-optimized chunks")
    for i, chunk in enumerate(token_chunks, 1):
        tokens = chunker.estimate_json_tokens(chunk)
        print(f"   Chunk {i}: {len(chunk['tables'])} tables, ~{tokens:,} tokens")
    print()
    
    # Export examples
    print("💾 EXPORTING EXAMPLES:")
    
    # Export minimal version
    minimal = chunker.compress_schema(large_schema, DetailLevel.MINIMAL)
    with open("schema_minimal.json", "w") as f:
        json.dump(minimal, f, indent=2, default=str)
    print(f"   schema_minimal.json: {Path('schema_minimal.json').stat().st_size:,} bytes")
    
    # Export first table chunk
    with open("schema_chunk_1.json", "w") as f:
        json.dump(table_chunks[0], f, indent=2, default=str)
    print(f"   schema_chunk_1.json: {Path('schema_chunk_1.json').stat().st_size:,} bytes")
    
    # Export hierarchical summary
    with open("schema_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"   schema_summary.json: {Path('schema_summary.json').stat().st_size:,} bytes")
    
    print()
    print("🎯 RECOMMENDATIONS FOR LLM PROCESSING:")
    print("   1. Start with HIERARCHICAL SUMMARY for overview")
    print("   2. Use DOMAIN CHUNKING for business logic understanding")
    print("   3. Apply RELEVANCE FILTERING for focused analysis")
    print("   4. Use MINIMAL/ESSENTIAL detail levels for large schemas")
    print("   5. Process chunks sequentially with context carryover")
    print("   6. Combine strategies (e.g., filter + compress + chunk)")
    print()
    
    print("=" * 80)
    print("SCHEMA CHUNKING STRATEGIES DEMONSTRATION COMPLETED")
    print("=" * 80)


def create_sample_large_schema() -> SchemaProfile:
    """Create a sample large schema for demonstration."""
    
    # Simulate a large e-commerce + healthcare hybrid schema
    tables = []
    
    # Healthcare tables
    for i in range(1, 4):
        columns = [
            ColumnProfile(name=f"patient_id_{i}", data_type="bigint", is_nullable=False, ordinal_position=1, is_primary_key=True),
            ColumnProfile(name=f"provider_id_{i}", data_type="int", is_nullable=True, ordinal_position=2, is_foreign_key=True),
            ColumnProfile(name=f"department_id_{i}", data_type="int", is_nullable=True, ordinal_position=3),
            ColumnProfile(name=f"email_{i}", data_type="varchar", is_nullable=True, ordinal_position=4, detected_patterns=["email_address"]),
            ColumnProfile(name=f"phone_{i}", data_type="varchar", is_nullable=True, ordinal_position=5, detected_patterns=["phone_number"]),
        ]
        
        table = TableProfile(
            name=f"patients_{i}",
            estimated_row_count=50000 + i * 10000,
            total_columns=len(columns),
            columns=columns,
            primary_keys=[f"patient_id_{i}"],
            foreign_keys=[{
                "column_name": f"provider_id_{i}",
                "referenced_table": f"providers_{i}",
                "referenced_column": "provider_id",
                "constraint_name": f"fk_patients_provider_{i}"
            }]
        )
        tables.append(table)
    
    # User management tables
    for i in range(1, 3):
        columns = [
            ColumnProfile(name=f"user_id_{i}", data_type="int", is_nullable=False, ordinal_position=1, is_primary_key=True),
            ColumnProfile(name=f"username_{i}", data_type="varchar", is_nullable=False, ordinal_position=2, is_unique=True),
            ColumnProfile(name=f"email_{i}", data_type="varchar", is_nullable=True, ordinal_position=3, detected_patterns=["email_address"]),
            ColumnProfile(name=f"profile_id_{i}", data_type="int", is_nullable=True, ordinal_position=4, is_foreign_key=True),
        ]
        
        table = TableProfile(
            name=f"users_{i}",
            estimated_row_count=25000 + i * 5000,
            total_columns=len(columns),
            columns=columns,
            primary_keys=[f"user_id_{i}"]
        )
        tables.append(table)
    
    # Content tables
    for i in range(1, 3):
        columns = [
            ColumnProfile(f"content_id_{i}", "bigint", ordinal_position=1, is_primary_key=True),
            ColumnProfile(f"title_{i}", "varchar", ordinal_position=2),
            ColumnProfile(f"author_id_{i}", "int", ordinal_position=3, is_foreign_key=True),
            ColumnProfile(f"status_{i}", "varchar", ordinal_position=4, detected_patterns=["status_field"]),
        ]
        
        table = TableProfile(
            name=f"articles_{i}",
            estimated_row_count=15000 + i * 3000,
            total_columns=len(columns),
            columns=columns,
            primary_keys=[f"content_id_{i}"]
        )
        tables.append(table)
    
    # System tables (should be filtered out)
    system_columns = [
        ColumnProfile("sys_id", "int", ordinal_position=1, is_primary_key=True),
        ColumnProfile("config_key", "varchar", ordinal_position=2),
        ColumnProfile("config_value", "text", ordinal_position=3),
    ]
    
    system_table = TableProfile(
        name="sys_config",
        estimated_row_count=500,
        total_columns=len(system_columns),
        columns=system_columns,
        primary_keys=["sys_id"]
    )
    tables.append(system_table)
    
    # Create comprehensive schema
    schema = SchemaProfile(
        database_name="large_enterprise_db",
        schema_name="public",
        database_type="postgresql",
        profiling_timestamp="2025-08-21T17:00:00",
        total_tables=len(tables),
        total_columns=sum(len(t.columns) for t in tables),
        tables=tables,
        cross_table_relationships=[
            {
                "type": "foreign_key",
                "from_table": "patients_1",
                "from_column": "provider_id_1",
                "to_table": "providers_1",
                "to_column": "provider_id",
                "constraint_name": "fk_patients_provider_1"
            }
        ],
        potential_relationships=[
            {
                "type": "potential_foreign_key",
                "from_table": "users_1",
                "from_column": "profile_id_1",
                "to_table": "user_profiles",
                "to_column": "profile_id",
                "confidence": "high",
                "reason": "Column name pattern match"
            }
        ],
        pattern_summary={
            "email_address": 5,
            "phone_number": 3,
            "status_field": 2
        }
    )
    
    return schema


if __name__ == "__main__":
    demonstrate_chunking_strategies() 