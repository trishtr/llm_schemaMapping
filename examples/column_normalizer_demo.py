#!/usr/bin/env python3
"""
Column Normalizer Demo

This script demonstrates the various normalization functions that convert
minimal column summaries into different views and formats for easier
processing, analysis, and LLM consumption.

Shows:
- Field list view normalization
- Column dictionary view
- Flat structure conversion
- Pattern-based grouping
- Data type categorization
- LLM-optimized format
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from normalizer import (
    ColumnFieldNormalizer,
    normalize_to_field_list,
    normalize_to_column_dict,
    normalize_to_flat_structure,
    normalize_by_patterns,
    normalize_by_data_types,
    normalize_for_llm,
    ViewType
)
import json


def get_sample_column_summary():
    """Get sample minimal column summary for demonstration."""
    return {
        "table": "patients",
        "total_columns": 6,
        "columns": [
            {
                "name": "patient_id",
                "type": "bigint",
                "nullable": False,
                "key_type": "PK",
                "patterns": [],
                "null_pct": 0.0,
                "unique_pct": 100.0
            },
            {
                "name": "provider_id",
                "type": "int",
                "nullable": True,
                "key_type": "FK",
                "patterns": [],
                "null_pct": 5.0,
                "unique_pct": 3.0
            },
            {
                "name": "email_address",
                "type": "varchar",
                "nullable": True,
                "key_type": None,
                "patterns": ["email_address"],
                "null_pct": 10.0,
                "unique_pct": 90.0
            },
            {
                "name": "phone_number",
                "type": "varchar",
                "nullable": True,
                "key_type": None,
                "patterns": ["phone_number"],
                "null_pct": 15.0,
                "unique_pct": 84.0
            },
            {
                "name": "status",
                "type": "varchar",
                "nullable": False,
                "key_type": None,
                "patterns": ["status_field"],
                "null_pct": 0.0,
                "unique_pct": 0.01
            },
            {
                "name": "birth_date",
                "type": "date",
                "nullable": True,
                "key_type": None,
                "patterns": [],
                "null_pct": 2.0,
                "unique_pct": 95.0
            }
        ]
    }


def demonstrate_normalization_views():
    """Demonstrate all normalization views and their outputs."""
    
    print("=" * 80)
    print("COLUMN NORMALIZER DEMONSTRATION")
    print("=" * 80)
    print()
    
    # Get sample data
    column_summary = get_sample_column_summary()
    normalizer = ColumnFieldNormalizer()
    
    print("📊 ORIGINAL MINIMAL COLUMN SUMMARY:")
    print(f"   Table: {column_summary['table']}")
    print(f"   Total Columns: {column_summary['total_columns']}")
    print("   Columns:")
    for col in column_summary['columns']:
        key_info = f" ({col['key_type']})" if col['key_type'] else ""
        pattern_info = f" [{','.join(col['patterns'])}]" if col['patterns'] else ""
        print(f"     {col['name']}: {col['type']}{key_info}{pattern_info}")
    print()
    
    # 1. Field List View
    print("🔧 VIEW 1: FIELD LIST")
    field_list = normalize_to_field_list(column_summary)
    print(f"   View Type: {field_list['view_type']}")
    print(f"   Total Fields: {field_list['total_columns']}")
    print("   Summary:")
    summary = field_list['summary']
    print(f"     Key Fields: {summary['key_fields']}")
    print(f"     Pattern Fields: {summary['pattern_fields']}")
    print(f"     Nullable Fields: {summary['nullable_fields']}")
    print(f"     Avg Quality Score: {summary['avg_quality_score']:.2f}")
    print("   Sample Field:")
    if field_list['fields']:
        sample_field = field_list['fields'][2]  # Email field
        print(f"     Name: {sample_field['name']}")
        print(f"     Data Category: {sample_field['data_category']}")
        print(f"     Quality Score: {sample_field['quality_score']:.2f}")
        print(f"     Is Pattern Field: {sample_field['is_pattern_field']}")
    print()
    
    # 2. Column Dictionary View
    print("🔧 VIEW 2: COLUMN DICTIONARY")
    column_dict = normalize_to_column_dict(column_summary)
    print(f"   View Type: {column_dict['view_type']}")
    print(f"   Column Names: {', '.join(column_dict['metadata']['column_names'])}")
    print("   Sample Column (email_address):")
    if 'email_address' in column_dict['columns']:
        email_col = column_dict['columns']['email_address']
        print(f"     Type: {email_col['type']}")
        print(f"     Data Category: {email_col['data_category']}")
        print(f"     Patterns: {email_col['patterns']}")
        print(f"     Quality Score: {email_col['quality_score']:.2f}")
        print(f"     Statistics: {email_col['statistics']}")
    print()
    
    # 3. Flat Structure View
    print("🔧 VIEW 3: FLAT STRUCTURE")
    flat_structure = normalize_to_flat_structure(column_summary)
    print(f"   View Type: {flat_structure['view_type']}")
    print(f"   Total Columns: {flat_structure['total_columns']}")
    print("   Sample Flat Column:")
    if flat_structure['columns']:
        flat_col = flat_structure['columns'][0]  # First column
        print(f"     Full Identifier: {flat_col['full_identifier']}")
        print(f"     Data Category: {flat_col['data_category']}")
        print(f"     Is Key: {flat_col['is_key']}")
        print(f"     Has Patterns: {flat_col['has_patterns']}")
        print(f"     Patterns List: {flat_col['patterns_list']}")
    print()
    
    # 4. Pattern Groups View
    print("🔧 VIEW 4: PATTERN GROUPS")
    pattern_groups = normalize_by_patterns(column_summary)
    print(f"   View Type: {pattern_groups['view_type']}")
    print("   Pattern Groups:")
    for pattern, fields in pattern_groups['pattern_groups'].items():
        field_names = [f['name'] for f in fields]
        print(f"     {pattern}: {', '.join(field_names)}")
    print(f"   No Pattern Fields: {len(pattern_groups['no_pattern_fields'])}")
    if pattern_groups['no_pattern_fields']:
        no_pattern_names = [f['name'] for f in pattern_groups['no_pattern_fields']]
        print(f"     Fields: {', '.join(no_pattern_names)}")
    print("   Summary:")
    summary = pattern_groups['summary']
    print(f"     Total Patterns: {summary['total_patterns']}")
    print(f"     Fields with Patterns: {summary['fields_with_patterns']}")
    print(f"     Fields without Patterns: {summary['fields_without_patterns']}")
    print()
    
    # 5. Type Groups View
    print("🔧 VIEW 5: DATA TYPE GROUPS")
    type_groups = normalize_by_data_types(column_summary)
    print(f"   View Type: {type_groups['view_type']}")
    print("   By Data Category:")
    for category, fields in type_groups['by_category'].items():
        field_names = [f['name'] for f in fields]
        print(f"     {category}: {', '.join(field_names)}")
    print("   By Specific Type:")
    for data_type, fields in type_groups['by_data_type'].items():
        field_names = [f['name'] for f in fields]
        print(f"     {data_type}: {', '.join(field_names)}")
    print("   Category Distribution:")
    for category, count in type_groups['summary']['category_distribution'].items():
        print(f"     {category}: {count} fields")
    print()
    
    # 6. LLM-Optimized View
    print("🔧 VIEW 6: LLM-OPTIMIZED")
    llm_optimized = normalize_for_llm(column_summary)
    print(f"   View Type: {llm_optimized['view_type']}")
    print("   Compact Fields:")
    for field in llm_optimized['fields']:
        print(f"     {field}")
    print(f"   Keys: {', '.join(llm_optimized['keys'])}")
    print("   Pattern Fields:")
    for pattern_field in llm_optimized['patterns']:
        print(f"     {pattern_field['field']}: {', '.join(pattern_field['patterns'])}")
    print("   Statistics:")
    stats = llm_optimized['stats']
    print(f"     Total: {stats['total']}, Keys: {stats['keys']}, Patterns: {stats['patterns']}")
    print()
    
    # Export sample outputs
    print("💾 EXPORTING NORMALIZATION EXAMPLES:")
    
    # Export field list view
    with open("normalized_field_list.json", "w") as f:
        json.dump(field_list, f, indent=2, default=str)
    print(f"   normalized_field_list.json: {Path('normalized_field_list.json').stat().st_size:,} bytes")
    
    # Export column dictionary view
    with open("normalized_column_dict.json", "w") as f:
        json.dump(column_dict, f, indent=2, default=str)
    print(f"   normalized_column_dict.json: {Path('normalized_column_dict.json').stat().st_size:,} bytes")
    
    # Export flat structure
    with open("normalized_flat_structure.json", "w") as f:
        json.dump(flat_structure, f, indent=2, default=str)
    print(f"   normalized_flat_structure.json: {Path('normalized_flat_structure.json').stat().st_size:,} bytes")
    
    # Export pattern groups
    with open("normalized_pattern_groups.json", "w") as f:
        json.dump(pattern_groups, f, indent=2, default=str)
    print(f"   normalized_pattern_groups.json: {Path('normalized_pattern_groups.json').stat().st_size:,} bytes")
    
    # Export LLM-optimized
    with open("normalized_llm_optimized.json", "w") as f:
        json.dump(llm_optimized, f, indent=2, default=str)
    print(f"   normalized_llm_optimized.json: {Path('normalized_llm_optimized.json').stat().st_size:,} bytes")
    
    print()
    print("🎯 NORMALIZATION VIEW CHARACTERISTICS:")
    print()
    print("   📋 FIELD LIST VIEW:")
    print("      - Array of enriched field objects")
    print("      - Includes quality scores and categorization")
    print("      - Summary statistics for quick overview")
    print("      - Best for: Field-by-field analysis")
    print()
    print("   📚 COLUMN DICTIONARY VIEW:")
    print("      - Key-value mapping by column name")
    print("      - Easy lookup and access by name")
    print("      - Structured metadata organization")
    print("      - Best for: Column-specific operations")
    print()
    print("   📄 FLAT STRUCTURE VIEW:")
    print("      - Single-level objects with full context")
    print("      - Each column is self-contained")
    print("      - Includes full identifiers and flags")
    print("      - Best for: CSV export, tabular analysis")
    print()
    print("   🎯 PATTERN GROUPS VIEW:")
    print("      - Groups columns by detected patterns")
    print("      - Separates pattern vs non-pattern fields")
    print("      - Pattern-centric organization")
    print("      - Best for: Pattern-based schema mapping")
    print()
    print("   📊 TYPE GROUPS VIEW:")
    print("      - Groups by data types and categories")
    print("      - Both specific and broad categorization")
    print("      - Type distribution analysis")
    print("      - Best for: Type-based transformations")
    print()
    print("   🤖 LLM-OPTIMIZED VIEW:")
    print("      - Compact string representations")
    print("      - Minimal token usage")
    print("      - Quick reference sections")
    print("      - Best for: LLM processing and prompts")
    
    print()
    print("💡 USE CASE RECOMMENDATIONS:")
    print()
    print("   🔄 SCHEMA MAPPING:")
    print("      - Use PATTERN GROUPS for pattern-based mapping")
    print("      - Use TYPE GROUPS for type compatibility")
    print("      - Use LLM-OPTIMIZED for AI-assisted mapping")
    print()
    print("   📊 DATA ANALYSIS:")
    print("      - Use FIELD LIST for quality assessment")
    print("      - Use FLAT STRUCTURE for export/reporting")
    print("      - Use COLUMN DICT for programmatic access")
    print()
    print("   🤖 LLM PROCESSING:")
    print("      - Use LLM-OPTIMIZED for token efficiency")
    print("      - Use PATTERN GROUPS for semantic understanding")
    print("      - Use TYPE GROUPS for transformation planning")
    
    print()
    print("=" * 80)
    print("COLUMN NORMALIZER DEMONSTRATION COMPLETED")
    print("=" * 80)


def demonstrate_programmatic_usage():
    """Show programmatic usage examples."""
    
    print("\n" + "=" * 60)
    print("PROGRAMMATIC USAGE EXAMPLES")
    print("=" * 60)
    
    column_summary = get_sample_column_summary()
    normalizer = ColumnFieldNormalizer()
    
    print("\n🔧 USING THE NORMALIZER CLASS:")
    print("```python")
    print("from normalizer import ColumnFieldNormalizer, ViewType")
    print("normalizer = ColumnFieldNormalizer()")
    print("result = normalizer.normalize(column_summary, ViewType.FIELD_LIST)")
    print("```")
    
    print("\n🔧 USING CONVENIENCE FUNCTIONS:")
    print("```python")
    print("from normalizer import normalize_for_llm, normalize_by_patterns")
    print("llm_view = normalize_for_llm(column_summary)")
    print("pattern_view = normalize_by_patterns(column_summary)")
    print("```")
    
    print("\n🔧 CHAINING NORMALIZATIONS:")
    print("```python")
    print("# Start with minimal summary")
    print("field_list = normalize_to_field_list(column_summary)")
    print("# Extract high-quality fields")
    print("quality_fields = [f for f in field_list['fields'] if f['quality_score'] > 0.8]")
    print("```")
    
    # Show actual results
    llm_view = normalize_for_llm(column_summary)
    pattern_view = normalize_by_patterns(column_summary)
    
    print(f"\n📊 LLM VIEW TOKEN COUNT: ~{len(json.dumps(llm_view)) // 4} tokens")
    print(f"📊 PATTERN VIEW GROUPS: {len(pattern_view['pattern_groups'])} patterns found")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    demonstrate_normalization_views()
    demonstrate_programmatic_usage() 