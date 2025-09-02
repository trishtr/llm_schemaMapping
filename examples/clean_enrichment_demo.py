#!/usr/bin/env python3
"""
Clean Features Enrichment Demo

This script demonstrates clean enrichment of minimal column summaries with only
key phrases and entity types, removing statistical noise for solid embeddings.

Shows:
- Clean key phrase extraction
- Entity type classification
- Minimal, focused output for embeddings
- No statistical percentages or pattern noise
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from features_enrichment.clean_column_enricher import CleanColumnEnricher, enrich_clean_column_summary
import json


def get_clean_minimal_data():
    """Get clean minimal column data without statistical noise."""
    return {
        "table": "patients",
        "total_columns": 6,
        "columns": [
            {
                "name": "patient_id",
                "type": "bigint",
                "nullable": False,
                "key_type": "PK"
            },
            {
                "name": "provider_id",
                "type": "int",
                "nullable": True,
                "key_type": "FK"
            },
            {
                "name": "email_address",
                "type": "varchar",
                "nullable": True,
                "key_type": None
            },
            {
                "name": "phone_number",
                "type": "varchar",
                "nullable": True,
                "key_type": None
            },
            {
                "name": "status",
                "type": "varchar",
                "nullable": False,
                "key_type": None
            },
            {
                "name": "birth_date",
                "type": "date",
                "nullable": True,
                "key_type": None
            }
        ]
    }


def demonstrate_clean_enrichment():
    """Demonstrate clean enrichment process."""
    
    print("=" * 80)
    print("CLEAN FEATURES ENRICHMENT DEMONSTRATION")
    print("=" * 80)
    print()
    
    # Get clean minimal data
    minimal_data = get_clean_minimal_data()
    
    print("📊 CLEAN MINIMAL COLUMN DATA:")
    print(f"   Table: {minimal_data['table']}")
    print(f"   Total Columns: {minimal_data['total_columns']}")
    print("   Columns (core info only):")
    for col in minimal_data['columns']:
        key_info = f" ({col['key_type']})" if col['key_type'] else ""
        nullable_info = " NOT NULL" if not col['nullable'] else ""
        print(f"     {col['name']}: {col['type']}{key_info}{nullable_info}")
    print()
    
    # Initialize clean enricher
    try:
        enricher = CleanColumnEnricher()
        print("🔧 CLEAN ENRICHMENT ENGINE:")
        print("   ✅ Focus on semantic enrichment only")
        print("   ✅ No statistical noise")
        print("   ✅ No pattern dependencies")
        print("   ✅ Clean embedding-ready output")
        print()
        
        # Enrich the data
        enriched_data = enricher.enrich_column_summary(minimal_data)
        
        print("🎯 CLEAN ENRICHMENT RESULTS:")
        print(f"   Table: {enriched_data['table']}")
        print(f"   Total Columns: {enriched_data['total_columns']}")
        
        summary = enriched_data['enrichment_summary']
        print("   Summary:")
        print(f"     Columns with Key Phrases: {summary['columns_with_key_phrases']}")
        print(f"     Columns with Entity Types: {summary['columns_with_entity_types']}")
        print(f"     Unique Entity Types: {summary['unique_entity_types']}")
        print(f"     Avg Entity Confidence: {summary['avg_entity_confidence']:.2f}")
        print(f"     Total Key Phrases: {summary['total_key_phrases']}")
        print()
        
        # Show clean enrichment for each column
        print("📝 CLEAN COLUMN ENRICHMENTS:")
        print()
        
        for i, col_data in enumerate(enriched_data['enriched_columns'], 1):
            print(f"   📋 COLUMN {i}: {col_data['name']} ({col_data['type']})")
            
            if col_data['key_type']:
                print(f"      🔑 Key Type: {col_data['key_type']}")
            
            if col_data['entity_type']:
                print(f"      🏷️  Entity: {col_data['entity_type']} (confidence: {col_data['entity_confidence']:.2f})")
            else:
                print(f"      🏷️  Entity: Not classified")
            
            if col_data['key_phrases']:
                print(f"      💬 Key Phrases: {', '.join(col_data['key_phrases'][:3])}")
            else:
                print(f"      💬 Key Phrases: None")
            
            if col_data['semantic_tags']:
                print(f"      🏷️  Tags: {', '.join(col_data['semantic_tags'])}")
            
            print(f"      🆔 Identifier: {col_data['full_identifier']}")
            print()
        
        # Export clean formats
        print("💾 EXPORTING CLEAN FORMATS:")
        
        # Export clean embedding format (no statistical noise)
        clean_embedding = {
            "table": enriched_data['table'],
            "columns": []
        }
        
        for col_data in enriched_data['enriched_columns']:
            clean_col = {
                "table_name": enriched_data['table'],
                "column_name": col_data['name'],
                "data_type": col_data['type'],
                "is_nullable": col_data['nullable'],
                "key_type": col_data['key_type'] or 'none',
                "full_identifier": col_data['full_identifier'],
                
                # Pure enrichment data
                "key_phrases": col_data['key_phrases'],
                "key_phrases_text": ' | '.join(col_data['key_phrases']),
                "entity_type": col_data['entity_type'] or 'UNKNOWN',
                "entity_confidence": col_data['entity_confidence'],
                "semantic_tags": col_data['semantic_tags'],
                "semantic_tags_text": ' | '.join(col_data['semantic_tags']),
                
                # Clean embedding features
                "has_key_phrases": len(col_data['key_phrases']) > 0,
                "has_entity_type": col_data['entity_type'] is not None,
                "is_key_field": col_data['key_type'] is not None
            }
            clean_embedding["columns"].append(clean_col)
        
        with open("clean_embedding_output.json", "w") as f:
            json.dump(clean_embedding, f, indent=2, default=str)
        print(f"   clean_embedding_output.json: {Path('clean_embedding_output.json').stat().st_size:,} bytes")
        
        # Export ultra-clean format for embeddings
        ultra_clean = []
        for col_data in enriched_data['enriched_columns']:
            clean_col = {
                "id": col_data['full_identifier'],
                "name": col_data['name'],
                "type": col_data['type'],
                "nullable": col_data['nullable'],
                "key": col_data['key_type'] or 'none',
                "entity": col_data['entity_type'] or 'UNKNOWN',
                "phrases": col_data['key_phrases'][:3],  # Top 3 phrases
                "tags": col_data['semantic_tags'][:3],   # Top 3 tags
                "confidence": col_data['entity_confidence']
            }
            ultra_clean.append(clean_col)
        
        with open("ultra_clean_embedding.json", "w") as f:
            json.dump({"table": enriched_data['table'], "fields": ultra_clean}, f, indent=2, default=str)
        print(f"   ultra_clean_embedding.json: {Path('ultra_clean_embedding.json').stat().st_size:,} bytes")
        
        # Export semantic description format
        semantic_descriptions = []
        for col_data in enriched_data['enriched_columns']:
            # Create clean semantic description
            description_parts = []
            
            # Add entity type context
            if col_data['entity_type']:
                description_parts.append(f"Entity: {col_data['entity_type'].replace('_', ' ').lower()}")
            
            # Add key phrases
            if col_data['key_phrases']:
                description_parts.append(f"Concepts: {', '.join(col_data['key_phrases'][:3])}")
            
            # Add semantic context
            if col_data['semantic_tags']:
                description_parts.append(f"Context: {', '.join(col_data['semantic_tags'])}")
            
            # Create clean description
            tech_info = f"Database field {col_data['name']} ({col_data['type']})"
            if col_data['key_type']:
                tech_info += f" - {col_data['key_type']}"
            
            semantic_description = f"{tech_info}. {'. '.join(description_parts)}."
            
            semantic_descriptions.append({
                "field_id": col_data['full_identifier'],
                "semantic_description": semantic_description,
                "entity_type": col_data['entity_type'] or 'UNKNOWN'
            })
        
        with open("semantic_descriptions.json", "w") as f:
            json.dump({"table": enriched_data['table'], "descriptions": semantic_descriptions}, f, indent=2, default=str)
        print(f"   semantic_descriptions.json: {Path('semantic_descriptions.json').stat().st_size:,} bytes")
        
        print()
        print("🎯 CLEAN ENRICHMENT CHARACTERISTICS:")
        print()
        print("   ✨ PURE SEMANTIC FOCUS:")
        print("      - Key phrases for context")
        print("      - Entity types for classification")
        print("      - Semantic tags for grouping")
        print("      - NO statistical noise")
        print("      - NO pattern dependencies")
        print()
        print("   🎯 EMBEDDING OPTIMIZATION:")
        print("      - Clean, consistent structure")
        print("      - Semantic richness only")
        print("      - Multiple granularity levels")
        print("      - Domain-aware enrichment")
        print()
        print("   📊 OUTPUT FORMATS:")
        print("      - CLEAN EMBEDDING: Full semantic structure")
        print("      - ULTRA CLEAN: Minimal essential fields")
        print("      - SEMANTIC DESCRIPTIONS: Natural language")
        
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        print("   This demo requires the enrichment config files")
    
    print()
    print("=" * 80)
    print("CLEAN FEATURES ENRICHMENT DEMONSTRATION COMPLETED")
    print("=" * 80)


def show_clean_comparison():
    """Show comparison of clean vs noisy enrichment."""
    
    print("\n" + "=" * 60)
    print("CLEAN VS NOISY ENRICHMENT COMPARISON")
    print("=" * 60)
    
    print("\n❌ NOISY ENRICHMENT (with statistics):")
    print("   {")
    print('     "name": "email_address",')
    print('     "type": "varchar",')
    print('     "null_pct": 10.0,        // ❌ Statistical noise')
    print('     "unique_pct": 90.0,      // ❌ Statistical noise') 
    print('     "patterns": ["email"],   // ❌ Pattern dependency')
    print('     "entity_type": "EMAIL_ADDRESS",')
    print('     "key_phrases": [...]')
    print("   }")
    print("   → Cluttered with statistics")
    
    print("\n✅ CLEAN ENRICHMENT (semantic only):")
    print("   {")
    print('     "name": "email_address",')
    print('     "type": "varchar",')
    print('     "nullable": true,')
    print('     "key_type": "none",')
    print('     "entity_type": "EMAIL_ADDRESS",')
    print('     "key_phrases": ["email address", "contact email"],')
    print('     "semantic_tags": ["contact", "communication"],')
    print('     "full_identifier": "patients.email_address"')
    print("   }")
    print("   → Clean, focused semantic data")
    
    print("\n🎯 CLEAN EMBEDDING BENEFITS:")
    print("   ✅ Pure semantic information")
    print("   ✅ No statistical distractions")
    print("   ✅ No pattern dependencies")
    print("   ✅ Consistent, solid structure")
    print("   ✅ Domain-focused enrichment")
    print("   ✅ Embedding-optimized format")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    demonstrate_clean_enrichment()
    show_clean_comparison() 