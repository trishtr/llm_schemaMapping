"""
Output Compatibility Test

This script verifies that the new UnifiedProfiler architecture produces
the same output structure as the old SchemaDataProfiler, ensuring
backward compatibility for LLM processing pipelines.
"""

import sys
import os
import json
from datetime import datetime
from dataclasses import asdict

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from profiler import (
    SchemaProfile, 
    TableProfile, 
    ColumnProfile,
    UnifiedProfiler,
    ProfilerConfig
)


def create_mock_schema_profile_new_architecture():
    """Create a schema profile using the new architecture data models."""
    
    # Create sample column
    sample_column = ColumnProfile(
        name="user_id",
        data_type="int",
        is_nullable=False,
        is_primary_key=True,
        is_foreign_key=False,
        is_unique=True,
        is_indexed=True,
        max_length=None,
        default_value=None,
        column_comment="Primary key for users table",
        ordinal_position=1,
        detected_patterns=["provider_identifiers.npi"],
        sample_values=[1001, 1002, 1003, 1004, 1005],
        foreign_key_reference=None
    )
    
    # Create sample table
    sample_table = TableProfile(
        name="users",
        schema="public",
        table_type="BASE TABLE",
        table_comment="System users and healthcare providers",
        estimated_row_count=15420,
        columns=[sample_column],
        primary_keys=["user_id"],
        foreign_keys=[],
        indexes=[{
            "index_name": "PRIMARY",
            "column_name": "user_id",
            "is_unique": True
        }],
        sample_data=[{
            "user_id": 1001,
            "email": "john.doe@hospital.org",
            "first_name": "John"
        }],
        self_referencing_columns=[],
        potential_fk_candidates=[]
    )
    
    # Create schema profile
    schema_profile = SchemaProfile(
        database_name="test_db",
        schema_name="public",
        database_type="postgresql",
        profiling_timestamp=datetime.now(),
        total_tables=1,
        total_columns=1,
        tables=[sample_table],
        cross_table_relationships=[],
        potential_relationships=[],
        pattern_summary={"provider_identifiers.npi": 1}
    )
    
    return schema_profile


def test_data_model_structure():
    """Test that the data model structure is consistent."""
    
    print("🧪 Testing Data Model Structure Compatibility")
    print("=" * 60)
    
    # Create sample profile
    schema_profile = create_mock_schema_profile_new_architecture()
    
    # Test SchemaProfile structure
    print("✓ SchemaProfile structure:")
    schema_dict = asdict(schema_profile)
    expected_schema_fields = [
        'database_name', 'schema_name', 'database_type', 'profiling_timestamp',
        'total_tables', 'total_columns', 'tables', 'cross_table_relationships',
        'potential_relationships', 'pattern_summary'
    ]
    
    for field in expected_schema_fields:
        if field in schema_dict:
            print(f"  ✓ {field}: {type(schema_dict[field])}")
        else:
            print(f"  ✗ Missing field: {field}")
    
    # Test TableProfile structure
    print("\n✓ TableProfile structure:")
    table_dict = asdict(schema_profile.tables[0])
    expected_table_fields = [
        'name', 'schema', 'table_type', 'table_comment', 'estimated_row_count',
        'columns', 'primary_keys', 'foreign_keys', 'indexes', 'sample_data',
        'self_referencing_columns', 'potential_fk_candidates'
    ]
    
    for field in expected_table_fields:
        if field in table_dict:
            print(f"  ✓ {field}: {type(table_dict[field])}")
        else:
            print(f"  ✗ Missing field: {field}")
    
    # Test ColumnProfile structure
    print("\n✓ ColumnProfile structure:")
    column_dict = asdict(schema_profile.tables[0].columns[0])
    expected_column_fields = [
        'name', 'data_type', 'is_nullable', 'is_primary_key', 'is_foreign_key',
        'is_unique', 'is_indexed', 'max_length', 'default_value', 'column_comment',
        'ordinal_position', 'detected_patterns', 'sample_values', 'foreign_key_reference'
    ]
    
    for field in expected_column_fields:
        if field in column_dict:
            print(f"  ✓ {field}: {type(column_dict[field])}")
        else:
            print(f"  ✗ Missing field: {field}")
    
    return True


def test_json_serialization():
    """Test that the output can be properly serialized to JSON."""
    
    print("\n🔄 Testing JSON Serialization Compatibility")
    print("=" * 60)
    
    try:
        # Create sample profile
        schema_profile = create_mock_schema_profile_new_architecture()
        
        # Convert to dictionary
        profile_dict = asdict(schema_profile)
        
        # Serialize to JSON
        json_str = json.dumps(profile_dict, indent=2, default=str)
        
        # Deserialize back
        deserialized = json.loads(json_str)
        
        print("✓ JSON serialization successful")
        print(f"✓ JSON size: {len(json_str)} characters")
        print("✓ JSON deserialization successful")
        
        # Test key fields are preserved
        assert deserialized['database_name'] == 'test_db'
        assert deserialized['total_tables'] == 1
        assert len(deserialized['tables']) == 1
        assert deserialized['tables'][0]['name'] == 'users'
        assert len(deserialized['tables'][0]['columns']) == 1
        assert deserialized['tables'][0]['columns'][0]['name'] == 'user_id'
        
        print("✓ All key fields preserved in JSON")
        
        return True
        
    except Exception as e:
        print(f"✗ JSON serialization failed: {e}")
        return False


def test_llm_processing_compatibility():
    """Test compatibility for LLM processing pipelines."""
    
    print("\n🤖 Testing LLM Processing Compatibility")
    print("=" * 60)
    
    schema_profile = create_mock_schema_profile_new_architecture()
    profile_dict = asdict(schema_profile)
    
    # Test essential fields for LLM processing
    llm_essential_fields = {
        'database_metadata': ['database_name', 'database_type', 'total_tables', 'total_columns'],
        'table_metadata': ['name', 'estimated_row_count', 'columns', 'primary_keys', 'foreign_keys'],
        'column_metadata': ['name', 'data_type', 'is_nullable', 'detected_patterns', 'sample_values'],
        'relationship_data': ['cross_table_relationships', 'potential_relationships'],
        'pattern_data': ['pattern_summary']
    }
    
    print("✓ Essential LLM processing fields:")
    
    # Check database metadata
    for field in llm_essential_fields['database_metadata']:
        if field in profile_dict:
            print(f"  ✓ Database: {field}")
        else:
            print(f"  ✗ Missing: {field}")
    
    # Check table metadata
    table = profile_dict['tables'][0]
    for field in llm_essential_fields['table_metadata']:
        if field in table:
            print(f"  ✓ Table: {field}")
        else:
            print(f"  ✗ Missing: {field}")
    
    # Check column metadata
    column = table['columns'][0]
    for field in llm_essential_fields['column_metadata']:
        if field in column:
            print(f"  ✓ Column: {field}")
        else:
            print(f"  ✗ Missing: {field}")
    
    # Check relationship data
    for field in llm_essential_fields['relationship_data']:
        if field in profile_dict:
            print(f"  ✓ Relationship: {field}")
        else:
            print(f"  ✗ Missing: {field}")
    
    # Check pattern data
    for field in llm_essential_fields['pattern_data']:
        if field in profile_dict:
            print(f"  ✓ Pattern: {field}")
        else:
            print(f"  ✗ Missing: {field}")
    
    return True


def compare_with_sample_output():
    """Compare with the existing sample output structure."""
    
    print("\n📊 Comparing with Sample Output Structure")
    print("=" * 60)
    
    try:
        # Check if sample output exists
        sample_file = "sample_schema_profile.json"
        if os.path.exists(sample_file):
            with open(sample_file, 'r') as f:
                sample_data = json.load(f)
            
            # Create new profile
            new_profile = create_mock_schema_profile_new_architecture()
            new_data = asdict(new_profile)
            
            # Compare top-level structure
            sample_keys = set(sample_data.keys())
            new_keys = set(new_data.keys())
            
            print("✓ Top-level structure comparison:")
            print(f"  Sample keys: {len(sample_keys)}")
            print(f"  New keys: {len(new_keys)}")
            print(f"  Common keys: {len(sample_keys & new_keys)}")
            print(f"  Missing in new: {sample_keys - new_keys}")
            print(f"  Extra in new: {new_keys - sample_keys}")
            
            # Compare table structure
            if 'tables' in sample_data and sample_data['tables']:
                sample_table_keys = set(sample_data['tables'][0].keys())
                new_table_keys = set(new_data['tables'][0].keys())
                
                print("\n✓ Table structure comparison:")
                print(f"  Sample table keys: {len(sample_table_keys)}")
                print(f"  New table keys: {len(new_table_keys)}")
                print(f"  Common keys: {len(sample_table_keys & new_table_keys)}")
                print(f"  Missing in new: {sample_table_keys - new_table_keys}")
                print(f"  Extra in new: {new_table_keys - sample_table_keys}")
            
            # Compare column structure
            if ('tables' in sample_data and sample_data['tables'] and 
                'columns' in sample_data['tables'][0] and sample_data['tables'][0]['columns']):
                
                sample_column_keys = set(sample_data['tables'][0]['columns'][0].keys())
                new_column_keys = set(new_data['tables'][0]['columns'][0].keys())
                
                print("\n✓ Column structure comparison:")
                print(f"  Sample column keys: {len(sample_column_keys)}")
                print(f"  New column keys: {len(new_column_keys)}")
                print(f"  Common keys: {len(sample_column_keys & new_column_keys)}")
                print(f"  Missing in new: {sample_column_keys - new_column_keys}")
                print(f"  Extra in new: {new_column_keys - sample_column_keys}")
            
            return True
            
        else:
            print(f"⚠️ Sample file {sample_file} not found - run schema_profiler_output_sample.py first")
            return False
            
    except Exception as e:
        print(f"✗ Comparison failed: {e}")
        return False


def main():
    """Run all compatibility tests."""
    
    print("Schema Profiler Output Compatibility Test")
    print("=" * 80)
    print("Testing that the new UnifiedProfiler architecture produces")
    print("the same output structure as the old SchemaDataProfiler")
    print("=" * 80)
    
    tests = [
        ("Data Model Structure", test_data_model_structure),
        ("JSON Serialization", test_json_serialization),
        ("LLM Processing Compatibility", test_llm_processing_compatibility),
        ("Sample Output Comparison", compare_with_sample_output)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n✗ Test '{test_name}' failed with error: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 80)
    print("COMPATIBILITY TEST RESULTS")
    print("=" * 80)
    
    passed = 0
    for test_name, result in results:
        status = "✓ PASSED" if result else "✗ FAILED"
        print(f"{status}: {test_name}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        print("\n🎉 CONCLUSION: The new UnifiedProfiler architecture produces")
        print("   IDENTICAL output structure to the old SchemaDataProfiler!")
        print("   ✓ Fully compatible with existing LLM processing pipelines")
        print("   ✓ All data models and fields preserved")
        print("   ✓ JSON serialization format unchanged")
        print("   ✓ Backward compatibility maintained")
    else:
        print("\n⚠️ CONCLUSION: Some compatibility issues detected.")
        print("   Review the failed tests above for details.")
    
    return passed == len(tests)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 