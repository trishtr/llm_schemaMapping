#!/usr/bin/env python3
"""
Simple Column Profiler Demo

This script demonstrates the simple column profiler output format and structure
that focuses specifically on column-level analysis without the complexity 
of full schema profiling.

Shows:
- Column metadata analysis output
- Data statistics format (null %, unique %)
- Pattern detection results
- Key properties detection
- Clean, focused output structure
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
import json
from datetime import datetime
from pathlib import Path


@dataclass
class ColumnAnalysis:
    """Simple column analysis result."""
    
    # Basic column information
    column_name: str
    table_name: str
    schema_name: Optional[str] = None
    
    # Data type information
    data_type: str = "unknown"
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


def create_sample_column_analyses():
    """Create sample column analyses to demonstrate output format."""
    
    # Sample 1: Primary key column
    patient_id_analysis = ColumnAnalysis(
        column_name="patient_id",
        table_name="patients",
        schema_name="public",
        data_type="bigint",
        is_nullable=False,
        is_primary_key=True,
        is_unique=True,
        is_indexed=True,
        ordinal_position=1,
        estimated_row_count=50000,
        null_count=0,
        unique_count=50000,
        null_percentage=0.0,
        unique_percentage=100.0,
        sample_values=[100001, 100002, 100003, 100004, 100005],
        detected_patterns=[],  # No patterns for ID columns typically
        analysis_timestamp=datetime.now().isoformat()
    )
    
    # Sample 2: Foreign key column
    provider_id_analysis = ColumnAnalysis(
        column_name="provider_id",
        table_name="patients",
        schema_name="public",
        data_type="int",
        is_nullable=True,
        is_primary_key=False,
        is_foreign_key=True,
        is_unique=False,
        is_indexed=True,
        ordinal_position=2,
        estimated_row_count=50000,
        null_count=2500,
        unique_count=1500,
        null_percentage=5.0,
        unique_percentage=3.0,
        sample_values=[1001, 1002, 1003, 1001, 1004],
        detected_patterns=[],
        foreign_key_reference={
            "referenced_table": "providers",
            "referenced_column": "provider_id",
            "constraint_name": "fk_patients_provider"
        },
        analysis_timestamp=datetime.now().isoformat()
    )
    
    # Sample 3: Email column with pattern
    email_analysis = ColumnAnalysis(
        column_name="email_address",
        table_name="patients",
        schema_name="public",
        data_type="varchar",
        max_length=255,
        is_nullable=True,
        is_primary_key=False,
        is_foreign_key=False,
        is_unique=False,
        is_indexed=False,
        ordinal_position=3,
        estimated_row_count=50000,
        null_count=5000,
        unique_count=45000,
        null_percentage=10.0,
        unique_percentage=90.0,
        sample_values=[
            "john.doe@example.com", 
            "jane.smith@test.org", 
            "bob.wilson@clinic.net",
            "alice.brown@hospital.edu",
            "charlie.davis@medical.com"
        ],
        detected_patterns=["email_address"],
        analysis_timestamp=datetime.now().isoformat()
    )
    
    # Sample 4: NPI identifier column
    npi_analysis = ColumnAnalysis(
        column_name="npi",
        table_name="providers",
        schema_name="public",
        data_type="varchar",
        max_length=10,
        is_nullable=False,
        is_primary_key=False,
        is_foreign_key=False,
        is_unique=True,
        is_indexed=True,
        ordinal_position=2,
        estimated_row_count=1500,
        null_count=0,
        unique_count=1500,
        null_percentage=0.0,
        unique_percentage=100.0,
        sample_values=["1234567890", "9876543210", "5555666677", "1111222233", "9999888877"],
        detected_patterns=["npi_identifier"],
        analysis_timestamp=datetime.now().isoformat()
    )
    
    # Sample 5: Phone number column
    phone_analysis = ColumnAnalysis(
        column_name="phone_number",
        table_name="patients",
        schema_name="public",
        data_type="varchar",
        max_length=20,
        is_nullable=True,
        is_primary_key=False,
        is_foreign_key=False,
        is_unique=False,
        is_indexed=False,
        ordinal_position=4,
        estimated_row_count=50000,
        null_count=7500,
        unique_count=42000,
        null_percentage=15.0,
        unique_percentage=84.0,
        sample_values=["555-123-4567", "555-987-6543", "555-456-7890", "555-234-5678", "555-345-6789"],
        detected_patterns=["phone_number"],
        analysis_timestamp=datetime.now().isoformat()
    )
    
    # Sample 6: Status/enum column
    status_analysis = ColumnAnalysis(
        column_name="status",
        table_name="patients",
        schema_name="public",
        data_type="varchar",
        max_length=20,
        is_nullable=False,
        is_primary_key=False,
        is_foreign_key=False,
        is_unique=False,
        is_indexed=True,
        ordinal_position=5,
        estimated_row_count=50000,
        null_count=0,
        unique_count=5,
        null_percentage=0.0,
        unique_percentage=0.01,
        sample_values=["active", "inactive", "pending", "completed", "cancelled"],
        detected_patterns=["status_field"],
        analysis_timestamp=datetime.now().isoformat()
    )
    
    return [
        patient_id_analysis,
        provider_id_analysis, 
        email_analysis,
        npi_analysis,
        phone_analysis,
        status_analysis
    ]


def demonstrate_simple_column_profiler():
    """Demonstrate the simple column profiler output."""
    
    print("=" * 80)
    print("SIMPLE COLUMN PROFILER OUTPUT FORMAT DEMONSTRATION")
    print("=" * 80)
    print()
    
    # Create sample column analyses
    column_analyses = create_sample_column_analyses()
    
    print("🔧 SIMPLE COLUMN PROFILER FEATURES:")
    print("   ✅ Column metadata extraction")
    print("   ✅ Data type analysis") 
    print("   ✅ Null/unique/key detection")
    print("   ✅ Pattern recognition")
    print("   ✅ Sample data collection")
    print("   ✅ Clean, focused output")
    print()
    
    # Show individual column analyses
    print("📊 INDIVIDUAL COLUMN ANALYSES:")
    print()
    
    for i, analysis in enumerate(column_analyses, 1):
        print(f"   📝 COLUMN {i}: {analysis.table_name}.{analysis.column_name}")
        print(f"      Data Type: {analysis.data_type}" + 
              (f"({analysis.max_length})" if analysis.max_length else ""))
        print(f"      Position: #{analysis.ordinal_position}")
        
        # Key properties
        key_props = []
        if analysis.is_primary_key:
            key_props.append("PRIMARY KEY")
        if analysis.is_foreign_key:
            key_props.append("FOREIGN KEY")
        if analysis.is_unique:
            key_props.append("UNIQUE")
        if analysis.is_indexed:
            key_props.append("INDEXED")
        
        if key_props:
            print(f"      Properties: {', '.join(key_props)}")
        
        # Data statistics
        print(f"      Rows: {analysis.estimated_row_count:,}")
        print(f"      Nulls: {analysis.null_count:,} ({analysis.null_percentage:.1f}%)")
        print(f"      Unique: {analysis.unique_count:,} ({analysis.unique_percentage:.1f}%)")
        
        # Foreign key reference
        if analysis.foreign_key_reference:
            fk_ref = analysis.foreign_key_reference
            print(f"      🔗 References: {fk_ref['referenced_table']}.{fk_ref['referenced_column']}")
        
        # Detected patterns
        if analysis.detected_patterns:
            print(f"      🎯 Patterns: {', '.join(analysis.detected_patterns)}")
        else:
            print(f"      🎯 Patterns: None detected")
        
        # Sample values
        sample_str = ', '.join(str(v) for v in analysis.sample_values[:3])
        print(f"      📋 Samples: {sample_str}...")
        
        print()
    
    # Create table-level result
    patients_result = ColumnProfilingResult(
        database_name="healthcare_db",
        table_name="patients",
        schema_name="public",
        database_type="postgresql",
        columns=[col for col in column_analyses if col.table_name == "patients"],
        total_columns=5,
        total_rows_analyzed=50000,
        sample_size=5
    )
    
    providers_result = ColumnProfilingResult(
        database_name="healthcare_db",
        table_name="providers", 
        schema_name="public",
        database_type="postgresql",
        columns=[col for col in column_analyses if col.table_name == "providers"],
        total_columns=1,
        total_rows_analyzed=1500,
        sample_size=5
    )
    
    # Show table-level results
    print("📋 TABLE-LEVEL PROFILING RESULTS:")
    print()
    
    for result in [patients_result, providers_result]:
        print(f"   🏥 TABLE: {result.table_name}")
        print(f"      Database: {result.database_name} ({result.database_type})")
        print(f"      Schema: {result.schema_name}")
        print(f"      Total Columns: {result.total_columns}")
        print(f"      Total Rows: {result.total_rows_analyzed:,}")
        print(f"      Analysis Time: {result.analysis_timestamp}")
        
        # Column summary
        pk_columns = [col.column_name for col in result.columns if col.is_primary_key]
        fk_columns = [col.column_name for col in result.columns if col.is_foreign_key]
        pattern_columns = [col.column_name for col in result.columns if col.detected_patterns]
        
        print(f"      Primary Keys: {', '.join(pk_columns) if pk_columns else 'None'}")
        print(f"      Foreign Keys: {', '.join(fk_columns) if fk_columns else 'None'}")
        print(f"      Pattern Columns: {', '.join(pattern_columns) if pattern_columns else 'None'}")
        print()
    
    # Export sample outputs
    print("💾 EXPORTING SAMPLE OUTPUTS:")
    
    # Export individual column analysis
    from dataclasses import asdict
    email_analysis = [col for col in column_analyses if col.column_name == "email_address"][0]
    with open("sample_column_analysis.json", "w") as f:
        json.dump(asdict(email_analysis), f, indent=2, default=str)
    print(f"   sample_column_analysis.json: {Path('sample_column_analysis.json').stat().st_size:,} bytes")
    
    # Export table-level result
    with open("sample_table_column_profiling.json", "w") as f:
        json.dump(asdict(patients_result), f, indent=2, default=str)
    print(f"   sample_table_column_profiling.json: {Path('sample_table_column_profiling.json').stat().st_size:,} bytes")
    
    # Export minimal column summary
    minimal_summary = {
        "table": patients_result.table_name,
        "total_columns": patients_result.total_columns,
        "columns": [
            {
                "name": col.column_name,
                "type": col.data_type,
                "nullable": col.is_nullable,
                "key_type": "PK" if col.is_primary_key else "FK" if col.is_foreign_key else None,
                "patterns": col.detected_patterns,
                "null_pct": col.null_percentage,
                "unique_pct": col.unique_percentage
            } for col in patients_result.columns
        ]
    }
    
    with open("minimal_column_summary.json", "w") as f:
        json.dump(minimal_summary, f, indent=2, default=str)
    print(f"   minimal_column_summary.json: {Path('minimal_column_summary.json').stat().st_size:,} bytes")
    
    print()
    print("🎯 SIMPLE COLUMN PROFILER OUTPUT CHARACTERISTICS:")
    print()
    print("   📊 FOCUSED ANALYSIS:")
    print("      - Column-level metadata only")
    print("      - No table or schema relationships")
    print("      - Clean, structured data models")
    print()
    print("   📈 DATA STATISTICS:")
    print("      - Row counts and percentages")
    print("      - Null and unique value analysis")
    print("      - Sample data collection")
    print()
    print("   🔍 PATTERN DETECTION:")
    print("      - Automatic pattern recognition")
    print("      - Healthcare, email, phone patterns")
    print("      - Configurable pattern library")
    print()
    print("   🔗 KEY RELATIONSHIPS:")
    print("      - Primary/foreign key detection")
    print("      - Index identification")
    print("      - Foreign key reference details")
    print()
    print("   💡 USE CASES:")
    print("      - Targeted column analysis")
    print("      - Data quality assessment")
    print("      - Pattern discovery")
    print("      - LLM-friendly output format")
    print("      - Quick column profiling")
    
    print()
    print("=" * 80)
    print("SIMPLE COLUMN PROFILER OUTPUT FORMAT DEMONSTRATION COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    demonstrate_simple_column_profiler() 