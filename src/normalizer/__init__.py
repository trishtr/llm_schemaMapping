"""
Schema Normalizer Package

This package provides functions to normalize schema profiling output into different views
and formats for easier processing, analysis, and LLM consumption.

Key Features:
- Column/field view normalization
- Flat structure conversion
- Pattern-based grouping
- Data type categorization
- LLM-optimized formats
"""

from .column_normalizer import (
    ColumnFieldNormalizer,
    ViewType,
    NormalizedField,
    normalize_to_field_list,
    normalize_to_column_dict,
    normalize_to_flat_structure,
    normalize_by_patterns,
    normalize_by_data_types,
    normalize_for_llm
)

__all__ = [
    'ColumnFieldNormalizer',
    'ViewType',
    'NormalizedField',
    'normalize_to_field_list',
    'normalize_to_column_dict', 
    'normalize_to_flat_structure',
    'normalize_by_patterns',
    'normalize_by_data_types',
    'normalize_for_llm'
] 