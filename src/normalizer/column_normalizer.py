#!/usr/bin/env python3
"""
Column/Field Normalizer

This module provides functions to normalize minimal column summaries into different
views and formats for easier processing, analysis, and LLM consumption.

Key Normalization Views:
- Field list view (array of field objects)
- Column dictionary view (key-value mapping)
- Flat structure view (single-level objects)
- Pattern-based grouping
- Data type categorization
- LLM-optimized formats
"""

from typing import Dict, List, Any, Optional, Union
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum


class ViewType(Enum):
    """Different normalization view types."""
    FIELD_LIST = "field_list"
    COLUMN_DICT = "column_dict"
    FLAT_STRUCTURE = "flat_structure"
    PATTERN_GROUPS = "pattern_groups"
    TYPE_GROUPS = "type_groups"
    LLM_OPTIMIZED = "llm_optimized"


@dataclass
class NormalizedField:
    """Normalized field representation."""
    name: str
    type: str
    nullable: bool
    key_type: Optional[str] = None
    patterns: List[str] = field(default_factory=list)
    null_pct: float = 0.0
    unique_pct: float = 0.0
    
    # Additional normalized properties
    is_key: bool = field(init=False)
    is_pattern_field: bool = field(init=False)
    data_category: str = field(init=False)
    quality_score: float = field(init=False)
    
    def __post_init__(self):
        self.is_key = self.key_type is not None
        self.is_pattern_field = len(self.patterns) > 0
        self.data_category = self._categorize_data_type()
        self.quality_score = self._calculate_quality_score()
    
    def _categorize_data_type(self) -> str:
        """Categorize data type into broad categories."""
        type_lower = self.type.lower()
        
        if any(t in type_lower for t in ['int', 'bigint', 'smallint', 'tinyint']):
            return 'integer'
        elif any(t in type_lower for t in ['float', 'double', 'decimal', 'numeric']):
            return 'numeric'
        elif any(t in type_lower for t in ['varchar', 'text', 'char', 'string']):
            return 'text'
        elif any(t in type_lower for t in ['date', 'time', 'timestamp']):
            return 'temporal'
        elif any(t in type_lower for t in ['bool', 'bit']):
            return 'boolean'
        elif any(t in type_lower for t in ['json', 'xml']):
            return 'structured'
        else:
            return 'other'
    
    def _calculate_quality_score(self) -> float:
        """Calculate data quality score based on null percentage and uniqueness."""
        # Higher score for lower null percentage and appropriate uniqueness
        null_score = (100 - self.null_pct) / 100
        
        # Uniqueness score depends on field type
        if self.is_key:
            unique_score = self.unique_pct / 100  # Keys should be unique
        else:
            # Non-keys: moderate uniqueness is often good
            if self.unique_pct < 10:
                unique_score = self.unique_pct / 10  # Low uniqueness
            elif self.unique_pct > 90:
                unique_score = (100 - self.unique_pct) / 10 + 0.9  # Very high uniqueness
            else:
                unique_score = 1.0  # Good moderate uniqueness
        
        return (null_score * 0.7) + (unique_score * 0.3)


class ColumnFieldNormalizer:
    """Main class for normalizing column summaries into different views."""
    
    def __init__(self):
        self.supported_views = list(ViewType)
    
    def normalize(self, column_summary: Dict[str, Any], view_type: ViewType) -> Dict[str, Any]:
        """Main normalization method that routes to specific normalizers."""
        
        if view_type == ViewType.FIELD_LIST:
            return self.to_field_list(column_summary)
        elif view_type == ViewType.COLUMN_DICT:
            return self.to_column_dict(column_summary)
        elif view_type == ViewType.FLAT_STRUCTURE:
            return self.to_flat_structure(column_summary)
        elif view_type == ViewType.PATTERN_GROUPS:
            return self.to_pattern_groups(column_summary)
        elif view_type == ViewType.TYPE_GROUPS:
            return self.to_type_groups(column_summary)
        elif view_type == ViewType.LLM_OPTIMIZED:
            return self.to_llm_optimized(column_summary)
        else:
            raise ValueError(f"Unsupported view type: {view_type}")
    
    def to_field_list(self, column_summary: Dict[str, Any]) -> Dict[str, Any]:
        """Convert to field list view - array of field objects."""
        
        fields = []
        for col in column_summary.get('columns', []):
            field_obj = NormalizedField(
                name=col['name'],
                type=col['type'],
                nullable=col['nullable'],
                key_type=col.get('key_type'),
                patterns=col.get('patterns', []),
                null_pct=col.get('null_pct', 0.0),
                unique_pct=col.get('unique_pct', 0.0)
            )
            fields.append(field_obj.__dict__)
        
        return {
            'table': column_summary.get('table'),
            'total_columns': len(fields),
            'view_type': 'field_list',
            'fields': fields,
            'summary': {
                'key_fields': len([f for f in fields if f.get('is_key')]),
                'pattern_fields': len([f for f in fields if f.get('is_pattern_field')]),
                'nullable_fields': len([f for f in fields if f.get('nullable')]),
                'avg_quality_score': sum(f.get('quality_score', 0) for f in fields) / len(fields) if fields else 0
            }
        }
    
    def to_column_dict(self, column_summary: Dict[str, Any]) -> Dict[str, Any]:
        """Convert to column dictionary view - key-value mapping by column name."""
        
        columns_dict = {}
        for col in column_summary.get('columns', []):
            field_obj = NormalizedField(
                name=col['name'],
                type=col['type'],
                nullable=col['nullable'],
                key_type=col.get('key_type'),
                patterns=col.get('patterns', []),
                null_pct=col.get('null_pct', 0.0),
                unique_pct=col.get('unique_pct', 0.0)
            )
            
            columns_dict[col['name']] = {
                'type': field_obj.type,
                'nullable': field_obj.nullable,
                'key_type': field_obj.key_type,
                'patterns': field_obj.patterns,
                'data_category': field_obj.data_category,
                'quality_score': field_obj.quality_score,
                'statistics': {
                    'null_pct': field_obj.null_pct,
                    'unique_pct': field_obj.unique_pct
                }
            }
        
        return {
            'table': column_summary.get('table'),
            'view_type': 'column_dict',
            'columns': columns_dict,
            'metadata': {
                'total_columns': len(columns_dict),
                'column_names': list(columns_dict.keys())
            }
        }
    
    def to_flat_structure(self, column_summary: Dict[str, Any]) -> Dict[str, Any]:
        """Convert to flat structure view - single-level objects."""
        
        flat_columns = []
        for col in column_summary.get('columns', []):
            field_obj = NormalizedField(
                name=col['name'],
                type=col['type'],
                nullable=col['nullable'],
                key_type=col.get('key_type'),
                patterns=col.get('patterns', []),
                null_pct=col.get('null_pct', 0.0),
                unique_pct=col.get('unique_pct', 0.0)
            )
            
            flat_col = {
                'table_name': column_summary.get('table'),
                'column_name': field_obj.name,
                'data_type': field_obj.type,
                'data_category': field_obj.data_category,
                'is_nullable': field_obj.nullable,
                'is_key': field_obj.is_key,
                'key_type': field_obj.key_type or 'none',
                'has_patterns': field_obj.is_pattern_field,
                'pattern_count': len(field_obj.patterns),
                'patterns_list': ','.join(field_obj.patterns) if field_obj.patterns else 'none',
                'null_percentage': field_obj.null_pct,
                'unique_percentage': field_obj.unique_pct,
                'quality_score': field_obj.quality_score,
                'full_identifier': f"{column_summary.get('table')}.{field_obj.name}"
            }
            flat_columns.append(flat_col)
        
        return {
            'view_type': 'flat_structure',
            'columns': flat_columns,
            'total_columns': len(flat_columns)
        }
    
    def to_pattern_groups(self, column_summary: Dict[str, Any]) -> Dict[str, Any]:
        """Convert to pattern-based grouping view."""
        
        pattern_groups = defaultdict(list)
        no_pattern_fields = []
        
        for col in column_summary.get('columns', []):
            field_obj = NormalizedField(
                name=col['name'],
                type=col['type'],
                nullable=col['nullable'],
                key_type=col.get('key_type'),
                patterns=col.get('patterns', []),
                null_pct=col.get('null_pct', 0.0),
                unique_pct=col.get('unique_pct', 0.0)
            )
            
            field_info = {
                'name': field_obj.name,
                'type': field_obj.type,
                'key_type': field_obj.key_type,
                'quality_score': field_obj.quality_score
            }
            
            if field_obj.patterns:
                for pattern in field_obj.patterns:
                    pattern_groups[pattern].append(field_info)
            else:
                no_pattern_fields.append(field_info)
        
        return {
            'table': column_summary.get('table'),
            'view_type': 'pattern_groups',
            'pattern_groups': dict(pattern_groups),
            'no_pattern_fields': no_pattern_fields,
            'summary': {
                'total_patterns': len(pattern_groups),
                'fields_with_patterns': sum(len(fields) for fields in pattern_groups.values()),
                'fields_without_patterns': len(no_pattern_fields)
            }
        }
    
    def to_type_groups(self, column_summary: Dict[str, Any]) -> Dict[str, Any]:
        """Convert to data type grouping view."""
        
        type_groups = defaultdict(list)
        category_groups = defaultdict(list)
        
        for col in column_summary.get('columns', []):
            field_obj = NormalizedField(
                name=col['name'],
                type=col['type'],
                nullable=col['nullable'],
                key_type=col.get('key_type'),
                patterns=col.get('patterns', []),
                null_pct=col.get('null_pct', 0.0),
                unique_pct=col.get('unique_pct', 0.0)
            )
            
            field_info = {
                'name': field_obj.name,
                'nullable': field_obj.nullable,
                'key_type': field_obj.key_type,
                'patterns': field_obj.patterns,
                'quality_score': field_obj.quality_score
            }
            
            # Group by specific data type
            type_groups[field_obj.type].append(field_info)
            
            # Group by data category
            category_groups[field_obj.data_category].append(field_info)
        
        return {
            'table': column_summary.get('table'),
            'view_type': 'type_groups',
            'by_data_type': dict(type_groups),
            'by_category': dict(category_groups),
            'summary': {
                'unique_types': len(type_groups),
                'unique_categories': len(category_groups),
                'category_distribution': {cat: len(fields) for cat, fields in category_groups.items()}
            }
        }
    
    def to_llm_optimized(self, column_summary: Dict[str, Any]) -> Dict[str, Any]:
        """Convert to LLM-optimized view with minimal tokens and clear structure."""
        
        fields = []
        key_fields = []
        pattern_fields = []
        
        for col in column_summary.get('columns', []):
            field_obj = NormalizedField(
                name=col['name'],
                type=col['type'],
                nullable=col['nullable'],
                key_type=col.get('key_type'),
                patterns=col.get('patterns', []),
                null_pct=col.get('null_pct', 0.0),
                unique_pct=col.get('unique_pct', 0.0)
            )
            
            # Compact field representation
            field_compact = f"{field_obj.name}:{field_obj.type}"
            if not field_obj.nullable:
                field_compact += " NOT NULL"
            if field_obj.key_type:
                field_compact += f" {field_obj.key_type}"
            if field_obj.patterns:
                field_compact += f" [{','.join(field_obj.patterns)}]"
            
            fields.append(field_compact)
            
            # Separate key and pattern fields for quick reference
            if field_obj.is_key:
                key_fields.append(field_obj.name)
            if field_obj.is_pattern_field:
                pattern_fields.append({
                    'field': field_obj.name,
                    'patterns': field_obj.patterns
                })
        
        return {
            'table': column_summary.get('table'),
            'view_type': 'llm_optimized',
            'fields': fields,  # Compact string representation
            'keys': key_fields,
            'patterns': pattern_fields,
            'stats': {
                'total': len(fields),
                'keys': len(key_fields),
                'patterns': len(pattern_fields)
            }
        }


# Convenience functions for direct usage
def normalize_to_field_list(column_summary: Dict[str, Any]) -> Dict[str, Any]:
    """Convert minimal column summary to field list view."""
    normalizer = ColumnFieldNormalizer()
    return normalizer.to_field_list(column_summary)


def normalize_to_column_dict(column_summary: Dict[str, Any]) -> Dict[str, Any]:
    """Convert minimal column summary to column dictionary view."""
    normalizer = ColumnFieldNormalizer()
    return normalizer.to_column_dict(column_summary)


def normalize_to_flat_structure(column_summary: Dict[str, Any]) -> Dict[str, Any]:
    """Convert minimal column summary to flat structure view."""
    normalizer = ColumnFieldNormalizer()
    return normalizer.to_flat_structure(column_summary)


def normalize_by_patterns(column_summary: Dict[str, Any]) -> Dict[str, Any]:
    """Convert minimal column summary to pattern-based grouping."""
    normalizer = ColumnFieldNormalizer()
    return normalizer.to_pattern_groups(column_summary)


def normalize_by_data_types(column_summary: Dict[str, Any]) -> Dict[str, Any]:
    """Convert minimal column summary to data type grouping."""
    normalizer = ColumnFieldNormalizer()
    return normalizer.to_type_groups(column_summary)


def normalize_for_llm(column_summary: Dict[str, Any]) -> Dict[str, Any]:
    """Convert minimal column summary to LLM-optimized format."""
    normalizer = ColumnFieldNormalizer()
    return normalizer.to_llm_optimized(column_summary) 