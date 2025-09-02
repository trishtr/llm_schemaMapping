#!/usr/bin/env python3
"""
Column Enricher

This module provides functions to enrich minimal column summaries with key phrases
and entity types for improved embeddings and semantic understanding.

Key Features:
- Key phrase extraction based on field names, data types, and patterns
- Entity type classification using configurable rules
- Confidence scoring for enrichments
- Clean output format optimized for embeddings
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple
import logging
from datetime import datetime

from .config_loader import EnrichmentConfigLoader


@dataclass
class EnrichedColumnData:
    """Enriched column data optimized for embeddings."""
    
    # Original column information (from minimal summary)
    name: str
    type: str
    nullable: bool
    key_type: Optional[str] = None
    
    # Enrichment data
    key_phrases: List[str] = field(default_factory=list)
    entity_type: Optional[str] = None
    entity_confidence: float = 0.0
    semantic_tags: List[str] = field(default_factory=list)
    
    # Context information
    table_name: Optional[str] = None
    full_identifier: Optional[str] = None
    
    # Enrichment metadata
    enrichment_timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


class ColumnEnricher:
    """Main class for enriching column data with key phrases and entity types."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_loader = EnrichmentConfigLoader(config_path)
        self.logger = logging.getLogger(__name__)
        
        # Load configurations
        self.key_phrases_config = self.config_loader.load_key_phrases_config()
        self.entity_types_config = self.config_loader.load_entity_types_config()
        
        self.logger.info("ColumnEnricher initialized with configurations")
    
    def enrich_column_summary(self, column_summary: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich a complete minimal column summary."""
        
        table_name = column_summary.get('table', 'unknown')
        enriched_columns = []
        
        for col in column_summary.get('columns', []):
            enriched_col = self.enrich_single_column(col, table_name)
            enriched_columns.append(enriched_col)
        
        return {
            'table': table_name,
            'total_columns': len(enriched_columns),
            'enriched_columns': [col.__dict__ for col in enriched_columns],
            'enrichment_summary': self._create_enrichment_summary(enriched_columns),
            'enrichment_timestamp': datetime.now().isoformat()
        }
    
    def enrich_single_column(self, column_data: Dict[str, Any], table_name: str = None) -> EnrichedColumnData:
        """Enrich a single column with key phrases and entity types."""
        
        # Create base enriched column
        enriched = EnrichedColumnData(
            name=column_data.get('name', ''),
            type=column_data.get('type', ''),
            nullable=column_data.get('nullable', True),
            key_type=column_data.get('key_type'),
            table_name=table_name,
            full_identifier=f"{table_name}.{column_data.get('name', '')}" if table_name else column_data.get('name', '')
        )
        
        # Extract key phrases
        enriched.key_phrases = self.extract_key_phrases(enriched)
        
        # Classify entity type
        entity_result = self.classify_entity_type(enriched)
        enriched.entity_type = entity_result['entity_type']
        enriched.entity_confidence = entity_result['confidence']
        enriched.semantic_tags = entity_result['semantic_tags']
        
        return enriched
    
    def extract_key_phrases(self, column: EnrichedColumnData) -> List[str]:
        """Extract key phrases for a column based on configured rules."""
        
        key_phrases = []
        extraction_rules = self.key_phrases_config.get('key_phrase_extraction_rules', {})
        
        # Check healthcare domain rules first
        healthcare_rules = extraction_rules.get('healthcare_domain', {})
        phrases = self._extract_from_domain_rules(column, healthcare_rules)
        key_phrases.extend(phrases)
        
        # Check general domain rules
        general_rules = extraction_rules.get('general_domain', {})
        phrases = self._extract_from_domain_rules(column, general_rules)
        key_phrases.extend(phrases)
        
        # Apply phrase selection rules
        key_phrases = self._apply_phrase_selection_rules(key_phrases)
        
        return key_phrases
    
    def classify_entity_type(self, column: EnrichedColumnData) -> Dict[str, Any]:
        """Classify the entity type of a column."""
        
        entity_types = self.entity_types_config.get('entity_types', {})
        classification_rules = self.entity_types_config.get('entity_classification_rules', {})
        
        best_match = None
        best_confidence = 0.0
        best_semantic_tags = []
        
        # Check all entity categories
        for category, entities in entity_types.items():
            for entity_key, entity_config in entities.items():
                confidence = self._calculate_entity_confidence(column, entity_config, classification_rules)
                
                if confidence > best_confidence:
                    best_confidence = confidence
                    best_match = entity_config.get('type')
                    best_semantic_tags = entity_config.get('semantic_tags', [])
        
        # Apply confidence thresholds
        thresholds = classification_rules.get('confidence_thresholds', {})
        min_confidence = thresholds.get('low_confidence', 0.4)
        
        if best_confidence < min_confidence:
            best_match = None
            best_semantic_tags = []
        
        return {
            'entity_type': best_match,
            'confidence': best_confidence,
            'semantic_tags': best_semantic_tags
        }
    
    def _extract_from_domain_rules(self, column: EnrichedColumnData, domain_rules: Dict[str, Any]) -> List[str]:
        """Extract key phrases from domain-specific rules."""
        
        phrases = []
        
        for rule_name, rule_config in domain_rules.items():
            if self._matches_rule(column, rule_config):
                rule_phrases = rule_config.get('key_phrases', [])
                phrases.extend(rule_phrases)
        
        return phrases
    
    def _matches_rule(self, column: EnrichedColumnData, rule_config: Dict[str, Any]) -> bool:
        """Check if a column matches a specific rule configuration."""
        
        # Check field name patterns
        field_patterns = rule_config.get('field_name_patterns', [])
        if field_patterns:
            field_match = any(pattern.lower() in column.name.lower() for pattern in field_patterns)
            if not field_match:
                return False
        
        # Check data type patterns
        type_patterns = rule_config.get('data_type_patterns', [])
        if type_patterns:
            type_match = any(pattern.lower() in column.type.lower() for pattern in type_patterns)
            if not type_match:
                return False
        
        # Skip pattern-based matching since we removed patterns field
        # Focus on field name and data type matching only
        
        return True
    
    def _calculate_entity_confidence(self, column: EnrichedColumnData, 
                                   entity_config: Dict[str, Any],
                                   classification_rules: Dict[str, Any]) -> float:
        """Calculate confidence score for entity type classification."""
        
        confidence = 0.0
        
        # Field name matching
        field_indicators = entity_config.get('field_indicators', [])
        if field_indicators:
            field_match = any(indicator.lower() in column.name.lower() for indicator in field_indicators)
            if field_match:
                confidence += 0.4
        
        # Data characteristics matching
        data_chars = entity_config.get('data_characteristics', {})
        
        if data_chars.get('typically_primary_key') and column.key_type == 'PK':
            confidence += 0.3
        if data_chars.get('often_foreign_key') and column.key_type == 'FK':
            confidence += 0.3
        if data_chars.get('high_uniqueness') and column.unique_pct > 80:
            confidence += 0.2
        if data_chars.get('low_nullability') and column.null_pct < 10:
            confidence += 0.1
        
        # Skip pattern-based matching since patterns are removed for clean profiling
        
        return min(confidence, 1.0)  # Cap at 1.0
    
    def _apply_phrase_selection_rules(self, phrases: List[str]) -> List[str]:
        """Apply phrase selection rules to filter and deduplicate key phrases."""
        
        selection_rules = self.key_phrases_config.get('phrase_selection_rules', {})
        max_phrases = selection_rules.get('max_phrases_per_field', 5)
        
        # Remove duplicates while preserving order
        unique_phrases = []
        seen = set()
        for phrase in phrases:
            if phrase.lower() not in seen:
                unique_phrases.append(phrase)
                seen.add(phrase.lower())
        
        # Limit to max phrases
        return unique_phrases[:max_phrases]
    
    def _create_enrichment_summary(self, enriched_columns: List[EnrichedColumnData]) -> Dict[str, Any]:
        """Create summary of enrichment results."""
        
        return {
            'total_columns': len(enriched_columns),
            'columns_with_key_phrases': len([col for col in enriched_columns if col.key_phrases]),
            'columns_with_entity_types': len([col for col in enriched_columns if col.entity_type]),
            'unique_entity_types': len(set(col.entity_type for col in enriched_columns if col.entity_type)),
            'avg_entity_confidence': sum(col.entity_confidence for col in enriched_columns) / len(enriched_columns) if enriched_columns else 0.0,
            'total_key_phrases': sum(len(col.key_phrases) for col in enriched_columns),
            'unique_semantic_tags': len(set(tag for col in enriched_columns for tag in col.semantic_tags))
        }


# Convenience functions
def enrich_minimal_column_summary(column_summary: Dict[str, Any], config_path: Optional[str] = None) -> Dict[str, Any]:
    """Convenience function to enrich a minimal column summary."""
    enricher = ColumnEnricher(config_path)
    return enricher.enrich_column_summary(column_summary)


def extract_key_phrases(column_data: Dict[str, Any], table_name: str = None, config_path: Optional[str] = None) -> List[str]:
    """Convenience function to extract key phrases for a single column."""
    enricher = ColumnEnricher(config_path)
    enriched = enricher.enrich_single_column(column_data, table_name)
    return enriched.key_phrases


def classify_entity_types(column_data: Dict[str, Any], table_name: str = None, config_path: Optional[str] = None) -> Dict[str, Any]:
    """Convenience function to classify entity type for a single column."""
    enricher = ColumnEnricher(config_path)
    enriched = enricher.enrich_single_column(column_data, table_name)
    return {
        'entity_type': enriched.entity_type,
        'confidence': enriched.entity_confidence,
        'semantic_tags': enriched.semantic_tags
    } 