#!/usr/bin/env python3
"""
Clean Column Enricher

A simplified enricher that focuses only on key phrases and entity types
without statistical noise or pattern dependencies.

Key Features:
- Clean key phrase extraction
- Entity type classification
- No statistical percentages
- No pattern dependencies
- Focused on semantic enrichment only
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime

from .config_loader import EnrichmentConfigLoader


@dataclass
class CleanEnrichedColumn:
    """Clean enriched column data without statistical noise."""
    
    # Core column information
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


class CleanColumnEnricher:
    """Clean column enricher focused on semantic enrichment only."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_loader = EnrichmentConfigLoader(config_path)
        self.logger = logging.getLogger(__name__)
        
        # Load configurations
        self.key_phrases_config = self.config_loader.load_key_phrases_config()
        self.entity_types_config = self.config_loader.load_entity_types_config()
        
        self.logger.info("CleanColumnEnricher initialized")
    
    def enrich_column_summary(self, column_summary: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich a minimal column summary with clean semantic data."""
        
        table_name = column_summary.get('table', 'unknown')
        enriched_columns = []
        
        for col in column_summary.get('columns', []):
            enriched_col = self.enrich_single_column(col, table_name)
            enriched_columns.append(enriched_col)
        
        return {
            'table': table_name,
            'total_columns': len(enriched_columns),
            'enriched_columns': [col.__dict__ for col in enriched_columns],
            'enrichment_summary': self._create_clean_summary(enriched_columns),
            'enrichment_timestamp': datetime.now().isoformat()
        }
    
    def enrich_single_column(self, column_data: Dict[str, Any], table_name: str = None) -> CleanEnrichedColumn:
        """Enrich a single column with clean semantic data."""
        
        # Create clean enriched column
        enriched = CleanEnrichedColumn(
            name=column_data.get('name', ''),
            type=column_data.get('type', ''),
            nullable=column_data.get('nullable', True),
            key_type=column_data.get('key_type'),
            table_name=table_name,
            full_identifier=f"{table_name}.{column_data.get('name', '')}" if table_name else column_data.get('name', '')
        )
        
        # Extract key phrases
        enriched.key_phrases = self.extract_clean_key_phrases(enriched)
        
        # Classify entity type
        entity_result = self.classify_clean_entity_type(enriched)
        enriched.entity_type = entity_result['entity_type']
        enriched.entity_confidence = entity_result['confidence']
        enriched.semantic_tags = entity_result['semantic_tags']
        
        return enriched
    
    def extract_clean_key_phrases(self, column: CleanEnrichedColumn) -> List[str]:
        """Extract key phrases based only on field name and data type."""
        
        key_phrases = []
        extraction_rules = self.key_phrases_config.get('key_phrase_extraction_rules', {})
        
        # Check healthcare domain rules
        healthcare_rules = extraction_rules.get('healthcare_domain', {})
        phrases = self._extract_from_clean_rules(column, healthcare_rules)
        key_phrases.extend(phrases)
        
        # Check general domain rules
        general_rules = extraction_rules.get('general_domain', {})
        phrases = self._extract_from_clean_rules(column, general_rules)
        key_phrases.extend(phrases)
        
        # Apply phrase selection rules
        key_phrases = self._apply_clean_phrase_selection(key_phrases)
        
        return key_phrases
    
    def classify_clean_entity_type(self, column: CleanEnrichedColumn) -> Dict[str, Any]:
        """Classify entity type based on clean field information only."""
        
        entity_types = self.entity_types_config.get('entity_types', {})
        
        best_match = None
        best_confidence = 0.0
        best_semantic_tags = []
        
        # Check all entity categories
        for category, entities in entity_types.items():
            for entity_key, entity_config in entities.items():
                confidence = self._calculate_clean_confidence(column, entity_config)
                
                if confidence > best_confidence:
                    best_confidence = confidence
                    best_match = entity_config.get('type')
                    best_semantic_tags = entity_config.get('semantic_tags', [])
        
        # Apply minimum confidence threshold
        if best_confidence < 0.4:
            best_match = None
            best_semantic_tags = []
        
        return {
            'entity_type': best_match,
            'confidence': best_confidence,
            'semantic_tags': best_semantic_tags
        }
    
    def _extract_from_clean_rules(self, column: CleanEnrichedColumn, domain_rules: Dict[str, Any]) -> List[str]:
        """Extract key phrases from domain rules using clean matching."""
        
        phrases = []
        
        for rule_name, rule_config in domain_rules.items():
            if self._matches_clean_rule(column, rule_config):
                rule_phrases = rule_config.get('key_phrases', [])
                phrases.extend(rule_phrases)
        
        return phrases
    
    def _matches_clean_rule(self, column: CleanEnrichedColumn, rule_config: Dict[str, Any]) -> bool:
        """Check if column matches rule based on field name and data type only."""
        
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
        
        return True
    
    def _calculate_clean_confidence(self, column: CleanEnrichedColumn, entity_config: Dict[str, Any]) -> float:
        """Calculate confidence based on clean field information only."""
        
        confidence = 0.0
        
        # Field name matching (primary indicator)
        field_indicators = entity_config.get('field_indicators', [])
        if field_indicators:
            field_match = any(indicator.lower() in column.name.lower() for indicator in field_indicators)
            if field_match:
                confidence += 0.6  # Higher weight for field name
        
        # Data type matching
        data_chars = entity_config.get('data_characteristics', {})
        
        # Key type matching
        if data_chars.get('typically_primary_key') and column.key_type == 'PK':
            confidence += 0.4
        elif data_chars.get('often_foreign_key') and column.key_type == 'FK':
            confidence += 0.4
        
        # Data type characteristics
        if column.type.lower() in ['varchar', 'text'] and data_chars.get('text_format'):
            confidence += 0.2
        elif column.type.lower() in ['int', 'bigint'] and data_chars.get('usually_numeric'):
            confidence += 0.2
        elif column.type.lower() in ['date', 'datetime', 'timestamp'] and data_chars.get('temporal_data'):
            confidence += 0.2
        
        return min(confidence, 1.0)  # Cap at 1.0
    
    def _apply_clean_phrase_selection(self, phrases: List[str]) -> List[str]:
        """Apply clean phrase selection rules."""
        
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
    
    def _create_clean_summary(self, enriched_columns: List[CleanEnrichedColumn]) -> Dict[str, Any]:
        """Create clean summary of enrichment results."""
        
        return {
            'total_columns': len(enriched_columns),
            'columns_with_key_phrases': len([col for col in enriched_columns if col.key_phrases]),
            'columns_with_entity_types': len([col for col in enriched_columns if col.entity_type]),
            'unique_entity_types': len(set(col.entity_type for col in enriched_columns if col.entity_type)),
            'avg_entity_confidence': sum(col.entity_confidence for col in enriched_columns) / len(enriched_columns) if enriched_columns else 0.0,
            'total_key_phrases': sum(len(col.key_phrases) for col in enriched_columns),
            'unique_semantic_tags': len(set(tag for col in enriched_columns for tag in col.semantic_tags))
        }


# Clean convenience functions
def enrich_clean_column_summary(column_summary: Dict[str, Any], config_path: Optional[str] = None) -> Dict[str, Any]:
    """Convenience function to cleanly enrich a minimal column summary."""
    enricher = CleanColumnEnricher(config_path)
    return enricher.enrich_column_summary(column_summary) 