"""
Features Enrichment Package

This package provides functions to enrich minimal column summaries with key phrases
and entity types for improved embeddings and semantic understanding.

Key Features:
- Key phrase extraction based on configurable rules
- Entity type classification
- Semantic enrichment for embeddings
- Healthcare domain specialization
- Clean, modular architecture
"""

from .column_enricher import (
    ColumnEnricher,
    enrich_minimal_column_summary,
    extract_key_phrases,
    classify_entity_types,
    EnrichedColumnData
)

from .config_loader import (
    EnrichmentConfigLoader,
    load_key_phrases_config,
    load_entity_types_config
)

__all__ = [
    'ColumnEnricher',
    'EnrichedColumnData',
    'enrich_minimal_column_summary',
    'extract_key_phrases', 
    'classify_entity_types',
    'EnrichmentConfigLoader',
    'load_key_phrases_config',
    'load_entity_types_config'
] 