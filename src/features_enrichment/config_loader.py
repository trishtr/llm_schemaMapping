#!/usr/bin/env python3
"""
Enrichment Configuration Loader

This module provides utilities to load and manage enrichment configurations
for key phrase extraction and entity type classification.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class EnrichmentConfig:
    """Container for enrichment configuration data."""
    key_phrases_config: Dict[str, Any]
    entity_types_config: Dict[str, Any]
    config_metadata: Dict[str, Any]


class EnrichmentConfigLoader:
    """Loads and manages enrichment configurations."""
    
    def __init__(self, config_base_path: Optional[str] = None):
        if config_base_path is None:
            # Default to config/enrichment relative to this file
            self.config_base_path = Path(__file__).parent.parent.parent / "config" / "enrichment"
        else:
            self.config_base_path = Path(config_base_path)
        
        self.logger = logging.getLogger(__name__)
        self._cached_configs = {}
    
    def load_key_phrases_config(self) -> Dict[str, Any]:
        """Load key phrases extraction rules configuration."""
        
        config_file = self.config_base_path / "key_phrases_config.json"
        
        if 'key_phrases' not in self._cached_configs:
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                
                self._cached_configs['key_phrases'] = config
                self.logger.info(f"Loaded key phrases config from: {config_file}")
                
            except FileNotFoundError:
                self.logger.error(f"Key phrases config file not found: {config_file}")
                raise
            except json.JSONDecodeError as e:
                self.logger.error(f"Invalid JSON in key phrases config: {e}")
                raise
        
        return self._cached_configs['key_phrases']
    
    def load_entity_types_config(self) -> Dict[str, Any]:
        """Load entity types classification configuration."""
        
        config_file = self.config_base_path / "entity_types_config.json"
        
        if 'entity_types' not in self._cached_configs:
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                
                self._cached_configs['entity_types'] = config
                self.logger.info(f"Loaded entity types config from: {config_file}")
                
            except FileNotFoundError:
                self.logger.error(f"Entity types config file not found: {config_file}")
                raise
            except json.JSONDecodeError as e:
                self.logger.error(f"Invalid JSON in entity types config: {e}")
                raise
        
        return self._cached_configs['entity_types']
    
    def load_complete_config(self) -> EnrichmentConfig:
        """Load complete enrichment configuration."""
        
        key_phrases = self.load_key_phrases_config()
        entity_types = self.load_entity_types_config()
        
        metadata = {
            'config_path': str(self.config_base_path),
            'loaded_configs': ['key_phrases', 'entity_types'],
            'cache_status': len(self._cached_configs)
        }
        
        return EnrichmentConfig(
            key_phrases_config=key_phrases,
            entity_types_config=entity_types,
            config_metadata=metadata
        )
    
    def reload_configs(self) -> None:
        """Clear cache and reload all configurations."""
        self._cached_configs.clear()
        self.logger.info("Cleared configuration cache")
    
    def get_config_info(self) -> Dict[str, Any]:
        """Get information about loaded configurations."""
        
        info = {
            'config_base_path': str(self.config_base_path),
            'cached_configs': list(self._cached_configs.keys()),
            'config_files': {
                'key_phrases': (self.config_base_path / "key_phrases_config.json").exists(),
                'entity_types': (self.config_base_path / "entity_types_config.json").exists()
            }
        }
        
        return info


# Convenience functions
def load_key_phrases_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Convenience function to load key phrases configuration."""
    loader = EnrichmentConfigLoader(config_path)
    return loader.load_key_phrases_config()


def load_entity_types_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Convenience function to load entity types configuration."""
    loader = EnrichmentConfigLoader(config_path)
    return loader.load_entity_types_config() 