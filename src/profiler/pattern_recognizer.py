"""
Field Pattern Recognition Module

This module provides pattern recognition capabilities for database fields,
particularly useful for identifying healthcare patterns, contact information,
identifiers, and other structured data patterns.

Classes:
- FieldPatternRecognizer: Main class for detecting patterns in field data
"""

import json
import re
import logging
from typing import Dict, Any, List, Optional, Set
from pathlib import Path


class FieldPatternRecognizer:
    """
    Recognizes patterns in database field data using configurable regex patterns.
    
    This class loads pattern definitions from JSON configuration files and applies
    them to field data to identify structured patterns like healthcare identifiers,
    contact information, and other domain-specific data formats.
    """
    
    def __init__(self, patterns_config_path: str = None):
        """
        Initialize the pattern recognizer.
        
        Args:
            patterns_config_path: Path to the patterns configuration JSON file.
                                 If None, uses default config/field_patterns.json
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.patterns = {}
        self.compiled_patterns = {}
        
        # Set default config path if not provided
        if patterns_config_path is None:
            patterns_config_path = Path(__file__).parent.parent.parent / "config" / "field_patterns.json"
        
        self.patterns_config_path = patterns_config_path
        self._load_patterns()
    
    def _load_patterns(self):
        """Load pattern definitions from the configuration file."""
        try:
            if not Path(self.patterns_config_path).exists():
                self.logger.warning(f"Pattern config file not found: {self.patterns_config_path}")
                self.patterns = {}
                return
            
            with open(self.patterns_config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Extract patterns from nested structure
            self.patterns = {}
            if 'healthcare_provider_patterns' in config:
                hcp_patterns = config['healthcare_provider_patterns']
                if 'pattern_categories' in hcp_patterns:
                    for category_name, category_data in hcp_patterns['pattern_categories'].items():
                        if 'patterns' in category_data:
                            for pattern_name, pattern_info in category_data['patterns'].items():
                                pattern_key = f"{category_name}.{pattern_name}"
                                self.patterns[pattern_key] = pattern_info
            
            # Compile regex patterns for better performance
            self._compile_patterns()
            
            self.logger.info(f"Loaded {len(self.patterns)} patterns from {self.patterns_config_path}")
            
        except Exception as e:
            self.logger.error(f"Error loading patterns from {self.patterns_config_path}: {e}")
            self.patterns = {}
    
    def _compile_patterns(self):
        """Compile regex patterns for better performance."""
        self.compiled_patterns = {}
        for pattern_key, pattern_info in self.patterns.items():
            try:
                if 'regex' in pattern_info:
                    self.compiled_patterns[pattern_key] = re.compile(pattern_info['regex'])
            except re.error as e:
                self.logger.warning(f"Invalid regex pattern for {pattern_key}: {e}")
    
    def detect_patterns(self, values: List[Any], field_name: str = None) -> List[str]:
        """
        Detect patterns in a list of field values.
        
        Args:
            values: List of values to analyze for patterns
            field_name: Optional field name for field-name-based matching
            
        Returns:
            List of detected pattern keys
        """
        if not values or not self.compiled_patterns:
            return []
        
        detected_patterns = set()
        
        # Convert values to strings and filter out None/empty values
        string_values = []
        for value in values:
            if value is not None:
                str_value = str(value).strip()
                if str_value:
                    string_values.append(str_value)
        
        if not string_values:
            return []
        
        # Test each pattern against the values
        for pattern_key, compiled_regex in self.compiled_patterns.items():
            pattern_info = self.patterns[pattern_key]
            
            # Check if field name matches expected field names
            if field_name and 'field_names' in pattern_info:
                field_name_lower = field_name.lower()
                expected_names = [name.lower() for name in pattern_info['field_names']]
                if not any(expected_name in field_name_lower for expected_name in expected_names):
                    continue
            
            # Test pattern against sample of values
            matches = 0
            sample_size = min(len(string_values), 10)  # Test up to 10 values
            
            for value in string_values[:sample_size]:
                if compiled_regex.match(value):
                    matches += 1
            
            # If majority of sampled values match, consider pattern detected
            match_ratio = matches / sample_size
            if match_ratio >= 0.7:  # 70% threshold
                detected_patterns.add(pattern_key)
                self.logger.debug(f"Pattern {pattern_key} detected with {match_ratio:.2%} match rate")
        
        return list(detected_patterns)
    
    def get_pattern_info(self, pattern_key: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a specific pattern.
        
        Args:
            pattern_key: The pattern identifier (e.g., "provider_identifiers.npi")
            
        Returns:
            Dictionary containing pattern information or None if not found
        """
        return self.patterns.get(pattern_key)
    
    def get_pattern_description(self, pattern_key: str) -> Optional[str]:
        """
        Get the description of a specific pattern.
        
        Args:
            pattern_key: The pattern identifier
            
        Returns:
            Pattern description string or None if not found
        """
        pattern_info = self.get_pattern_info(pattern_key)
        return pattern_info.get('description') if pattern_info else None
    
    def get_pattern_examples(self, pattern_key: str) -> List[str]:
        """
        Get example values for a specific pattern.
        
        Args:
            pattern_key: The pattern identifier
            
        Returns:
            List of example values or empty list if not found
        """
        pattern_info = self.get_pattern_info(pattern_key)
        return pattern_info.get('examples', []) if pattern_info else []
    
    def validate_value(self, value: Any, pattern_key: str) -> bool:
        """
        Validate a single value against a specific pattern.
        
        Args:
            value: The value to validate
            pattern_key: The pattern identifier to validate against
            
        Returns:
            True if value matches the pattern, False otherwise
        """
        if pattern_key not in self.compiled_patterns or value is None:
            return False
        
        str_value = str(value).strip()
        if not str_value:
            return False
        
        compiled_regex = self.compiled_patterns[pattern_key]
        return bool(compiled_regex.match(str_value))
    
    def get_available_patterns(self) -> List[str]:
        """
        Get list of all available pattern keys.
        
        Returns:
            List of pattern identifiers
        """
        return list(self.patterns.keys())
    
    def get_patterns_by_category(self, category: str) -> Dict[str, Dict[str, Any]]:
        """
        Get all patterns belonging to a specific category.
        
        Args:
            category: Category name (e.g., "provider_identifiers")
            
        Returns:
            Dictionary of patterns in the category
        """
        category_patterns = {}
        for pattern_key, pattern_info in self.patterns.items():
            if pattern_key.startswith(f"{category}."):
                category_patterns[pattern_key] = pattern_info
        return category_patterns
    
    def get_pattern_categories(self) -> Set[str]:
        """
        Get all available pattern categories.
        
        Returns:
            Set of category names
        """
        categories = set()
        for pattern_key in self.patterns.keys():
            if '.' in pattern_key:
                category = pattern_key.split('.')[0]
                categories.add(category)
        return categories
    
    def reload_patterns(self):
        """
        Reload patterns from the configuration file.
        
        Useful for updating patterns without restarting the application.
        """
        self.logger.info("Reloading pattern configurations...")
        self._load_patterns()
    
    def get_pattern_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about loaded patterns.
        
        Returns:
            Dictionary containing pattern statistics
        """
        categories = self.get_pattern_categories()
        category_counts = {}
        
        for category in categories:
            category_patterns = self.get_patterns_by_category(category)
            category_counts[category] = len(category_patterns)
        
        return {
            "total_patterns": len(self.patterns),
            "total_categories": len(categories),
            "categories": list(categories),
            "patterns_per_category": category_counts,
            "config_path": str(self.patterns_config_path)
        }
    
    def detect_patterns_with_confidence(self, values: List[Any], field_name: str = None) -> List[Dict[str, Any]]:
        """
        Detect patterns with confidence scores.
        
        Args:
            values: List of values to analyze for patterns
            field_name: Optional field name for field-name-based matching
            
        Returns:
            List of dictionaries containing pattern info and confidence scores
        """
        if not values or not self.compiled_patterns:
            return []
        
        detected_patterns = []
        
        # Convert values to strings and filter out None/empty values
        string_values = []
        for value in values:
            if value is not None:
                str_value = str(value).strip()
                if str_value:
                    string_values.append(str_value)
        
        if not string_values:
            return []
        
        # Test each pattern against the values
        for pattern_key, compiled_regex in self.compiled_patterns.items():
            pattern_info = self.patterns[pattern_key]
            
            # Check if field name matches expected field names
            field_name_match = False
            if field_name and 'field_names' in pattern_info:
                field_name_lower = field_name.lower()
                expected_names = [name.lower() for name in pattern_info['field_names']]
                field_name_match = any(expected_name in field_name_lower for expected_name in expected_names)
            
            # Test pattern against sample of values
            matches = 0
            sample_size = min(len(string_values), 10)  # Test up to 10 values
            
            for value in string_values[:sample_size]:
                if compiled_regex.match(value):
                    matches += 1
            
            # Calculate confidence score
            match_ratio = matches / sample_size
            confidence_score = match_ratio
            
            # Boost confidence if field name matches
            if field_name_match:
                confidence_score = min(1.0, confidence_score + 0.2)
            
            # Only include patterns with reasonable confidence
            if confidence_score >= 0.7:
                detected_patterns.append({
                    "pattern_key": pattern_key,
                    "confidence": round(confidence_score, 3),
                    "match_ratio": round(match_ratio, 3),
                    "matches": matches,
                    "sample_size": sample_size,
                    "field_name_match": field_name_match,
                    "description": pattern_info.get('description', ''),
                    "examples": pattern_info.get('examples', [])
                })
        
        # Sort by confidence score (highest first)
        detected_patterns.sort(key=lambda x: x['confidence'], reverse=True)
        
        return detected_patterns 