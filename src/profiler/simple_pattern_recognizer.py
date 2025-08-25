"""
Simple Pattern Recognition Module

This module provides simplified pattern recognition for database fields,
focusing only on obvious and easily detectable patterns to eliminate complexity.
"""

import json
import re
import logging
from typing import Dict, Any, List, Optional, Union
from pathlib import Path


class SimplePatternRecognizer:
    """
    Simple pattern recognizer focusing on obvious, high-confidence patterns only.
    
    Eliminates complex scoring, priority systems, and context awareness to
    provide straightforward pattern detection for clear cases.
    """
    
    def __init__(self, patterns_config_path: Optional[Union[str, Path]] = None):
        """
        Initialize the simple pattern recognizer.
        
        Args:
            patterns_config_path: Path to the patterns configuration JSON file.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.patterns: Dict[str, Dict[str, Any]] = {}
        self.compiled_patterns: Dict[str, re.Pattern[str]] = {}
        
        # Simple thresholds
        self.min_match_ratio = 0.8  # 80% of values must match
        self.min_sample_size = 3    # Need at least 3 values to test
        
        # Set default config path
        if patterns_config_path is None:
            patterns_config_path = Path(__file__).parent.parent.parent / "config" / "field_patterns.json"
        
        self.patterns_config_path = Path(patterns_config_path)
        self._load_patterns()
    
    def _load_patterns(self) -> None:
        """Load simplified pattern definitions."""
        try:
            if not self.patterns_config_path.exists():
                self.logger.warning(f"Pattern config file not found: {self.patterns_config_path}")
                return
            
            with open(self.patterns_config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Load patterns directly (no nested structure)
            self.patterns = config
            self._compile_regex_patterns()
            
            self.logger.info(f"Loaded {len(self.patterns)} simple patterns")
            
        except Exception as e:
            self.logger.error(f"Error loading patterns: {e}")
            self.patterns = {}
    
    def _compile_regex_patterns(self) -> None:
        """Compile regex patterns for performance."""
        self.compiled_patterns = {}
        for pattern_name, pattern_info in self.patterns.items():
            if 'regex' in pattern_info:
                try:
                    self.compiled_patterns[pattern_name] = re.compile(pattern_info['regex'])
                except re.error as e:
                    self.logger.warning(f"Invalid regex for {pattern_name}: {e}")
    
    def detect_patterns(self, values: List[Any], field_name: Optional[str] = None) -> List[str]:
        """
        Detect obvious patterns in field values.
        
        Args:
            values: List of values to analyze
            field_name: Field name for name-based matching
            
        Returns:
            List of detected pattern names (simple, no metadata)
        """
        if not values or len(values) < self.min_sample_size:
            return []
        
        detected = []
        
        # Convert values to strings
        string_values = [str(v).strip() for v in values if v is not None and str(v).strip()]
        if not string_values:
            return []
        
        # Test each pattern
        for pattern_name, pattern_info in self.patterns.items():
            if self._test_pattern(pattern_name, pattern_info, string_values, field_name):
                detected.append(pattern_name)
        
        # Remove conflicting patterns (keep most specific)
        return self._resolve_conflicts(detected, field_name)
    
    def _test_pattern(self, pattern_name: str, pattern_info: Dict[str, Any], 
                     values: List[str], field_name: Optional[str]) -> bool:
        """Test if a pattern matches the values."""
        
        field_name_match = field_name and self._matches_field_name(field_name, pattern_info)
        data_match = self._test_data_match(pattern_name, pattern_info, values, threshold=self.min_match_ratio)
        
        # For obvious detection, require BOTH field name AND data match for high confidence
        # OR very strong data match (95%+) without field name for patterns with regex
        if field_name_match and data_match:
            return True
        elif not field_name_match and data_match and 'regex' in pattern_info:
            # Only allow data-only matches for patterns with strong regex validation
            return self._test_data_match(pattern_name, pattern_info, values, threshold=0.95)
        
        return False
    
    def _matches_field_name(self, field_name: str, pattern_info: Dict[str, Any]) -> bool:
        """Check if field name matches pattern expectations."""
        field_lower = field_name.lower()
        
        # Exact field name match
        if 'field_names' in pattern_info:
            for expected_name in pattern_info['field_names']:
                if field_lower == expected_name.lower():
                    return True
        
        # Pattern matching (wildcards)
        if 'patterns' in pattern_info:
            for pattern in pattern_info['patterns']:
                if self._matches_wildcard_pattern(field_lower, pattern.lower()):
                    return True
        
        return False
    
    def _matches_wildcard_pattern(self, field_name: str, pattern: str) -> bool:
        """Simple wildcard pattern matching."""
        if pattern.startswith('*') and pattern.endswith('*'):
            # *text* - contains
            search_text = pattern[1:-1]
            return search_text in field_name
        elif pattern.startswith('*'):
            # *text - ends with
            suffix = pattern[1:]
            return field_name.endswith(suffix)
        elif pattern.endswith('*'):
            # text* - starts with
            prefix = pattern[:-1]
            return field_name.startswith(prefix)
        else:
            # exact match
            return field_name == pattern
    
    def _test_data_match(self, pattern_name: str, pattern_info: Dict[str, Any], 
                        values: List[str], threshold: float) -> bool:
        """Test if data values match the pattern."""
        sample_size = min(len(values), 10)  # Test up to 10 values
        matches = 0
        
        # Regex pattern matching
        if pattern_name in self.compiled_patterns:
            regex = self.compiled_patterns[pattern_name]
            for value in values[:sample_size]:
                if regex.match(value):
                    matches += 1
        
        # Valid values matching
        elif 'valid_values' in pattern_info:
            valid_values_lower = [v.lower() for v in pattern_info['valid_values']]
            for value in values[:sample_size]:
                if value.lower() in valid_values_lower:
                    matches += 1
        
        # No data validation available - rely on field name only
        else:
            return False
        
        match_ratio = matches / sample_size
        return match_ratio >= threshold
    
    def get_pattern_info(self, pattern_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific pattern."""
        return self.patterns.get(pattern_name)
    
    def get_available_patterns(self) -> List[str]:
        """Get list of all available patterns."""
        return list(self.patterns.keys())
    
    def validate_value(self, value: Any, pattern_name: str) -> bool:
        """Validate a single value against a pattern."""
        if pattern_name not in self.patterns:
            return False
        
        str_value = str(value).strip()
        if not str_value:
            return False
        
        pattern_info = self.patterns[pattern_name]
        
        # Test regex
        if pattern_name in self.compiled_patterns:
            return bool(self.compiled_patterns[pattern_name].match(str_value))
        
        # Test valid values
        if 'valid_values' in pattern_info:
            return str_value.lower() in [v.lower() for v in pattern_info['valid_values']]
        
        return False
    
    def _resolve_conflicts(self, detected: List[str], field_name: Optional[str]) -> List[str]:
        """Resolve conflicting patterns by keeping the most specific."""
        if len(detected) <= 1:
            return detected
        
        # Define pattern specificity (higher = more specific)
        specificity = {
            'npi_identifier': 10,
            'email_address': 9,
            'patient_id': 8,
            'provider_id': 8,
            'phone_number': 7,
            'status_field': 6,
            'person_name': 5,
            'date_of_birth': 5,
            'basic_id_fallback': 1  # Least specific
        }
        
        # Sort by specificity (highest first)
        detected_sorted = sorted(detected, key=lambda x: specificity.get(x, 0), reverse=True)
        
        # For very specific patterns, only return the most specific
        if detected_sorted[0] in ['npi_identifier', 'email_address']:
            return [detected_sorted[0]]
        
        # For other patterns, return top 2 if they're close in specificity
        result = [detected_sorted[0]]
        if (len(detected_sorted) > 1 and 
            specificity.get(detected_sorted[1], 0) >= specificity.get(detected_sorted[0], 0) - 2):
            result.append(detected_sorted[1])
        
        return result


# Backward compatibility - use SimplePatternRecognizer as the default
FieldPatternRecognizer = SimplePatternRecognizer 