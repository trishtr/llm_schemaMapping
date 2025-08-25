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
import threading
from typing import Dict, Any, List, Optional, Set, Union, Tuple
from pathlib import Path


class FieldPatternRecognizer:
    """
    Recognizes patterns in database field data using configurable regex patterns.
    
    This class loads pattern definitions from JSON configuration files and applies
    them to field data to identify structured patterns like healthcare identifiers,
    contact information, and other domain-specific data formats.
    """
    
    def __init__(self, patterns_config_path: Optional[Union[str, Path]] = None, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the pattern recognizer.
        
        Args:
            patterns_config_path: Path to the patterns configuration JSON file.
                                 If None, uses default config/field_patterns.json
            config: Optional configuration overrides for thresholds and scoring
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.patterns: Dict[str, Dict[str, Any]] = {}
        self.compiled_patterns: Dict[str, re.Pattern[str]] = {}
        
        # Thread safety lock for pattern reloading
        self._patterns_lock = threading.RLock()
        
        # Configurable thresholds and scoring parameters
        self.config = {
            'match_ratio_threshold': 0.7,
            'early_termination_confidence': 0.95,
            'data_evidence_weight': 0.7,  # Higher weight for actual data matches
            'pattern_confidence_weight': 0.3,  # Lower weight for base confidence
            'field_name_bonus': 0.15,
            'max_sample_size': 10,
            'priority_threshold_base': 0.85,  # Higher priority = higher threshold
            'priority_threshold_step': 0.05,  # Decrease per priority level
            'enable_early_termination': True,
            'enable_composite_scoring': True
        }
        
        # Override with provided config
        if config:
            self.config.update(config)
        
        # Set default config path if not provided
        if patterns_config_path is None:
            patterns_config_path = Path(__file__).parent.parent.parent / "config" / "field_patterns.json"
        
        self.patterns_config_path = Path(patterns_config_path)
        self._load_patterns()
    
    def _load_patterns(self) -> None:
        """Load pattern definitions from the configuration file (thread-safe)."""
        with self._patterns_lock:
            try:
                if not self.patterns_config_path.exists():
                    self.logger.warning(f"Pattern config file not found: {self.patterns_config_path}")
                    self.patterns = {}
                    self.compiled_patterns = {}
                    return
                
                with open(self.patterns_config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                
                # Extract patterns from the new structure
                new_patterns: Dict[str, Dict[str, Any]] = {}
                if 'healthcare_patterns' in config:
                    hcp_patterns = config['healthcare_patterns']
                    if 'patterns' in hcp_patterns:
                        # New structure: healthcare_patterns.patterns directly contains pattern definitions
                        for pattern_name, pattern_info in hcp_patterns['patterns'].items():
                            # Use semantic_category as the category prefix if available
                            category = pattern_info.get('semantic_category', 'general')
                            pattern_key = f"{category}.{pattern_name}"
                            new_patterns[pattern_key] = pattern_info
                
                # Atomic update of patterns
                self.patterns = new_patterns
                
                # Compile regex patterns for better performance
                self._compile_patterns()
                
                self.logger.info(f"Loaded {len(self.patterns)} patterns from {self.patterns_config_path}")
                
            except Exception as e:
                self.logger.error(f"Error loading patterns from {self.patterns_config_path}: {e}")
                self.patterns = {}
                self.compiled_patterns = {}
    
    def _compile_patterns(self) -> None:
        """Compile regex patterns for better performance (must be called within lock)."""
        new_compiled_patterns: Dict[str, re.Pattern[str]] = {}
        for pattern_key, pattern_info in self.patterns.items():
            try:
                if 'regex' in pattern_info:
                    new_compiled_patterns[pattern_key] = re.compile(pattern_info['regex'])
            except re.error as e:
                self.logger.warning(f"Invalid regex pattern for {pattern_key}: {e}")
        
        # Atomic update of compiled patterns
        self.compiled_patterns = new_compiled_patterns
    
    def _check_field_name_match(self, field_name: Optional[str], pattern_info: Dict[str, Any]) -> bool:
        """
        Check if field name matches the pattern's field name criteria.
        
        Args:
            field_name: The field name to check
            pattern_info: Pattern information dictionary
            
        Returns:
            True if field name matches, False otherwise
        """
        if not field_name:
            return True  # No field name provided, so don't filter
        
        field_name_lower = field_name.lower()
        
        # Check exact field name matches
        if 'field_names' in pattern_info:
            expected_names = [name.lower() for name in pattern_info['field_names']]
            if any(expected_name == field_name_lower for expected_name in expected_names):
                return True
            # Also check if field name contains any of the expected names
            if any(expected_name in field_name_lower for expected_name in expected_names):
                return True
        
        # Check pattern-based matching (new feature)
        if 'patterns' in pattern_info:
            for pattern in pattern_info['patterns']:
                pattern_lower = pattern.lower()
                if pattern_lower.startswith('*') and pattern_lower.endswith('*'):
                    # Wildcard pattern: *text*
                    search_text = pattern_lower[1:-1]
                    if search_text in field_name_lower:
                        return True
                elif pattern_lower.startswith('*'):
                    # Suffix pattern: *text
                    suffix = pattern_lower[1:]
                    if field_name_lower.endswith(suffix):
                        return True
                elif pattern_lower.endswith('*'):
                    # Prefix pattern: text*
                    prefix = pattern_lower[:-1]
                    if field_name_lower.startswith(prefix):
                        return True
                else:
                    # Exact match
                    if pattern_lower == field_name_lower:
                        return True
        
        return False
    
    def detect_patterns(self, values: List[Any], field_name: Optional[str] = None) -> List[str]:
        """
        Detect patterns in a list of field values (thread-safe).
        
        Args:
            values: List of values to analyze for patterns
            field_name: Optional field name for field-name-based matching
            
        Returns:
            List of detected pattern keys
        """
        if not values:
            return []
        
        # Create thread-safe snapshots of patterns
        with self._patterns_lock:
            if not self.compiled_patterns:
                return []
            compiled_patterns_snapshot = self.compiled_patterns.copy()
            patterns_snapshot = self.patterns.copy()
        
        detected_patterns: Set[str] = set()
        
        # Convert values to strings and filter out None/empty values
        string_values: List[str] = []
        for value in values:
            if value is not None:
                str_value = str(value).strip()
                if str_value:
                    string_values.append(str_value)
        
        if not string_values:
            return []
        
        # Test each pattern against the values
        for pattern_key, pattern_info in patterns_snapshot.items():
            
            # Check if field name matches expected field names or patterns
            field_name_match = self._check_field_name_match(field_name, pattern_info)
            if field_name and not field_name_match:
                continue
            
            # Test regex pattern if available
            regex_matches = 0
            if pattern_key in compiled_patterns_snapshot:
                compiled_regex = compiled_patterns_snapshot[pattern_key]
                sample_size = min(len(string_values), 10)  # Test up to 10 values
                
                for value in string_values[:sample_size]:
                    if compiled_regex.match(value):
                        regex_matches += 1
                
                # If majority of sampled values match, consider pattern detected
                match_ratio = regex_matches / sample_size
                if match_ratio >= 0.7:  # 70% threshold
                    detected_patterns.add(pattern_key)
                    self.logger.debug(f"Pattern {pattern_key} detected with {match_ratio:.2%} match rate")
            
            # Also check for valid_values if available (for status fields, etc.)
            elif 'valid_values' in pattern_info and field_name_match:
                valid_values = [str(v).lower() for v in pattern_info['valid_values']]
                value_matches = sum(1 for v in string_values[:10] if str(v).lower() in valid_values)
                if value_matches > 0 and (value_matches / min(len(string_values), 10)) >= 0.5:
                    detected_patterns.add(pattern_key)
                    self.logger.debug(f"Pattern {pattern_key} detected by valid values match")
        
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
    
    def reload_patterns(self) -> None:
        """
        Reload patterns from the configuration file (thread-safe).
        
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
    
    def detect_patterns_with_confidence(self, values: List[Any], field_name: Optional[str] = None, 
                                       table_context: Optional[Dict[str, Any]] = None) -> List[Dict[str, Union[str, float, int, bool, List[str]]]]:
        """
        Detect patterns with improved confidence scores and context awareness.
        
        Args:
            values: List of values to analyze for patterns
            field_name: Optional field name for field-name-based matching
            table_context: Optional table-level context (table_name, schema, etc.)
            
        Returns:
            List of dictionaries containing pattern info and confidence scores
        """
        if not values:
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
        
        # Sort patterns by priority for early termination
        sorted_patterns = sorted(self.patterns.items(), key=lambda x: x[1].get('priority', 5))
        
        # Test each pattern against the values (in priority order)
        for pattern_key, pattern_info in sorted_patterns:
            
            # Check if field name matches expected field names or patterns
            field_name_match = self._check_field_name_match(field_name, pattern_info)
            if field_name and not field_name_match:
                continue
            
            # Calculate match score
            match_result = self._calculate_match_score(string_values, pattern_key, pattern_info)
            if match_result is None:
                continue
                
            matches, sample_size, match_ratio = match_result
            
            # Calculate improved confidence score
            confidence_score = self._calculate_improved_confidence(
                pattern_info, match_ratio, field_name_match, table_context
            )
            
            # Get priority-based threshold (higher priority = higher threshold)
            priority = pattern_info.get('priority', 5)
            confidence_threshold = self._get_priority_threshold(priority)
            
            # Only include patterns with reasonable confidence
            if confidence_score >= confidence_threshold:
                pattern_result = {
                    "pattern_key": pattern_key,
                    "confidence": round(confidence_score, 3),
                    "match_ratio": round(match_ratio, 3),
                    "matches": matches,
                    "sample_size": sample_size,
                    "field_name_match": field_name_match,
                    "priority": priority,
                    "semantic_category": pattern_info.get('semantic_category', 'unknown'),
                    "sensitivity_level": pattern_info.get('sensitivity_level', 'public'),
                    "data_types": pattern_info.get('data_types', []),
                    "description": pattern_info.get('description', ''),
                    "examples": pattern_info.get('examples', []),
                    "confidence_threshold": confidence_threshold
                }
                
                detected_patterns.append(pattern_result)
                
                # Early termination for high-confidence, high-priority matches
                if (self.config['enable_early_termination'] and 
                    priority <= 2 and 
                    confidence_score >= self.config['early_termination_confidence']):
                    self.logger.debug(f"Early termination: {pattern_key} with confidence {confidence_score:.3f}")
                    break
        
        # Apply composite scoring if enabled
        if self.config['enable_composite_scoring']:
            detected_patterns = self._apply_composite_scoring(detected_patterns)
        
        # Sort by priority first (lower number = higher priority), then by confidence
        detected_patterns.sort(key=lambda x: (x['priority'], -x['confidence']))
        
        return detected_patterns
    
    def get_sensitivity_levels(self) -> Dict[str, Dict[str, Any]]:
        """
        Get sensitivity level definitions from the configuration.
        
        Returns:
            Dictionary of sensitivity levels and their properties
        """
        with self._patterns_lock:
            try:
                if not self.patterns_config_path.exists():
                    return {}
                
                with open(self.patterns_config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                
                if 'healthcare_patterns' in config and 'sensitivity_levels' in config['healthcare_patterns']:
                    return config['healthcare_patterns']['sensitivity_levels']
                
            except Exception as e:
                self.logger.error(f"Error loading sensitivity levels: {e}")
        
        return {}
    
    def get_pattern_by_sensitivity(self, sensitivity_level: str) -> List[str]:
        """
        Get all patterns with a specific sensitivity level.
        
        Args:
            sensitivity_level: The sensitivity level to filter by
            
        Returns:
            List of pattern keys with the specified sensitivity level
        """
        matching_patterns = []
        for pattern_key, pattern_info in self.patterns.items():
            if pattern_info.get('sensitivity_level') == sensitivity_level:
                matching_patterns.append(pattern_key)
        
        return matching_patterns
    
    def get_entity_relationships(self) -> Dict[str, Dict[str, Any]]:
        """
        Get entity relationship definitions from the configuration.
        
        Returns:
            Dictionary of entity relationships
        """
        with self._patterns_lock:
            try:
                if not self.patterns_config_path.exists():
                    return {}
                
                with open(self.patterns_config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                
                if 'healthcare_patterns' in config and 'entity_relationships' in config['healthcare_patterns']:
                    return config['healthcare_patterns']['entity_relationships']
                
            except Exception as e:
                self.logger.error(f"Error loading entity relationships: {e}")
        
        return {}
    
    def validate_pattern_value(self, value: Any, pattern_key: str) -> Dict[str, Any]:
        """
        Validate a value against a pattern with detailed results.
        
        Args:
            value: The value to validate
            pattern_key: The pattern to validate against
            
        Returns:
            Dictionary with validation results and details
        """
        result = {
            'is_valid': False,
            'pattern_key': pattern_key,
            'value': value,
            'validation_details': {}
        }
        
        if pattern_key not in self.patterns:
            result['validation_details']['error'] = f"Pattern {pattern_key} not found"
            return result
        
        pattern_info = self.patterns[pattern_key]
        str_value = str(value).strip() if value is not None else ""
        
        if not str_value:
            result['validation_details']['error'] = "Empty value"
            return result
        
        # Check regex validation
        if pattern_key in self.compiled_patterns:
            compiled_regex = self.compiled_patterns[pattern_key]
            if compiled_regex.match(str_value):
                result['is_valid'] = True
                result['validation_details']['regex_match'] = True
        
        # Check valid_values validation
        elif 'valid_values' in pattern_info:
            valid_values = [str(v).lower() for v in pattern_info['valid_values']]
            if str_value.lower() in valid_values:
                result['is_valid'] = True
                result['validation_details']['valid_value_match'] = True
        
        # Add pattern metadata
        result['validation_details'].update({
            'semantic_category': pattern_info.get('semantic_category'),
            'sensitivity_level': pattern_info.get('sensitivity_level'),
            'priority': pattern_info.get('priority'),
            'data_types': pattern_info.get('data_types', [])
        })
        
        return result
    
    def _calculate_match_score(self, string_values: List[str], pattern_key: str, pattern_info: Dict[str, Any]) -> Optional[Tuple[int, int, float]]:
        """Calculate match score for a pattern against values."""
        compiled_regex = self.compiled_patterns.get(pattern_key)
        sample_size = min(len(string_values), self.config['max_sample_size'])
        matches = 0
        
        if compiled_regex:
            for value in string_values[:sample_size]:
                if compiled_regex.match(value):
                    matches += 1
        elif 'valid_values' in pattern_info:
            # Handle patterns with valid_values instead of regex
            valid_values = [str(v).lower() for v in pattern_info['valid_values']]
            matches = sum(1 for v in string_values[:sample_size] if str(v).lower() in valid_values)
        else:
            return None
        
        match_ratio = matches / sample_size if sample_size > 0 else 0
        
        # Only proceed if match ratio meets threshold
        if match_ratio < self.config['match_ratio_threshold']:
            return None
            
        return matches, sample_size, match_ratio
    
    def _calculate_improved_confidence(self, pattern_info: Dict[str, Any], match_ratio: float, 
                                     field_name_match: bool, table_context: Optional[Dict[str, Any]] = None) -> float:
        """Calculate improved confidence score prioritizing data evidence."""
        base_confidence = pattern_info.get('confidence', 0.5)
        
        # Weighted combination: prioritize data evidence over base confidence
        confidence_score = (
            match_ratio * self.config['data_evidence_weight'] + 
            base_confidence * self.config['pattern_confidence_weight']
        )
        
        # Field name bonus
        if field_name_match:
            confidence_score = min(1.0, confidence_score + self.config['field_name_bonus'])
        
        # Table context bonus (if healthcare table contains healthcare patterns)
        if table_context:
            table_name = table_context.get('table_name', '').lower()
            semantic_category = pattern_info.get('semantic_category', '')
            
            # Healthcare context bonus
            if ('patient' in table_name or 'provider' in table_name or 'medical' in table_name):
                if semantic_category in ['healthcare_identifier', 'patient_identifier', 'clinical_data']:
                    confidence_score = min(1.0, confidence_score + 0.1)
        
        return confidence_score
    
    def _get_priority_threshold(self, priority: int) -> float:
        """Get confidence threshold based on priority (higher priority = higher threshold)."""
        # Higher priority patterns should have higher thresholds (more stringent)
        threshold = self.config['priority_threshold_base'] - (priority - 1) * self.config['priority_threshold_step']
        return max(0.3, min(0.95, threshold))
    
    def _apply_composite_scoring(self, detected_patterns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply composite scoring for multiple pattern matches."""
        if len(detected_patterns) <= 1:
            return detected_patterns
        
        # Group patterns by semantic category
        category_groups = {}
        for pattern in detected_patterns:
            category = pattern['semantic_category']
            if category not in category_groups:
                category_groups[category] = []
            category_groups[category].append(pattern)
        
        # Apply composite scoring within each category
        enhanced_patterns = []
        for category, patterns in category_groups.items():
            if len(patterns) > 1:
                # Boost confidence for multiple patterns in same category
                for pattern in patterns:
                    pattern['composite_boost'] = 0.05 * (len(patterns) - 1)
                    pattern['confidence'] = min(1.0, pattern['confidence'] + pattern['composite_boost'])
            enhanced_patterns.extend(patterns)
        
        return enhanced_patterns 