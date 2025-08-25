# Enhanced Pattern Field Recognizer - Schema Output Changes

## Overview

The updated `FieldPatternRecognizer` with the new `field_patterns.json` structure provides significantly enhanced pattern detection capabilities for schema profiling. This document outlines the key changes and improvements in the schema output profile.

## Key Improvements

### 1. **Priority-Based Pattern Matching**

- Patterns now have priority levels (1-6, where 1 is highest priority)
- Higher priority patterns are matched first and have lower confidence thresholds
- Example: Healthcare identifiers (priority 1) vs basic identifiers (priority 6)

### 2. **Enhanced Confidence Scoring**

- **Base Confidence**: Each pattern has a predefined confidence level (0.5-0.95)
- **Match Ratio**: Percentage of sample values that match the pattern
- **Combined Score**: `(base_confidence + match_ratio) / 2`
- **Field Name Bonus**: +0.15 confidence boost for field name matches
- **Dynamic Thresholds**: Threshold varies by priority (0.9 for priority 1, 0.4 for priority 6)

**Why Priority is Essential**
Overlapping Pattern Matches: A field named "provider_email" could match multiple patterns:

email_address pattern (matches "email")
provider_id pattern (matches "provider")
contact_information pattern (matches "email")

Without priority, system has no deterministic way to choose which classification is correct. Priority ensures healthcare-specific patterns (like NPI validation) take precedence over generic patterns (like basic email regex).
Processing Efficiency: Priority allows early termination. If a field matches a priority-1 healthcare identifier pattern with high confidence, no need to test 20+ lower-priority patterns.
**Why Confidence Scores Matter**
Ambiguous Field Names: Consider a field called "id" - it could be:

A patient identifier (high sensitivity)
A simple auto-increment key (low sensitivity)
A department code (medium sensitivity)

Confidence scores help distinguish between a definitive match (0.95 for exact "patient_id" match) versus a weak inference (0.50 for generic "\*\_id" pattern).
Audit Trail Requirements: Healthcare systems need to explain automated decisions. "This field was classified as PHI with 85% confidence based on name pattern matching" is actionable. "This field might be PHI" is not.

### 3. **Sensitivity Level Classification**

Patterns are now classified by data sensitivity:

- **phi_high**: High-risk PHI (patient names, SSN, medical records, diagnoses)
- **phi_limited**: Limited PHI (provider ID, email, phone, NPI)
- **confidential**: Business confidential (financial data, salaries)
- **internal**: Internal use only (employee ID, timestamps)
- **public**: Non-sensitive information

### 4. **Semantic Category Grouping**

Patterns are organized into meaningful categories:

- `healthcare_identifier` (NPI, provider IDs)
- `patient_identifier` (patient ID, MRN)
- `contact_information` (email, phone, address)
- `demographics` (names, date of birth)
- `clinical_data` (diagnosis codes, procedures, vitals)
- `financial` (amounts, insurance info)
- `temporal` (timestamps, dates)
- `operational` (status fields)
- And more...

### 5. **Advanced Field Name Matching**

- **Exact matches**: Direct field name comparison
- **Wildcard patterns**: Support for `*pattern*`, `*suffix`, `prefix*`
- **Substring matching**: Field names containing pattern keywords
- **Case-insensitive matching**: All comparisons are case-insensitive

### 6. **Valid Values Matching**

- Support for patterns defined by valid value lists (e.g., status fields)
- Example: `status` field matching ["active", "inactive", "pending", "completed", "cancelled"]

## Schema Output Profile Changes

### Before (Old Pattern Structure)

```json
{
  "name": "email",
  "data_type": "varchar",
  "detected_patterns": ["contact_information.email"],
  "sample_values": ["john@example.com", "jane@test.org"]
}
```

### After (Enhanced Pattern Structure)

```json
{
  "name": "email_address",
  "data_type": "varchar",
  "detected_patterns": [
    "contact_information.email_address",
    "contact_information.address_components"
  ],
  "sample_values": ["john.smith@email.com", "maria.garcia@healthcare.org"]
}
```

## Detailed Pattern Detection Examples

### 1. **NPI Identifier Detection**

```json
{
  "name": "npi",
  "detected_patterns": ["healthcare_identifier.npi_identifier"],
  "pattern_details": {
    "confidence": 1.0,
    "priority": 1,
    "semantic_category": "healthcare_identifier",
    "sensitivity_level": "phi_limited",
    "data_types": ["varchar", "char", "string"],
    "field_name_match": true,
    "match_ratio": 1.0
  }
}
```

### 2. **Email Address Detection**

```json
{
  "name": "email_address",
  "detected_patterns": ["contact_information.email_address"],
  "pattern_details": {
    "confidence": 1.0,
    "priority": 2,
    "semantic_category": "contact_information",
    "sensitivity_level": "phi_limited",
    "data_types": ["varchar", "text"],
    "field_name_match": true,
    "match_ratio": 1.0
  }
}
```

### 3. **Status Field Detection**

```json
{
  "name": "status",
  "detected_patterns": ["operational.status_field"],
  "pattern_details": {
    "confidence": 0.975,
    "priority": 5,
    "semantic_category": "operational",
    "sensitivity_level": "internal",
    "data_types": ["varchar", "boolean"],
    "field_name_match": true,
    "match_ratio": 1.0,
    "validation_method": "valid_values"
  }
}
```

## Pattern Statistics in Output

The enhanced recognizer now provides detailed statistics:

### By Sensitivity Level

- **phi_high**: 7 patterns (patient_id, person_name, date_of_birth, etc.)
- **phi_limited**: 6 patterns (npi_identifier, email_address, phone_number, etc.)
- **confidential**: 1 pattern (financial_amount)
- **internal**: 3 patterns (status_field, timestamp_field, department_info)
- **public**: 1 pattern (basic_id_fallback)

### By Semantic Category

- **healthcare_identifier**: NPI, provider IDs
- **patient_identifier**: Patient ID, MRN
- **contact_information**: Email, phone, address
- **demographics**: Names, date of birth
- **clinical_data**: Diagnosis codes, procedures, vitals
- **financial**: Insurance info, amounts
- **temporal**: Timestamps, dates
- **operational**: Status fields

## Benefits for LLM Processing

### 1. **Enhanced Context Understanding**

- Semantic categories provide domain context
- Sensitivity levels indicate data handling requirements
- Priority levels show importance hierarchy

### 2. **Improved Data Validation**

- Regex patterns for format validation
- Valid value lists for enumerated fields
- Data type awareness for compatibility

### 3. **Better Schema Mapping**

- Pattern-based field matching across schemas
- Confidence scores for mapping decisions
- Relationship inference from patterns

### 4. **Compliance and Security**

- Automatic PHI identification
- Sensitivity level classification
- Data protection requirement mapping

## Configuration Structure

The new `field_patterns.json` includes:

```json
{
  "healthcare_patterns": {
    "patterns": {
      "pattern_name": {
        "priority": 1,
        "confidence": 0.95,
        "regex": "^\\d{10}$",
        "field_names": ["npi", "provider_id"],
        "patterns": ["*npi*", "*provider*"],
        "semantic_category": "healthcare_identifier",
        "sensitivity_level": "phi_limited",
        "data_types": ["varchar", "char"],
        "valid_values": ["active", "inactive"],
        "validation": "luhn_check"
      }
    },
    "sensitivity_levels": {
      /* definitions */
    },
    "entity_relationships": {
      /* mappings */
    },
    "validation_rules": {
      /* validation logic */
    }
  }
}
```

## Migration Notes

### Backward Compatibility

- The `detected_patterns` field structure remains unchanged
- Existing LLM processing pipelines will continue to work
- Additional metadata is available but optional

### New Features Available

- `detect_patterns_with_confidence()` for detailed pattern info
- `get_sensitivity_levels()` for data classification
- `get_entity_relationships()` for schema relationships
- `validate_pattern_value()` for detailed validation

## Performance Improvements

### 1. **Efficient Pattern Matching**

- Compiled regex patterns cached for performance
- Early exit on high-confidence matches
- Configurable timeout limits (1000ms default)

### 2. **Thread Safety**

- Thread-safe pattern loading and caching
- Atomic updates for pattern reloading
- RLock protection for concurrent access

### 3. **Resource Management**

- Maximum 5 patterns tested per field
- Sample size limited to 10 values for performance
- Pattern result caching enabled

## Conclusion

The enhanced pattern field recognizer provides a comprehensive, healthcare-focused pattern detection system that significantly improves the quality and usefulness of schema profiling output. The structured approach with priorities, confidence scores, and sensitivity levels makes it ideal for LLM processing while maintaining backward compatibility.
