"""
Database Schema Profiler Package

This package provides comprehensive database schema profiling capabilities including:
- Schema data profiling with parallel processing support
- Database dialect support for MySQL, PostgreSQL, and MSSQL
- Field pattern recognition for healthcare and general patterns
- Structural relationship analysis
- Data models for organizing schema information

Main Classes:
- SchemaDataProfiler: Main profiler class for comprehensive schema analysis
- DatabaseDialect: Database-specific SQL syntax and operations
- FieldPatternRecognizer: Advanced pattern recognition for field data

Data Models:
- ColumnProfile: Information about database columns
- TableProfile: Information about database tables
- SchemaProfile: Complete schema information
"""

from .schemaProfiler import SchemaDataProfiler

from .database_dialect import DatabaseDialect
from .pattern_recognizer import FieldPatternRecognizer
from .schema_models import (
    ColumnProfile,
    TableProfile,
    SchemaProfile
)

__all__ = [
    'SchemaDataProfiler',
    'ColumnProfile',
    'TableProfile', 
    'SchemaProfile',
    'FieldPatternRecognizer',
    'DatabaseDialect'
]

__version__ = "1.0.0" 