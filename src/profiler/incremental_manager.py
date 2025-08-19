"""
Incremental Profiling Manager

This module provides comprehensive incremental profiling capabilities,
handling change detection, state management, and profile caching as
separate, focused concerns.
"""

import json
import hashlib
import logging
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, asdict

from .interfaces import IncrementalProfiler, SchemaProfiler, StateManager, ChangeDetector, ProfileCache, ProfilerConfig
from .schema_models import SchemaProfile, TableProfile


@dataclass
class TableChangeInfo:
    """Information about table changes for incremental profiling."""
    table_name: str
    schema_hash: str
    row_count: int
    last_modified: Optional[datetime] = None
    structure_changed: bool = False
    data_changed: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'table_name': self.table_name,
            'schema_hash': self.schema_hash,
            'row_count': self.row_count,
            'last_modified': self.last_modified.isoformat() if self.last_modified else None,
            'structure_changed': self.structure_changed,
            'data_changed': self.data_changed
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TableChangeInfo':
        """Create from dictionary."""
        return cls(
            table_name=data['table_name'],
            schema_hash=data['schema_hash'],
            row_count=data['row_count'],
            last_modified=datetime.fromisoformat(data['last_modified']) if data['last_modified'] else None,
            structure_changed=data.get('structure_changed', False),
            data_changed=data.get('data_changed', False)
        )


@dataclass
class IncrementalState:
    """State information for incremental profiling."""
    database_name: str
    schema_name: Optional[str]
    last_profile_timestamp: datetime
    table_states: Dict[str, TableChangeInfo]
    profile_version: str = "2.0"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'database_name': self.database_name,
            'schema_name': self.schema_name,
            'last_profile_timestamp': self.last_profile_timestamp.isoformat(),
            'table_states': {name: state.to_dict() for name, state in self.table_states.items()},
            'profile_version': self.profile_version
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'IncrementalState':
        """Create from dictionary loaded from JSON."""
        table_states = {}
        for name, state_data in data.get('table_states', {}).items():
            table_states[name] = TableChangeInfo.from_dict(state_data)
        
        return cls(
            database_name=data['database_name'],
            schema_name=data.get('schema_name'),
            last_profile_timestamp=datetime.fromisoformat(data['last_profile_timestamp']),
            table_states=table_states,
            profile_version=data.get('profile_version', '2.0')
        )


class FileStateManager(StateManager):
    """File-based state management for incremental profiling."""
    
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def load_state(self) -> Optional[Dict[str, Any]]:
        """Load incremental state from file."""
        try:
            if not self.file_path.exists():
                self.logger.info(f"No existing state found at {self.file_path}")
                return None
            
            with open(self.file_path, 'r', encoding='utf-8') as f:
                state_data = json.load(f)
            
            if not self.validate_state(state_data):
                self.logger.error("State validation failed, ignoring existing state")
                return None
            
            self.logger.info(f"Loaded incremental state from {self.file_path}")
            return state_data
            
        except Exception as e:
            self.logger.error(f"Error loading state: {e}")
            return None
    
    def save_state(self, state: Dict[str, Any]) -> None:
        """Save incremental state to file."""
        try:
            # Ensure directory exists
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Validate before saving
            if not self.validate_state(state):
                raise ValueError("Invalid state data")
            
            # Atomic write using temporary file
            temp_path = self.file_path.with_suffix('.tmp')
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
            
            # Atomic rename
            temp_path.replace(self.file_path)
            
            self.logger.debug(f"Saved incremental state to {self.file_path}")
            
        except Exception as e:
            self.logger.error(f"Error saving state: {e}")
            raise
    
    def validate_state(self, state: Dict[str, Any]) -> bool:
        """Validate loaded state data."""
        try:
            # Check required fields
            required_fields = ['database_name', 'last_profile_timestamp', 'table_states']
            for field in required_fields:
                if field not in state:
                    self.logger.error(f"Missing required field: {field}")
                    return False
            
            # Validate timestamp format
            datetime.fromisoformat(state['last_profile_timestamp'])
            
            # Validate table states
            if not isinstance(state['table_states'], dict):
                self.logger.error("table_states must be a dictionary")
                return False
            
            # Validate each table state
            for table_name, table_state in state['table_states'].items():
                if not isinstance(table_state, dict):
                    self.logger.error(f"Invalid table state for {table_name}")
                    return False
                
                required_table_fields = ['table_name', 'schema_hash', 'row_count']
                for field in required_table_fields:
                    if field not in table_state:
                        self.logger.error(f"Missing field {field} in table state for {table_name}")
                        return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"State validation error: {e}")
            return False


class DatabaseChangeDetector(ChangeDetector):
    """Database-aware change detection for incremental profiling."""
    
    def __init__(self, metadata_extractor):
        self.metadata_extractor = metadata_extractor
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def identify_changed_tables(self,
                               current_tables: List[Dict[str, Any]],
                               previous_state: Optional[Dict[str, Any]],
                               config: ProfilerConfig) -> List[Dict[str, Any]]:
        """Identify tables that need re-profiling."""
        if config.force_full_profile or not previous_state:
            self.logger.info("Full profiling requested or no previous state available")
            return current_tables
        
        try:
            # Parse previous state
            prev_state = IncrementalState.from_dict(previous_state)
            
            tables_to_profile = []
            current_table_names = {table['table_name'] for table in current_tables}
            previous_table_names = set(prev_state.table_states.keys())
            
            # Check for new tables
            new_tables = current_table_names - previous_table_names
            if new_tables:
                self.logger.info(f"New tables detected: {new_tables}")
                tables_to_profile.extend([t for t in current_tables if t['table_name'] in new_tables])
            
            # Check for removed tables (log only)
            removed_tables = previous_table_names - current_table_names
            if removed_tables:
                self.logger.info(f"Removed tables detected: {removed_tables}")
            
            # Check existing tables for changes
            for table_info in current_tables:
                table_name = table_info['table_name']
                
                if table_name in new_tables:
                    continue  # Already added
                
                if table_name not in prev_state.table_states:
                    continue  # Shouldn't happen, but be safe
                
                previous_table_state = prev_state.table_states[table_name]
                
                # Check schema structure changes
                if self._has_schema_changes(table_name, previous_table_state):
                    self.logger.info(f"Schema change detected for table: {table_name}")
                    tables_to_profile.append(table_info)
                    continue
                
                # Check data changes
                if self._has_data_changes(table_name, previous_table_state, config.data_change_threshold):
                    self.logger.info(f"Data change detected for table: {table_name}")
                    tables_to_profile.append(table_info)
                    continue
            
            self.logger.info(f"Change detection completed: {len(tables_to_profile)}/{len(current_tables)} tables need profiling")
            return tables_to_profile
            
        except Exception as e:
            self.logger.error(f"Error during change detection: {e}")
            # Fall back to full profiling on error
            return current_tables
    
    def _has_schema_changes(self, table_name: str, previous_state: TableChangeInfo) -> bool:
        """Check if table schema has changed."""
        try:
            current_hash = self._compute_table_schema_hash(table_name)
            return current_hash != previous_state.schema_hash
        except Exception as e:
            self.logger.warning(f"Error checking schema changes for {table_name}: {e}")
            return True  # Assume changed on error
    
    def _has_data_changes(self, table_name: str, previous_state: TableChangeInfo, threshold: float) -> bool:
        """Check if table data has changed significantly."""
        try:
            current_row_count = self.metadata_extractor.get_row_count(table_name)
            previous_row_count = previous_state.row_count
            
            if previous_row_count > 0:
                change_ratio = abs(current_row_count - previous_row_count) / previous_row_count
                return change_ratio > threshold
            elif current_row_count > 0:
                # Table was empty, now has data
                return True
            
            return False
            
        except Exception as e:
            self.logger.warning(f"Error checking data changes for {table_name}: {e}")
            return True  # Assume changed on error
    
    def _compute_table_schema_hash(self, table_name: str) -> str:
        """Compute a hash of the table's schema structure."""
        try:
            # Get table metadata for hashing
            columns = self.metadata_extractor.get_column_profiles(table_name)
            primary_keys = self.metadata_extractor.get_primary_keys(table_name)
            foreign_keys = self.metadata_extractor.get_foreign_keys(table_name)
            indexes = self.metadata_extractor.get_indexes(table_name)
            
            # Create deterministic representation
            schema_data = {
                'columns': [
                    {
                        'name': col.name,
                        'data_type': col.data_type,
                        'is_nullable': col.is_nullable,
                        'max_length': col.max_length,
                        'default_value': col.default_value,
                        'ordinal_position': col.ordinal_position
                    }
                    for col in sorted(columns, key=lambda x: x.ordinal_position)
                ],
                'primary_keys': sorted(primary_keys),
                'foreign_keys': sorted([
                    f"{fk['column_name']}->{fk['referenced_table']}.{fk['referenced_column']}"
                    for fk in foreign_keys
                ]),
                'indexes': sorted([
                    f"{idx['index_name']}:{idx['column_name']}:{idx.get('is_unique', False)}"
                    for idx in indexes
                ])
            }
            
            # Create hash
            schema_str = json.dumps(schema_data, sort_keys=True)
            return hashlib.md5(schema_str.encode()).hexdigest()
            
        except Exception as e:
            self.logger.warning(f"Error computing schema hash for {table_name}: {e}")
            return ""


class MemoryProfileCache(ProfileCache):
    """In-memory cache for table profiles."""
    
    def __init__(self, max_size_mb: int = 256):
        self.cache: Dict[str, TableProfile] = {}
        self.max_size_mb = max_size_mb
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_cached_profile(self, table_name: str) -> Optional[TableProfile]:
        """Get cached profile for a table."""
        return self.cache.get(table_name)
    
    def cache_profile(self, table_name: str, profile: TableProfile) -> None:
        """Cache a table profile."""
        # Simple caching - in production you'd implement size limits and LRU eviction
        self.cache[table_name] = profile
        self.logger.debug(f"Cached profile for table: {table_name}")
    
    def clear_cache(self) -> None:
        """Clear all cached profiles."""
        self.cache.clear()
        self.logger.info("Profile cache cleared")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            'cached_tables': len(self.cache),
            'max_size_mb': self.max_size_mb,
            'estimated_size_mb': len(self.cache) * 0.1  # Rough estimate
        }


class IncrementalProfilingManager(IncrementalProfiler):
    """Comprehensive incremental profiling manager."""
    
    def __init__(self, 
                 state_manager: StateManager,
                 change_detector: ChangeDetector,
                 profile_cache: Optional[ProfileCache] = None):
        self.state_manager = state_manager
        self.change_detector = change_detector
        self.profile_cache = profile_cache or MemoryProfileCache()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def profile_incremental(self, 
                           base_profiler: SchemaProfiler,
                           config: ProfilerConfig) -> SchemaProfile:
        """Perform incremental profiling using a base profiler."""
        self.logger.info(f"Starting incremental profiling for database: {config.database_name}")
        
        try:
            # Load previous state
            previous_state = self.state_manager.load_state()
            
            # Get current table information
            current_tables = base_profiler.get_tables_info()
            if not current_tables:
                self.logger.warning("No tables found to profile")
                return SchemaProfile(
                    database_name=config.database_name,
                    schema_name=config.schema_name,
                    database_type="unknown"
                )
            
            # Identify changed tables
            tables_to_profile = self.change_detector.identify_changed_tables(
                current_tables, previous_state, config
            )
            
            if not tables_to_profile:
                self.logger.info("No tables have changed since last profiling")
                return self._load_cached_schema_profile(config, current_tables)
            
            # Profile changed tables only
            self.logger.info(f"Profiling {len(tables_to_profile)} changed tables")
            changed_profiles = self._profile_changed_tables(base_profiler, tables_to_profile, config)
            
            # Merge with cached profiles
            complete_schema = self._merge_profiles(config, current_tables, changed_profiles, previous_state)
            
            # Update state
            self._update_state(config, current_tables, tables_to_profile)
            
            self.logger.info(f"Incremental profiling completed: {complete_schema.total_tables} tables, {complete_schema.total_columns} columns")
            return complete_schema
            
        except Exception as e:
            self.logger.error(f"Error during incremental profiling: {e}")
            # Fall back to full profiling
            self.logger.info("Falling back to full profiling")
            return base_profiler.profile_schema(config)
    
    def _profile_changed_tables(self, 
                               base_profiler: SchemaProfiler,
                               tables_to_profile: List[Dict[str, Any]],
                               config: ProfilerConfig) -> List[TableProfile]:
        """Profile only the changed tables."""
        profiles = []
        
        for table_info in tables_to_profile:
            table_name = table_info['table_name']
            try:
                profile = base_profiler.profile_table(table_name, config)
                profiles.append(profile)
                
                # Cache the new profile
                self.profile_cache.cache_profile(table_name, profile)
                
            except Exception as e:
                self.logger.error(f"Error profiling changed table {table_name}: {e}")
                continue
        
        return profiles
    
    def _load_cached_schema_profile(self, config: ProfilerConfig, current_tables: List[Dict[str, Any]]) -> SchemaProfile:
        """Load complete schema profile from cache."""
        # This is a placeholder - in a full implementation, you'd reconstruct
        # the complete schema from cached table profiles
        self.logger.info("Loading schema profile from cache (placeholder)")
        
        schema_profile = SchemaProfile(
            database_name=config.database_name,
            schema_name=config.schema_name,
            database_type="unknown",
            total_tables=len(current_tables)
        )
        
        # Load cached profiles
        for table_info in current_tables:
            table_name = table_info['table_name']
            cached_profile = self.profile_cache.get_cached_profile(table_name)
            if cached_profile:
                schema_profile.tables.append(cached_profile)
                schema_profile.total_columns += len(cached_profile.columns)
        
        return schema_profile
    
    def _merge_profiles(self, 
                       config: ProfilerConfig,
                       current_tables: List[Dict[str, Any]],
                       changed_profiles: List[TableProfile],
                       previous_state: Optional[Dict[str, Any]]) -> SchemaProfile:
        """Merge new profiles with cached profiles."""
        schema_profile = SchemaProfile(
            database_name=config.database_name,
            schema_name=config.schema_name,
            database_type="unknown",
            total_tables=len(current_tables)
        )
        
        changed_table_names = {p.name for p in changed_profiles}
        
        # Add changed profiles
        schema_profile.tables.extend(changed_profiles)
        
        # Add cached profiles for unchanged tables
        for table_info in current_tables:
            table_name = table_info['table_name']
            if table_name not in changed_table_names:
                cached_profile = self.profile_cache.get_cached_profile(table_name)
                if cached_profile:
                    schema_profile.tables.append(cached_profile)
        
        # Calculate total columns
        schema_profile.total_columns = sum(len(table.columns) for table in schema_profile.tables)
        
        # Sort tables for consistency
        schema_profile.tables.sort(key=lambda t: t.name)
        
        return schema_profile
    
    def _update_state(self, 
                     config: ProfilerConfig,
                     current_tables: List[Dict[str, Any]],
                     profiled_tables: List[Dict[str, Any]]) -> None:
        """Update the incremental profiling state."""
        try:
            # Load existing state or create new
            existing_state_data = self.state_manager.load_state()
            
            if existing_state_data:
                state = IncrementalState.from_dict(existing_state_data)
            else:
                state = IncrementalState(
                    database_name=config.database_name,
                    schema_name=config.schema_name,
                    last_profile_timestamp=datetime.now(),
                    table_states={}
                )
            
            # Update timestamp
            state.last_profile_timestamp = datetime.now()
            
            # Update table states
            profiled_table_names = {t['table_name'] for t in profiled_tables}
            
            for table_info in current_tables:
                table_name = table_info['table_name']
                
                # Compute current state
                schema_hash = self.change_detector._compute_table_schema_hash(table_name)
                row_count = self.change_detector.metadata_extractor.get_row_count(table_name)
                
                # Check if this table was profiled in this run
                was_profiled = table_name in profiled_table_names
                
                state.table_states[table_name] = TableChangeInfo(
                    table_name=table_name,
                    schema_hash=schema_hash,
                    row_count=row_count,
                    last_modified=datetime.now() if was_profiled else 
                                 state.table_states.get(table_name, TableChangeInfo(table_name, "", 0)).last_modified,
                    structure_changed=False,  # Reset after profiling
                    data_changed=False       # Reset after profiling
                )
            
            # Remove state for tables that no longer exist
            current_table_names = {table['table_name'] for table in current_tables}
            tables_to_remove = set(state.table_states.keys()) - current_table_names
            for table_name in tables_to_remove:
                del state.table_states[table_name]
                self.logger.info(f"Removed state for deleted table: {table_name}")
            
            # Save updated state
            self.state_manager.save_state(state.to_dict())
            
        except Exception as e:
            self.logger.error(f"Error updating incremental state: {e}")
            # Don't fail the whole operation for state update errors
            pass 