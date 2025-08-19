"""
Profiler Interfaces and Contracts

This module defines the core interfaces and contracts for the profiler architecture,
enabling clean separation of concerns and testability.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Protocol
from enum import Enum

from .schema_models import SchemaProfile, TableProfile


class ProfilingStrategy(Enum):
    """Enumeration of available schema profiling strategies."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    ADAPTIVE = "adaptive"


class ProfilerConfig(Protocol):
    """Configuration protocol for profilers."""
    max_workers: int
    parallel_threshold: int
    data_change_threshold: float
    incremental_enabled: bool
    incremental_state_path: Optional[str]


class TableProcessor(ABC):
    """Abstract interface for table processing strategies."""
    
    @abstractmethod
    def process_tables(self, 
                      tables_info: List[Dict[str, Any]], 
                      config: ProfilerConfig) -> List[TableProfile]:
        """
        Process a list of tables according to the strategy.
        
        Args:
            tables_info: List of table information dictionaries
            config: Processing configuration
            
        Returns:
            List of processed TableProfile objects
        """
        pass
    
    @abstractmethod
    def get_strategy_name(self) -> str:
        """Get the name of this processing strategy."""
        pass


class StateManager(ABC):
    """Abstract interface for incremental state management."""
    
    @abstractmethod
    def load_state(self) -> Optional[Dict[str, Any]]:
        """Load incremental state from storage."""
        pass
    
    @abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """Save incremental state to storage."""
        pass
    
    @abstractmethod
    def validate_state(self, state: Dict[str, Any]) -> bool:
        """Validate loaded state data."""
        pass


class ChangeDetector(ABC):
    """Abstract interface for detecting table changes."""
    
    @abstractmethod
    def identify_changed_tables(self,
                               current_tables: List[Dict[str, Any]],
                               previous_state: Optional[Dict[str, Any]],
                               config: ProfilerConfig) -> List[Dict[str, Any]]:
        """
        Identify which tables have changed since last profiling.
        
        Args:
            current_tables: Current table information
            previous_state: Previous profiling state
            config: Configuration for change detection
            
        Returns:
            List of tables that need re-profiling
        """
        pass


class ProfileCache(ABC):
    """Abstract interface for profile caching."""
    
    @abstractmethod
    def get_cached_profile(self, table_name: str) -> Optional[TableProfile]:
        """Get cached profile for a table."""
        pass
    
    @abstractmethod
    def cache_profile(self, table_name: str, profile: TableProfile) -> None:
        """Cache a table profile."""
        pass
    
    @abstractmethod
    def clear_cache(self) -> None:
        """Clear all cached profiles."""
        pass


class SchemaProfiler(ABC):
    """Abstract interface for schema profiling."""
    
    @abstractmethod
    def profile_schema(self, config: ProfilerConfig) -> SchemaProfile:
        """
        Profile the complete database schema.
        
        Args:
            config: Profiling configuration
            
        Returns:
            Complete schema profile
        """
        pass


class IncrementalProfiler(ABC):
    """Abstract interface for incremental profiling."""
    
    @abstractmethod
    def profile_incremental(self, 
                           base_profiler: SchemaProfiler,
                           config: ProfilerConfig) -> SchemaProfile:
        """
        Perform incremental profiling using a base profiler.
        
        Args:
            base_profiler: Base profiler for actual profiling work
            config: Profiling configuration
            
        Returns:
            Schema profile with incremental optimizations
        """
        pass


class ProfilerFactory(ABC):
    """Abstract factory for creating profiler components."""
    
    @abstractmethod
    def create_profiler(self, config: ProfilerConfig) -> SchemaProfiler:
        """Create a schema profiler."""
        pass
    
    @abstractmethod
    def create_incremental_profiler(self, config: ProfilerConfig) -> IncrementalProfiler:
        """Create an incremental profiler."""
        pass
    
    @abstractmethod
    def create_table_processor(self, strategy: ProfilingStrategy) -> TableProcessor:
        """Create a table processor for the given strategy."""
        pass 