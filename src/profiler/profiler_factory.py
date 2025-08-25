"""
Profiler Factory

This module provides a factory for creating and configuring profiler components
with proper dependency injection and clean architecture.
"""

import logging
from typing import Optional

from .interfaces import ProfilerFactory, SchemaProfiler, IncrementalProfiler, TableProcessor, ProfilingStrategy
from .config import ProfilerConfig
from .core_profiler import CoreSchemaProfiler
from .processing_strategies import ProcessingStrategyFactory, PerformanceMonitor, ResourceManager
from .incremental_manager import (
    IncrementalProfilingManager, 
    FileStateManager, 
    DatabaseChangeDetector, 
    MemoryProfileCache
)
from .metadata_extractor import MetadataExtractor
from .simple_pattern_recognizer import SimplePatternRecognizer as FieldPatternRecognizer
from .schema_models import SchemaProfile


class DefaultProfilerFactory(ProfilerFactory):
    """Default implementation of the profiler factory."""
    
    def __init__(self, connector):
        self.connector = connector
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def create_profiler(self, config: ProfilerConfig) -> SchemaProfiler:
        """Create a schema profiler with the specified configuration."""
        # Create dependencies
        metadata_extractor = MetadataExtractor(
            self.connector, 
            config.database_name, 
            config.schema_name, 
            self._detect_database_type()
        )
        
        pattern_recognizer = None
        if config.pattern_recognition_enabled:
            pattern_recognizer = FieldPatternRecognizer(config.patterns_config_path)
        
        # Create core profiler
        core_profiler = CoreSchemaProfiler(
            self.connector, 
            metadata_extractor, 
            pattern_recognizer
        )
        
        # Create processing strategy
        table_processor = self.create_table_processor(config.strategy, core_profiler)
        
        # Create orchestrating profiler
        return OrchestatingProfiler(core_profiler, table_processor, config)
    
    def create_incremental_profiler(self, config: ProfilerConfig) -> IncrementalProfiler:
        """Create an incremental profiler."""
        if not config.incremental_enabled or not config.incremental_state_path:
            raise ValueError("Incremental profiling requires incremental_enabled=True and incremental_state_path")
        
        # Create state manager
        state_manager = FileStateManager(config.incremental_state_path)
        
        # Create change detector
        metadata_extractor = MetadataExtractor(
            self.connector,
            config.database_name,
            config.schema_name,
            self._detect_database_type()
        )
        change_detector = DatabaseChangeDetector(metadata_extractor)
        
        # Create profile cache
        profile_cache = MemoryProfileCache(max_size_mb=256)
        
        return IncrementalProfilingManager(state_manager, change_detector, profile_cache)
    
    def create_table_processor(self, strategy: ProfilingStrategy, core_profiler: Optional[CoreSchemaProfiler] = None) -> TableProcessor:
        """Create a table processor for the given strategy."""
        if not core_profiler:
            # Create a basic core profiler
            core_profiler = CoreSchemaProfiler(self.connector)
        
        return ProcessingStrategyFactory.create_processor(strategy, core_profiler)
    
    def _detect_database_type(self) -> str:
        """Detect database type from connector."""
        connector_class = self.connector.__class__.__name__
        if 'MySQL' in connector_class:
            return 'mysql'
        elif 'PostgreSQL' in connector_class:
            return 'postgresql'
        elif 'MSSQL' in connector_class:
            return 'mssql'
        else:
            return 'unknown'


class OrchestatingProfiler(SchemaProfiler):
    """
    Orchestrating profiler that coordinates core profiling with processing strategies.
    
    This class handles high-level orchestration while delegating specific concerns
    to focused components.
    """
    
    def __init__(self, core_profiler: CoreSchemaProfiler, table_processor: TableProcessor, config: ProfilerConfig):
        self.core_profiler = core_profiler
        self.table_processor = table_processor
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Optional components
        self.performance_monitor = PerformanceMonitor() if config.profile_performance else None
        self.resource_manager = ResourceManager(config)
    
    def profile_schema(self, config: ProfilerConfig) -> SchemaProfile:
        """Profile the complete database schema using the configured strategy."""
        self.logger.info(f"Starting orchestrated schema profiling for database: {config.database_name}")
        
        # Start performance monitoring if enabled
        if self.performance_monitor:
            self.performance_monitor.start_monitoring()
        
        try:
            # Get table information
            tables_info = self.core_profiler.get_tables_info()
            if not tables_info:
                self.logger.warning("No tables found to profile")
                return SchemaProfile(
                    database_name=config.database_name,
                    schema_name=config.schema_name,
                    database_type=self.core_profiler.db_type
                )
            
            self.logger.info(f"Found {len(tables_info)} tables to profile")
            self.logger.info(f"Using {self.table_processor.get_strategy_name()} processing strategy")
            
            # Process tables using the configured strategy
            table_profiles = self.table_processor.process_tables(tables_info, config)
            
            # Create schema profile
            schema_profile = SchemaProfile(
                database_name=config.database_name,
                schema_name=config.schema_name,
                database_type=self.core_profiler.db_type,
                total_tables=len(tables_info),
                total_columns=sum(len(table.columns) for table in table_profiles),
                tables=table_profiles
            )
            
            # Analyze relationships if enabled
            if config.validate_relationships:
                self._analyze_schema_relationships(schema_profile)
            
            # Export if configured
            if config.output_path:
                self.core_profiler.export_profile(schema_profile, config.output_path)
            
            self.logger.info(f"Schema profiling completed: {schema_profile.total_tables} tables, {schema_profile.total_columns} columns")
            
            return schema_profile
            
        except Exception as e:
            self.logger.error(f"Error during orchestrated schema profiling: {e}")
            raise
        
        finally:
            # End performance monitoring if enabled
            if self.performance_monitor:
                self.performance_monitor.end_monitoring()
                if config.debug_mode:
                    report = self.performance_monitor.get_performance_report()
                    self.logger.info(f"Performance report: {report}")
    
    def get_tables_info(self) -> list:
        """Get basic information about all tables."""
        return self.core_profiler.get_tables_info()
    
    def profile_table(self, table_name: str, config: ProfilerConfig):
        """Profile a single table."""
        return self.core_profiler.profile_table(table_name, config)
    
    def _analyze_schema_relationships(self, schema_profile: SchemaProfile) -> None:
        """Analyze cross-table relationships."""
        try:
            # Delegate to core profiler
            self.core_profiler._analyze_schema_relationships(schema_profile, self.config)
        except Exception as e:
            self.logger.error(f"Error analyzing relationships: {e}")
            # Set empty relationships rather than failing
            schema_profile.cross_table_relationships = []
            schema_profile.potential_relationships = []
            schema_profile.pattern_summary = {}


class UnifiedProfiler:
    """
    Unified profiler that provides both regular and incremental profiling capabilities.
    
    This is the main entry point that users interact with, providing a clean API
    that hides the complexity of the underlying architecture.
    """
    
    def __init__(self, connector, config: ProfilerConfig):
        self.connector = connector
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Create factory
        self.factory = DefaultProfilerFactory(connector)
        
        # Create profilers
        self.schema_profiler = self.factory.create_profiler(config)
        self.incremental_profiler = None
        
        if config.incremental_enabled:
            try:
                self.incremental_profiler = self.factory.create_incremental_profiler(config)
            except Exception as e:
                self.logger.warning(f"Could not create incremental profiler: {e}")
    
    def profile_schema(self, custom_config: Optional[ProfilerConfig] = None) -> SchemaProfile:
        """
        Profile the database schema.
        
        Args:
            custom_config: Optional custom configuration (uses instance config if None)
            
        Returns:
            Complete schema profile
        """
        config = custom_config or self.config
        
        if config.incremental_enabled and self.incremental_profiler:
            return self.incremental_profiler.profile_incremental(self.schema_profiler, config)
        else:
            return self.schema_profiler.profile_schema(config)
    
    def profile_schema_full(self, custom_config: Optional[ProfilerConfig] = None) -> SchemaProfile:
        """
        Force full schema profiling (bypass incremental).
        
        Args:
            custom_config: Optional custom configuration
            
        Returns:
            Complete schema profile
        """
        config = custom_config or self.config
        return self.schema_profiler.profile_schema(config)
    
    def profile_table(self, table_name: str, custom_config: Optional[ProfilerConfig] = None):
        """
        Profile a single table.
        
        Args:
            table_name: Name of the table to profile
            custom_config: Optional custom configuration
            
        Returns:
            Table profile
        """
        config = custom_config or self.config
        return self.schema_profiler.profile_table(table_name, config)
    
    def get_tables_info(self):
        """Get basic information about all tables."""
        return self.schema_profiler.get_tables_info()
    
    def export_profile(self, schema_profile: SchemaProfile, output_path: str):
        """Export schema profile to file."""
        return self.schema_profiler.export_profile(schema_profile, output_path)
    
    def get_performance_stats(self):
        """Get performance statistics if available."""
        if hasattr(self.schema_profiler, 'performance_monitor') and self.schema_profiler.performance_monitor:
            return self.schema_profiler.performance_monitor.get_performance_report()
        return None
    
    def get_resource_stats(self):
        """Get resource usage statistics if available."""
        if hasattr(self.schema_profiler, 'resource_manager'):
            return self.schema_profiler.resource_manager.get_resource_status()
        return None 