"""
Profiler Configuration Management

This module provides centralized configuration management for all profiler components,
replacing scattered parameters with a clean configuration system.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from pathlib import Path

from .interfaces import ProfilingStrategy


@dataclass
class ProfilerConfig:
    """
    Centralized configuration for schema profiling operations.
    
    This replaces scattered method parameters with a single configuration object
    that can be easily validated, serialized, and passed between components.
    """
    
    # Core profiling settings
    database_name: str
    schema_name: Optional[str] = None
    
    # Processing strategy settings
    strategy: ProfilingStrategy = ProfilingStrategy.ADAPTIVE
    max_workers: int = 4
    parallel_threshold: int = 10
    
    # Incremental profiling settings
    incremental_enabled: bool = False
    incremental_state_path: Optional[str] = None
    data_change_threshold: float = 0.1  # 10% row count change
    force_full_profile: bool = False
    
    # Resource limits
    max_connections: int = 10
    query_timeout: int = 300  # 5 minutes
    memory_limit_mb: int = 1024  # 1GB
    
    # Pattern recognition settings
    pattern_recognition_enabled: bool = True
    patterns_config_path: Optional[str] = None
    
    # Logging and debugging
    log_level: str = "INFO"
    debug_mode: bool = False
    profile_performance: bool = False
    
    # Output settings
    export_format: str = "json"
    output_path: Optional[str] = None
    include_sample_data: bool = True
    sample_data_limit: int = 5
    
    # Validation settings
    validate_relationships: bool = True
    validate_patterns: bool = True
    strict_mode: bool = False
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate configuration values."""
        if self.max_workers < 1:
            raise ValueError("max_workers must be at least 1")
        
        if self.parallel_threshold < 1:
            raise ValueError("parallel_threshold must be at least 1")
        
        if not 0.0 <= self.data_change_threshold <= 1.0:
            raise ValueError("data_change_threshold must be between 0.0 and 1.0")
        
        if self.query_timeout < 1:
            raise ValueError("query_timeout must be at least 1 second")
        
        if self.memory_limit_mb < 64:
            raise ValueError("memory_limit_mb must be at least 64MB")
        
        if self.sample_data_limit < 0:
            raise ValueError("sample_data_limit must be non-negative")
        
        if self.incremental_enabled and not self.incremental_state_path:
            raise ValueError("incremental_state_path required when incremental_enabled=True")
        
        if self.log_level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise ValueError(f"Invalid log_level: {self.log_level}")
        
        if self.export_format not in ["json", "yaml", "xml"]:
            raise ValueError(f"Invalid export_format: {self.export_format}")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'database_name': self.database_name,
            'schema_name': self.schema_name,
            'strategy': self.strategy.value,
            'max_workers': self.max_workers,
            'parallel_threshold': self.parallel_threshold,
            'incremental_enabled': self.incremental_enabled,
            'incremental_state_path': self.incremental_state_path,
            'data_change_threshold': self.data_change_threshold,
            'force_full_profile': self.force_full_profile,
            'max_connections': self.max_connections,
            'query_timeout': self.query_timeout,
            'memory_limit_mb': self.memory_limit_mb,
            'pattern_recognition_enabled': self.pattern_recognition_enabled,
            'patterns_config_path': self.patterns_config_path,
            'log_level': self.log_level,
            'debug_mode': self.debug_mode,
            'profile_performance': self.profile_performance,
            'export_format': self.export_format,
            'output_path': self.output_path,
            'include_sample_data': self.include_sample_data,
            'sample_data_limit': self.sample_data_limit,
            'validate_relationships': self.validate_relationships,
            'validate_patterns': self.validate_patterns,
            'strict_mode': self.strict_mode
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ProfilerConfig':
        """Create configuration from dictionary."""
        # Convert strategy string back to enum
        if 'strategy' in data and isinstance(data['strategy'], str):
            data['strategy'] = ProfilingStrategy(data['strategy'])
        
        return cls(**data)
    
    @classmethod
    def from_file(cls, file_path: str) -> 'ProfilerConfig':
        """Load configuration from JSON file."""
        import json
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return cls.from_dict(data)
    
    def save_to_file(self, file_path: str) -> None:
        """Save configuration to JSON file."""
        import json
        
        # Ensure directory exists
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)
    
    def copy(self, **changes) -> 'ProfilerConfig':
        """Create a copy of the configuration with optional changes."""
        current_dict = self.to_dict()
        current_dict.update(changes)
        return self.from_dict(current_dict)
    
    def get_incremental_config(self) -> 'IncrementalConfig':
        """Extract incremental-specific configuration."""
        return IncrementalConfig(
            enabled=self.incremental_enabled,
            state_path=self.incremental_state_path,
            data_change_threshold=self.data_change_threshold,
            force_full_profile=self.force_full_profile
        )
    
    def get_processing_config(self) -> 'ProcessingConfig':
        """Extract processing-specific configuration."""
        return ProcessingConfig(
            strategy=self.strategy,
            max_workers=self.max_workers,
            parallel_threshold=self.parallel_threshold,
            max_connections=self.max_connections,
            query_timeout=self.query_timeout,
            memory_limit_mb=self.memory_limit_mb
        )


@dataclass
class IncrementalConfig:
    """Configuration specific to incremental profiling."""
    enabled: bool = False
    state_path: Optional[str] = None
    data_change_threshold: float = 0.1
    force_full_profile: bool = False
    cache_profiles: bool = True
    max_cache_size_mb: int = 256
    cache_ttl_hours: int = 24


@dataclass
class ProcessingConfig:
    """Configuration specific to processing strategies."""
    strategy: ProfilingStrategy = ProfilingStrategy.ADAPTIVE
    max_workers: int = 4
    parallel_threshold: int = 10
    max_connections: int = 10
    query_timeout: int = 300
    memory_limit_mb: int = 1024
    batch_size: int = 100
    retry_attempts: int = 3
    retry_delay_seconds: int = 1


@dataclass
class PatternConfig:
    """Configuration specific to pattern recognition."""
    enabled: bool = True
    config_path: Optional[str] = None
    confidence_threshold: float = 0.7
    max_sample_values: int = 10
    enable_field_name_matching: bool = True
    enable_regex_patterns: bool = True
    custom_patterns: Dict[str, str] = field(default_factory=dict)


class ConfigBuilder:
    """Builder pattern for creating profiler configurations."""
    
    def __init__(self, database_name: str):
        self._config_dict = {'database_name': database_name}
    
    def with_schema(self, schema_name: str) -> 'ConfigBuilder':
        """Set the schema name."""
        self._config_dict['schema_name'] = schema_name
        return self
    
    def with_strategy(self, strategy: ProfilingStrategy) -> 'ConfigBuilder':
        """Set the processing strategy."""
        self._config_dict['strategy'] = strategy
        return self
    
    def with_parallel_processing(self, max_workers: int = 4, threshold: int = 10) -> 'ConfigBuilder':
        """Configure parallel processing."""
        self._config_dict.update({
            'max_workers': max_workers,
            'parallel_threshold': threshold
        })
        return self
    
    def with_incremental(self, state_path: str, change_threshold: float = 0.1) -> 'ConfigBuilder':
        """Enable incremental profiling."""
        self._config_dict.update({
            'incremental_enabled': True,
            'incremental_state_path': state_path,
            'data_change_threshold': change_threshold
        })
        return self
    
    def with_patterns(self, config_path: Optional[str] = None, enabled: bool = True) -> 'ConfigBuilder':
        """Configure pattern recognition."""
        self._config_dict.update({
            'pattern_recognition_enabled': enabled,
            'patterns_config_path': config_path
        })
        return self
    
    def with_resource_limits(self, max_connections: int = 10, 
                           query_timeout: int = 300,
                           memory_limit_mb: int = 1024) -> 'ConfigBuilder':
        """Set resource limits."""
        self._config_dict.update({
            'max_connections': max_connections,
            'query_timeout': query_timeout,
            'memory_limit_mb': memory_limit_mb
        })
        return self
    
    def with_debugging(self, debug_mode: bool = True, 
                      log_level: str = "DEBUG",
                      profile_performance: bool = True) -> 'ConfigBuilder':
        """Enable debugging features."""
        self._config_dict.update({
            'debug_mode': debug_mode,
            'log_level': log_level,
            'profile_performance': profile_performance
        })
        return self
    
    def build(self) -> ProfilerConfig:
        """Build the final configuration."""
        return ProfilerConfig.from_dict(self._config_dict)


# Predefined configurations for common use cases
class CommonConfigs:
    """Common profiler configurations for typical use cases."""
    
    @staticmethod
    def development(database_name: str, schema_name: Optional[str] = None) -> ProfilerConfig:
        """Configuration optimized for development environments."""
        return ConfigBuilder(database_name) \
            .with_schema(schema_name) \
            .with_strategy(ProfilingStrategy.SEQUENTIAL) \
            .with_debugging(debug_mode=True, log_level="DEBUG") \
            .with_resource_limits(max_connections=2, query_timeout=60) \
            .build()
    
    @staticmethod
    def production(database_name: str, schema_name: Optional[str] = None,
                  state_path: Optional[str] = None) -> ProfilerConfig:
        """Configuration optimized for production environments."""
        builder = ConfigBuilder(database_name) \
            .with_schema(schema_name) \
            .with_strategy(ProfilingStrategy.ADAPTIVE) \
            .with_parallel_processing(max_workers=8, threshold=5) \
            .with_resource_limits(max_connections=10, query_timeout=300)
        
        if state_path:
            builder = builder.with_incremental(state_path, change_threshold=0.05)
        
        return builder.build()
    
    @staticmethod
    def large_database(database_name: str, schema_name: Optional[str] = None,
                      state_path: str = "./incremental_state.json") -> ProfilerConfig:
        """Configuration optimized for large databases."""
        return ConfigBuilder(database_name) \
            .with_schema(schema_name) \
            .with_strategy(ProfilingStrategy.PARALLEL) \
            .with_parallel_processing(max_workers=12, threshold=3) \
            .with_incremental(state_path, change_threshold=0.02) \
            .with_resource_limits(max_connections=15, memory_limit_mb=2048) \
            .build()
    
    @staticmethod
    def ci_cd(database_name: str, schema_name: Optional[str] = None) -> ProfilerConfig:
        """Configuration optimized for CI/CD pipelines."""
        return ConfigBuilder(database_name) \
            .with_schema(schema_name) \
            .with_strategy(ProfilingStrategy.ADAPTIVE) \
            .with_parallel_processing(max_workers=4, threshold=8) \
            .with_resource_limits(max_connections=5, query_timeout=180) \
            .build() 