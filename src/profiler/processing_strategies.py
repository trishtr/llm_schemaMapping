"""
Processing Strategies

This module implements pluggable processing strategies for table profiling,
separating the execution concerns from the core profiling logic.
"""

import logging
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

from .interfaces import TableProcessor, ProfilerConfig, ProfilingStrategy
from .schema_models import TableProfile
from .core_profiler import CoreSchemaProfiler


class SequentialTableProcessor(TableProcessor):
    """Sequential table processing strategy."""
    
    def __init__(self, core_profiler: CoreSchemaProfiler):
        self.core_profiler = core_profiler
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def process_tables(self, tables_info: List[Dict[str, Any]], config: ProfilerConfig) -> List[TableProfile]:
        """Process tables sequentially."""
        self.logger.info(f"Processing {len(tables_info)} tables sequentially")
        
        profiles = []
        for i, table_info in enumerate(tables_info, 1):
            table_name = table_info['table_name']
            
            try:
                self.logger.debug(f"Processing table {i}/{len(tables_info)}: {table_name}")
                profile = self.core_profiler.profile_table(table_name, config)
                profiles.append(profile)
                
            except Exception as e:
                self.logger.error(f"Error processing table {table_name}: {e}")
                # Continue with other tables
                continue
        
        self.logger.info(f"Sequential processing completed: {len(profiles)} tables processed")
        return profiles
    
    def get_strategy_name(self) -> str:
        return "sequential"


class ParallelTableProcessor(TableProcessor):
    """Parallel table processing strategy with resource management."""
    
    def __init__(self, core_profiler: CoreSchemaProfiler):
        self.core_profiler = core_profiler
        self.logger = logging.getLogger(self.__class__.__name__)
        self._connection_semaphore = None
    
    def process_tables(self, tables_info: List[Dict[str, Any]], config: ProfilerConfig) -> List[TableProfile]:
        """Process tables in parallel with resource management."""
        max_workers = min(config.max_workers, len(tables_info))
        self.logger.info(f"Processing {len(tables_info)} tables in parallel with {max_workers} workers")
        
        # Create connection semaphore to limit concurrent database connections
        self._connection_semaphore = threading.Semaphore(config.max_connections)
        
        profiles = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all table profiling tasks
            future_to_table = {
                executor.submit(self._profile_table_safe, table_info['table_name'], config): table_info['table_name']
                for table_info in tables_info
            }
            
            # Collect results as they complete
            completed_count = 0
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                completed_count += 1
                
                try:
                    profile = future.result(timeout=config.query_timeout)
                    if profile:
                        profiles.append(profile)
                        self.logger.debug(f"Completed profiling table ({completed_count}/{len(tables_info)}): {table_name}")
                    else:
                        self.logger.warning(f"No profile returned for table: {table_name}")
                        
                except Exception as e:
                    self.logger.error(f"Error profiling table {table_name}: {e}")
                    # Continue with other tables
                    continue
        
        # Sort profiles by table name for consistent output
        profiles.sort(key=lambda t: t.name)
        
        self.logger.info(f"Parallel processing completed: {len(profiles)} tables processed")
        return profiles
    
    def _profile_table_safe(self, table_name: str, config: ProfilerConfig) -> TableProfile:
        """Thread-safe wrapper for table profiling with resource management."""
        # Acquire connection semaphore
        with self._connection_semaphore:
            try:
                return self.core_profiler.profile_table(table_name, config)
            except Exception as e:
                self.logger.error(f"Thread-safe table profiling failed for {table_name}: {e}")
                # Return minimal profile rather than None
                return TableProfile(
                    name=table_name,
                    schema=config.schema_name,
                    columns=[],
                    primary_keys=[],
                    foreign_keys=[],
                    indexes=[],
                    sample_data=[],
                    estimated_row_count=0,
                    self_referencing_columns=[],
                    potential_fk_candidates=[]
                )
    
    def get_strategy_name(self) -> str:
        return "parallel"


class AdaptiveTableProcessor(TableProcessor):
    """Adaptive processing strategy that chooses between sequential and parallel."""
    
    def __init__(self, core_profiler: CoreSchemaProfiler):
        self.core_profiler = core_profiler
        self.sequential_processor = SequentialTableProcessor(core_profiler)
        self.parallel_processor = ParallelTableProcessor(core_profiler)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def process_tables(self, tables_info: List[Dict[str, Any]], config: ProfilerConfig) -> List[TableProfile]:
        """Choose processing strategy based on table count and configuration."""
        table_count = len(tables_info)
        
        # Determine which strategy to use
        if table_count >= config.parallel_threshold and config.max_workers > 1:
            self.logger.info(f"Adaptive strategy: Using parallel processing ({table_count} tables >= {config.parallel_threshold} threshold)")
            return self.parallel_processor.process_tables(tables_info, config)
        else:
            self.logger.info(f"Adaptive strategy: Using sequential processing ({table_count} tables < {config.parallel_threshold} threshold)")
            return self.sequential_processor.process_tables(tables_info, config)
    
    def get_strategy_name(self) -> str:
        return "adaptive"


class ProcessingStrategyFactory:
    """Factory for creating processing strategies."""
    
    @staticmethod
    def create_processor(strategy: ProfilingStrategy, core_profiler: CoreSchemaProfiler) -> TableProcessor:
        """Create a table processor for the given strategy."""
        if strategy == ProfilingStrategy.SEQUENTIAL:
            return SequentialTableProcessor(core_profiler)
        elif strategy == ProfilingStrategy.PARALLEL:
            return ParallelTableProcessor(core_profiler)
        elif strategy == ProfilingStrategy.ADAPTIVE:
            return AdaptiveTableProcessor(core_profiler)
        else:
            raise ValueError(f"Unknown processing strategy: {strategy}")


class PerformanceMonitor:
    """Monitor processing performance and resource usage."""
    
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.table_times = {}
        self.errors = []
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def start_monitoring(self):
        """Start performance monitoring."""
        self.start_time = time.time()
        self.logger.debug("Performance monitoring started")
    
    def end_monitoring(self):
        """End performance monitoring."""
        self.end_time = time.time()
        self.logger.debug("Performance monitoring ended")
    
    def record_table_time(self, table_name: str, duration: float):
        """Record processing time for a table."""
        self.table_times[table_name] = duration
    
    def record_error(self, table_name: str, error: str):
        """Record an error during processing."""
        self.errors.append({'table': table_name, 'error': error})
    
    def get_performance_report(self) -> Dict[str, Any]:
        """Get comprehensive performance report."""
        if not self.start_time or not self.end_time:
            return {'error': 'Monitoring not completed'}
        
        total_duration = self.end_time - self.start_time
        
        return {
            'total_duration_seconds': round(total_duration, 2),
            'total_tables_processed': len(self.table_times),
            'average_table_time_seconds': round(sum(self.table_times.values()) / len(self.table_times), 2) if self.table_times else 0,
            'slowest_table': max(self.table_times.items(), key=lambda x: x[1]) if self.table_times else None,
            'fastest_table': min(self.table_times.items(), key=lambda x: x[1]) if self.table_times else None,
            'error_count': len(self.errors),
            'errors': self.errors,
            'tables_per_second': round(len(self.table_times) / total_duration, 2) if total_duration > 0 else 0
        }


class ResourceManager:
    """Manage system resources during processing."""
    
    def __init__(self, config: ProfilerConfig):
        self.config = config
        self.connection_count = 0
        self.memory_usage_mb = 0
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def acquire_connection(self) -> bool:
        """Attempt to acquire a database connection."""
        if self.connection_count >= self.config.max_connections:
            self.logger.warning(f"Connection limit reached: {self.connection_count}/{self.config.max_connections}")
            return False
        
        self.connection_count += 1
        return True
    
    def release_connection(self):
        """Release a database connection."""
        if self.connection_count > 0:
            self.connection_count -= 1
    
    def check_memory_usage(self) -> bool:
        """Check if memory usage is within limits."""
        # This is a simplified check - in production you'd use psutil or similar
        if self.memory_usage_mb > self.config.memory_limit_mb:
            self.logger.warning(f"Memory limit exceeded: {self.memory_usage_mb}MB > {self.config.memory_limit_mb}MB")
            return False
        return True
    
    def get_resource_status(self) -> Dict[str, Any]:
        """Get current resource usage status."""
        return {
            'active_connections': self.connection_count,
            'max_connections': self.config.max_connections,
            'memory_usage_mb': self.memory_usage_mb,
            'memory_limit_mb': self.config.memory_limit_mb,
            'connection_utilization_percent': round((self.connection_count / self.config.max_connections) * 100, 1),
            'memory_utilization_percent': round((self.memory_usage_mb / self.config.memory_limit_mb) * 100, 1)
        } 