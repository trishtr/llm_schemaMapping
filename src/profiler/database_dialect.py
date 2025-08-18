"""
Database Dialect Module

This module provides database-specific SQL syntax and operations for different database systems.
Supports MySQL, PostgreSQL, and Microsoft SQL Server with appropriate queries and formatting.
"""

from typing import Dict, Any


class DatabaseDialect:
    """Handles database-specific SQL syntax and operations"""
    
    DIALECTS = {
        'mysql': {
            'table_info_query': """
                SELECT 
                    COLUMN_NAME as column_name,
                    DATA_TYPE as data_type,
                    IS_NULLABLE as is_nullable,
                    CHARACTER_MAXIMUM_LENGTH as character_maximum_length,
                    COLUMN_DEFAULT as column_default,
                    COLUMN_KEY as column_key,
                    EXTRA as extra,
                    ORDINAL_POSITION as ordinal_position,
                    COLUMN_COMMENT as column_comment
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """,
            'tables_query': """
                SELECT 
                    TABLE_NAME as table_name, 
                    TABLE_TYPE as table_type,
                    TABLE_COMMENT as table_comment,
                    TABLE_ROWS as estimated_rows
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """,
            'foreign_keys_query': """
                SELECT 
                    COLUMN_NAME as column_name,
                    REFERENCED_TABLE_NAME as referenced_table,
                    REFERENCED_COLUMN_NAME as referenced_column,
                    CONSTRAINT_NAME as constraint_name
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s 
                AND REFERENCED_TABLE_NAME IS NOT NULL
            """,
            'indexes_query': """
                SELECT 
                    INDEX_NAME as index_name,
                    COLUMN_NAME as column_name,
                    NON_UNIQUE as non_unique,
                    INDEX_TYPE as index_type,
                    SEQ_IN_INDEX as sequence_in_index
                FROM INFORMATION_SCHEMA.STATISTICS 
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY INDEX_NAME, SEQ_IN_INDEX
            """,
            'primary_keys_query': """
                SELECT COLUMN_NAME as column_name
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s 
                AND CONSTRAINT_NAME = 'PRIMARY'
                ORDER BY ORDINAL_POSITION
            """,
            'sample_query': 'SELECT {} FROM {} LIMIT {}',
            'count_query': 'SELECT COUNT(*) as row_count FROM {}',
            'quote_identifier': '`{}`'
        },
        'postgresql': {
            'table_info_query': """
                SELECT 
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.character_maximum_length,
                    c.column_default,
                    c.ordinal_position,
                    col_description(pgc.oid, c.ordinal_position) as column_comment
                FROM information_schema.columns c
                LEFT JOIN pg_class pgc ON pgc.relname = c.table_name
                LEFT JOIN pg_namespace pgn ON pgn.oid = pgc.relnamespace
                WHERE c.table_schema = %s AND c.table_name = %s
                ORDER BY c.ordinal_position
            """,
            'tables_query': """
                SELECT 
                    t.table_name, 
                    t.table_type,
                    obj_description(c.oid) as table_comment
                FROM information_schema.tables t
                LEFT JOIN pg_class c ON c.relname = t.table_name
                LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE t.table_schema = %s AND t.table_type = 'BASE TABLE'
                AND (n.nspname = %s OR n.nspname IS NULL)
                ORDER BY t.table_name
            """,
            'foreign_keys_query': """
                SELECT 
                    kcu.column_name,
                    ccu.table_name AS referenced_table,
                    ccu.column_name AS referenced_column,
                    tc.constraint_name
                FROM information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY' 
                AND tc.table_schema = %s AND tc.table_name = %s
            """,
            'indexes_query': """
                SELECT 
                    i.relname as index_name,
                    a.attname as column_name,
                    ix.indisunique as is_unique,
                    ix.indisprimary as is_primary
                FROM pg_class t
                JOIN pg_index ix ON t.oid = ix.indrelid
                JOIN pg_class i ON i.oid = ix.indexrelid
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE n.nspname = %s AND t.relname = %s
                AND NOT ix.indisprimary
            """,
            'primary_keys_query': """
                SELECT a.attname as column_name
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE i.indisprimary AND n.nspname = %s AND c.relname = %s
                ORDER BY a.attnum
            """,
            'sample_query': 'SELECT {} FROM {} LIMIT {}',
            'count_query': 'SELECT COUNT(*) as row_count FROM {}',
            'quote_identifier': '"{}"'
        },
        'mssql': {
            'table_info_query': """
                SELECT 
                    c.COLUMN_NAME as column_name,
                    c.DATA_TYPE as data_type,
                    c.IS_NULLABLE as is_nullable,
                    c.CHARACTER_MAXIMUM_LENGTH as character_maximum_length,
                    c.COLUMN_DEFAULT as column_default,
                    c.ORDINAL_POSITION as ordinal_position,
                    ep.value as column_comment
                FROM INFORMATION_SCHEMA.COLUMNS c
                LEFT JOIN sys.columns sc ON sc.name = c.COLUMN_NAME
                LEFT JOIN sys.tables st ON st.name = c.TABLE_NAME
                LEFT JOIN sys.extended_properties ep ON ep.major_id = st.object_id 
                    AND ep.minor_id = sc.column_id AND ep.name = 'MS_Description'
                WHERE c.TABLE_CATALOG = ? AND c.TABLE_NAME = ?
                ORDER BY c.ORDINAL_POSITION
            """,
            'tables_query': """
                SELECT 
                    t.TABLE_NAME as table_name, 
                    t.TABLE_TYPE as table_type,
                    ep.value as table_comment
                FROM INFORMATION_SCHEMA.TABLES t
                LEFT JOIN sys.tables st ON st.name = t.TABLE_NAME
                LEFT JOIN sys.extended_properties ep ON ep.major_id = st.object_id 
                    AND ep.minor_id = 0 AND ep.name = 'MS_Description'
                WHERE t.TABLE_CATALOG = ? AND t.TABLE_TYPE = 'BASE TABLE'
                ORDER BY t.TABLE_NAME
            """,
            'foreign_keys_query': """
                SELECT 
                    COL_NAME(fkc.parent_object_id, fkc.parent_column_id) as column_name,
                    OBJECT_NAME(fkc.referenced_object_id) as referenced_table,
                    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) as referenced_column,
                    fk.name as constraint_name
                FROM sys.foreign_key_columns fkc
                JOIN sys.foreign_keys fk ON fkc.constraint_object_id = fk.object_id
                WHERE OBJECT_NAME(fkc.parent_object_id) = ?
            """,
            'indexes_query': """
                SELECT 
                    i.name as index_name,
                    c.name as column_name,
                    i.is_unique,
                    i.is_primary_key
                FROM sys.indexes i
                JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                JOIN sys.tables t ON i.object_id = t.object_id
                WHERE t.name = ? AND i.is_primary_key = 0
            """,
            'primary_keys_query': """
                SELECT c.name as column_name
                FROM sys.indexes i
                JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                JOIN sys.tables t ON i.object_id = t.object_id
                WHERE t.name = ? AND i.is_primary_key = 1
                ORDER BY ic.key_ordinal
            """,
            'sample_query': 'SELECT TOP {} {} FROM {}',
            'count_query': 'SELECT COUNT(*) as row_count FROM {}',
            'quote_identifier': '[{}]'
        }
    }
    
    def __init__(self, db_type: str):
        """
        Initialize the database dialect.
        
        Args:
            db_type: Database type ('mysql', 'postgresql', 'mssql')
        """
        self.db_type = db_type.lower()
        self.dialect = self.DIALECTS.get(self.db_type, self.DIALECTS['postgresql'])
    
    def get_sample_query(self, columns: str, table: str, limit: int = 5) -> str:
        """
        Generate database-specific sample query.
        
        Args:
            columns: Column specification (e.g., '*' or 'col1, col2')
            table: Table name (should already be quoted if needed)
            limit: Number of rows to limit
            
        Returns:
            Database-specific sample query string
        """
        if self.db_type == 'mssql':
            return self.dialect['sample_query'].format(limit, columns, table)
        else:
            return self.dialect['sample_query'].format(columns, table, limit)
    
    def get_count_query(self, table: str) -> str:
        """
        Generate database-specific count query.
        
        Args:
            table: Table name (should already be quoted if needed)
            
        Returns:
            Database-specific count query string
        """
        return self.dialect['count_query'].format(table)
    
    def get_column_info_query(self) -> str:
        """Get the column information query for this database type."""
        return self.dialect['table_info_query']
    
    def get_tables_query(self) -> str:
        """Get the tables listing query for this database type."""
        return self.dialect['tables_query']
    
    def get_foreign_keys_query(self) -> str:
        """Get the foreign keys query for this database type."""
        return self.dialect['foreign_keys_query']
    
    def get_indexes_query(self) -> str:
        """Get the indexes query for this database type."""
        return self.dialect['indexes_query']
    
    def get_primary_keys_query(self) -> str:
        """Get the primary keys query for this database type."""
        return self.dialect['primary_keys_query']
    
    def quote_identifier(self, identifier: str) -> str:
        """
        Quote an identifier (table name, column name) according to database rules.
        
        Args:
            identifier: The identifier to quote
            
        Returns:
            Properly quoted identifier for this database type
        """
        return self.dialect['quote_identifier'].format(identifier)
    
    def get_supported_databases(self) -> list:
        """Get list of supported database types."""
        return list(self.DIALECTS.keys())
    
    def is_supported(self, db_type: str) -> bool:
        """Check if a database type is supported."""
        return db_type.lower() in self.DIALECTS 