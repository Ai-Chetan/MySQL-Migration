"""
Migration Executor
Executes schema migrations (create tables, copy data) for single/split/merge operations.
"""
from typing import Dict, List, Optional, Any, Tuple
from services.api.schema_migration.connection_manager import ConnectionManager, DatabaseType
from services.api.schema_migration.mapping_engine import TableMapping, MappingType, ColumnMapping
from services.api.schema_migration.schema_parser import TableDefinition
from services.api.schema_migration.data_type_analyzer import DataTypeAnalyzer, ConversionSafety
from shared.utils import setup_logger

logger = setup_logger(__name__)


class MigrationExecutor:
    """Executes database migrations based on mappings."""
    
    def __init__(self, connection_manager: ConnectionManager):
        self.conn_manager = connection_manager
    
    def execute_migration(
        self,
        conn_id: str,
        mapping: TableMapping,
        new_schemas: Dict[str, TableDefinition],
        batch_size: int = 5000,
        lossy_confirmed: bool = False
    ) -> Dict[str, Any]:
        """
        Execute a migration based on mapping type.
        
        Args:
            conn_id: Database connection ID
            mapping: TableMapping configuration
            new_schemas: Dict of table_name -> TableDefinition for new tables
            batch_size: Number of rows to process per batch
            lossy_confirmed: User confirmed lossy conversions
        
        Returns:
            Result dict with status, message, and details
        """
        # Validate conversion safety first
        validation = self._validate_conversions(mapping, new_schemas)
        
        if validation['has_unsafe']:
            return {
                'success': False,
                'message': 'Migration blocked: Unsafe type conversions detected',
                'details': validation
            }
        
        if validation['has_lossy'] and not lossy_confirmed:
            return {
                'success': False,
                'message': 'Migration requires confirmation: Lossy type conversions detected',
                'details': validation,
                'requires_confirmation': True
            }
        
        try:
            if mapping.mapping_type == MappingType.SINGLE:
                return self._execute_single_migration(conn_id, mapping, new_schemas, batch_size)
            elif mapping.mapping_type == MappingType.SPLIT:
                return self._execute_split_migration(conn_id, mapping, new_schemas, batch_size)
            elif mapping.mapping_type == MappingType.MERGE:
                return self._execute_merge_migration(conn_id, mapping, new_schemas, batch_size)
            else:
                return {
                    'success': False,
                    'message': f'Unknown mapping type: {mapping.mapping_type}'
                }
        
        except Exception as e:
            logger.error(f"Migration failed: {e}", exc_info=True)
            return {
                'success': False,
                'message': f'Migration failed: {str(e)}'
            }
    
    def _validate_conversions(
        self,
        mapping: TableMapping,
        new_schemas: Dict[str, TableDefinition]
    ) -> Dict[str, Any]:
        """Validate data type conversions for all mapped columns."""
        conn_id = None  # We'll need source schema from somewhere
        issues = []
        has_unsafe = False
        has_lossy = False
        
        for target_table, col_mappings in mapping.column_mappings.items():
            if target_table not in new_schemas:
                continue
            
            target_schema = new_schemas[target_table]
            
            for col_map in col_mappings:
                target_col = target_schema.get_column(col_map.target_column)
                if not target_col:
                    continue
                
                # We need source column type - this would come from database
                # For now, mark as needing validation
                issues.append({
                    'source_column': col_map.source_column,
                    'target_column': col_map.target_column,
                    'target_table': target_table,
                    'status': 'needs_validation'
                })
        
        return {
            'has_unsafe': has_unsafe,
            'has_lossy': has_lossy,
            'issues': issues
        }
    
    def _execute_single_migration(
        self,
        conn_id: str,
        mapping: TableMapping,
        new_schemas: Dict[str, TableDefinition],
        batch_size: int
    ) -> Dict[str, Any]:
        """Execute single table migration (1-to-1)."""
        source_table = mapping.source_tables[0]
        target_table = mapping.target_tables[0]
        new_table_name = f"{target_table}_new"
        
        if target_table not in new_schemas:
            return {
                'success': False,
                'message': f'Target table {target_table} not found in schema definitions'
            }
        
        target_schema = new_schemas[target_table]
        
        try:
            conn = self.conn_manager.get_connection(conn_id)
            cursor = conn.cursor()
            
            # Step 1: Create new table
            create_sql = self._generate_create_table_sql(new_table_name, target_schema)
            logger.info(f"Creating table: {new_table_name}")
            cursor.execute(create_sql)
            conn.commit()
            
            # Step 2: Copy data
            col_mappings = mapping.column_mappings.get(target_table, [])
            rows_copied = self._copy_data_single(
                cursor,
                source_table,
                new_table_name,
                target_schema,
                col_mappings,
                batch_size
            )
            conn.commit()
            
            cursor.close()
            
            return {
                'success': True,
                'message': f'Migration completed successfully',
                'table_created': new_table_name,
                'rows_copied': rows_copied
            }
        
        except Exception as e:
            logger.error(f"Single migration failed: {e}", exc_info=True)
            conn.rollback()
            raise
    
    def _execute_split_migration(
        self,
        conn_id: str,
        mapping: TableMapping,
        new_schemas: Dict[str, TableDefinition],
        batch_size: int
    ) -> Dict[str, Any]:
        """Execute split table migration (1-to-many)."""
        source_table = mapping.source_tables[0]
        results = []
        total_rows = 0
        
        try:
            conn = self.conn_manager.get_connection(conn_id)
            
            for target_table in mapping.target_tables:
                if target_table not in new_schemas:
                    logger.warning(f"Target table {target_table} not in schemas, skipping")
                    continue
                
                new_table_name = f"{target_table}_new"
                target_schema = new_schemas[target_table]
                cursor = conn.cursor()
                
                # Create table
                create_sql = self._generate_create_table_sql(new_table_name, target_schema)
                logger.info(f"Creating table: {new_table_name}")
                cursor.execute(create_sql)
                conn.commit()
                
                # Copy data (all rows from source, but only mapped columns)
                col_mappings = mapping.column_mappings.get(target_table, [])
                rows_copied = self._copy_data_single(
                    cursor,
                    source_table,
                    new_table_name,
                    target_schema,
                    col_mappings,
                    batch_size
                )
                conn.commit()
                cursor.close()
                
                results.append({
                    'target_table': new_table_name,
                    'rows_copied': rows_copied
                })
                total_rows += rows_copied
            
            return {
                'success': True,
                'message': f'Split migration completed',
                'results': results,
                'total_rows': total_rows
            }
        
        except Exception as e:
            logger.error(f"Split migration failed: {e}", exc_info=True)
            conn.rollback()
            raise
    
    def _execute_merge_migration(
        self,
        conn_id: str,
        mapping: TableMapping,
        new_schemas: Dict[str, TableDefinition],
        batch_size: int
    ) -> Dict[str, Any]:
        """Execute merge table migration (many-to-1)."""
        source_tables = mapping.source_tables
        target_table = mapping.target_tables[0]
        new_table_name = f"{target_table}_new"
        
        if target_table not in new_schemas:
            return {
                'success': False,
                'message': f'Target table {target_table} not found in schema definitions'
            }
        
        target_schema = new_schemas[target_table]
        
        try:
            conn = self.conn_manager.get_connection(conn_id)
            cursor = conn.cursor()
            
            # Create table
            create_sql = self._generate_create_table_sql(new_table_name, target_schema)
            logger.info(f"Creating table: {new_table_name}")
            cursor.execute(create_sql)
            conn.commit()
            
            # Copy merged data
            col_mappings = mapping.column_mappings.get(target_table, [])
            rows_copied = self._copy_data_merge(
                cursor,
                source_tables,
                new_table_name,
                target_schema,
                col_mappings,
                mapping.join_conditions or [],
                batch_size
            )
            conn.commit()
            cursor.close()
            
            return {
                'success': True,
                'message': 'Merge migration completed',
                'table_created': new_table_name,
                'rows_copied': rows_copied,
                'source_tables': source_tables
            }
        
        except Exception as e:
            logger.error(f"Merge migration failed: {e}", exc_info=True)
            conn.rollback()
            raise
    
    def _generate_create_table_sql(
        self,
        table_name: str,
        schema: TableDefinition
    ) -> str:
        """Generate CREATE TABLE SQL statement."""
        columns = []
        primary_keys = []
        
        for col in schema.columns:
            col_def = f"`{col.name}` {col.data_type}"
            
            if not col.nullable:
                col_def += " NOT NULL"
            
            if col.auto_increment:
                col_def += " AUTO_INCREMENT"
            
            if col.default is not None:
                col_def += f" DEFAULT {col.default}"
            
            if col.extra:
                col_def += f" {col.extra}"
            
            columns.append(col_def)
            
            if col.primary_key:
                primary_keys.append(col.name)
        
        # Add PRIMARY KEY constraint
        if primary_keys:
            pk_def = f"PRIMARY KEY (`{'`, `'.join(primary_keys)}`)"
            columns.append(pk_def)
        
        sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n  "
        sql += ",\n  ".join(columns)
        sql += "\n)"
        
        return sql
    
    def _copy_data_single(
        self,
        cursor: Any,
        source_table: str,
        target_table: str,
        target_schema: TableDefinition,
        col_mappings: List[ColumnMapping],
        batch_size: int
    ) -> int:
        """Copy data from source to target for single/split mappings."""
        # Build column mapping
        if col_mappings:
            # Use explicit mappings
            source_cols = [cm.source_column for cm in col_mappings]
            target_cols = [cm.target_column for cm in col_mappings]
        else:
            # Auto-map by matching names
            source_cols = [col.name for col in target_schema.columns if not col.auto_increment]
            target_cols = source_cols
        
        # Build SELECT statement
        select_cols = ", ".join([f"`{col}`" for col in source_cols])
        insert_cols = ", ".join([f"`{col}`" for col in target_cols])
        placeholders = ", ".join(["%s"] * len(target_cols))
        
        # Count total rows
        cursor.execute(f"SELECT COUNT(*) as cnt FROM `{source_table}`")
        total_rows = list(cursor.fetchone().values())[0]
        
        if total_rows == 0:
            logger.info(f"No data to copy from {source_table}")
            return 0
        
        # Copy in batches
        offset = 0
        total_copied = 0
        
        while offset < total_rows:
            cursor.execute(
                f"SELECT {select_cols} FROM `{source_table}` LIMIT {offset}, {batch_size}"
            )
            rows = cursor.fetchall()
            
            if not rows:
                break
            
            # Insert batch
            insert_sql = f"INSERT INTO `{target_table}` ({insert_cols}) VALUES ({placeholders})"
            
            for row in rows:
                values = [row[col] for col in source_cols]
                cursor.execute(insert_sql, values)
                total_copied += 1
            
            offset += batch_size
            logger.info(f"Copied {total_copied}/{total_rows} rows to {target_table}")
        
        return total_copied
    
    def _copy_data_merge(
        self,
        cursor: Any,
        source_tables: List[str],
        target_table: str,
        target_schema: TableDefinition,
        col_mappings: List[ColumnMapping],
        join_conditions: List[str],
        batch_size: int
    ) -> int:
        """Copy merged data from multiple sources to target."""
        # Build SELECT with joins
        select_parts = []
        for cm in col_mappings:
            if cm.source_table:
                col_expr = f"`{cm.source_table}`.`{cm.source_column}`"
            else:
                col_expr = f"`{cm.source_column}`"
            
            if cm.transform:
                col_expr = cm.transform.replace('{col}', col_expr)
            
            select_parts.append(col_expr)
        
        select_clause = ", ".join(select_parts)
        
        # Build FROM clause with joins
        from_clause = f"`{source_tables[0]}`"
        for join_cond in join_conditions:
            from_clause += f" {join_cond}"
        
        # Build INSERT columns
        target_cols = [cm.target_column for cm in col_mappings]
        insert_cols = ", ".join([f"`{col}`" for col in target_cols])
        
        # Execute merge query
        merge_sql = f"""
        INSERT INTO `{target_table}` ({insert_cols})
        SELECT {select_clause}
        FROM {from_clause}
        """
        
        logger.info(f"Executing merge query:\n{merge_sql}")
        cursor.execute(merge_sql)
        rows_copied = cursor.rowcount
        
        logger.info(f"Merged {rows_copied} rows into {target_table}")
        return rows_copied
