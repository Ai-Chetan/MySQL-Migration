"""
Script Generator
Generates Python migration scripts for complex manual migrations.
"""
from typing import Dict, List, Optional, Any
from datetime import datetime
from services.api.schema_migration.mapping_engine import TableMapping, MappingType
from services.api.schema_migration.schema_parser import TableDefinition
from services.api.schema_migration.connection_manager import DatabaseConnection
from shared.utils import setup_logger

logger = setup_logger(__name__)


class ScriptGenerator:
    """Generates Python migration scripts for manual execution."""
    
    @staticmethod
    def generate_manual_script(
        mapping: TableMapping,
        old_schema: Dict[str, TableDefinition],  # table_name -> TableDefinition
        new_schemas: Dict[str, TableDefinition],
        connection_info: DatabaseConnection,
        output_dir: str = "."
    ) -> str:
        """
        Generate a Python script for manual migration.
        
        Args:
            mapping: TableMapping configuration
            old_schema: Dict of old table schemas
            new_schemas: Dict of new table schemas
            connection_info: Database connection details
            output_dir: Directory to save the script
        
        Returns:
            Path to generated script file
        """
        if mapping.mapping_type == MappingType.SINGLE:
            return ScriptGenerator._generate_single_script(
                mapping, old_schema, new_schemas, connection_info, output_dir
            )
        elif mapping.mapping_type == MappingType.SPLIT:
            return ScriptGenerator._generate_split_script(
                mapping, old_schema, new_schemas, connection_info, output_dir
            )
        elif mapping.mapping_type == MappingType.MERGE:
            return ScriptGenerator._generate_merge_script(
                mapping, old_schema, new_schemas, connection_info, output_dir
            )
        else:
            raise ValueError(f"Unknown mapping type: {mapping.mapping_type}")
    
    @staticmethod
    def _generate_single_script(
        mapping: TableMapping,
        old_schema: Dict[str, TableDefinition],
        new_schemas: Dict[str, TableDefinition],
        connection_info: DatabaseConnection,
        output_dir: str
    ) -> str:
        """Generate script for single table migration."""
        source_table = mapping.source_tables[0]
        target_table = mapping.target_tables[0]
        
        old_table = old_schema.get(source_table)
        new_table = new_schemas.get(target_table)
        
        if not old_table or not new_table:
            raise ValueError("Table schema not found")
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"manual_migration_{source_table}_to_{target_table}_{timestamp}.py"
        filepath = f"{output_dir}/{filename}"
        
        # Build script content
        script = f'''#!/usr/bin/env python3
"""
Manual Migration Script
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Source Table: {source_table}
Target Table: {target_table}_new

IMPORTANT: This is a template. You MUST review and modify this script before running!
- Update connection details
- Implement data transformations in the TODO sections
- Test thoroughly before running on production data
"""

import pymysql
import sys
from datetime import datetime

# ==========================================
# CONNECTION CONFIGURATION
# ==========================================
# TODO: Update these connection details!
DB_CONFIG = {{
    'host': '{connection_info.host}',
    'port': {connection_info.port},
    'user': 'YOUR_USERNAME',  # TODO: Update
    'password': 'YOUR_PASSWORD',  # TODO: Update
    'database': '{connection_info.database}',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}}

BATCH_SIZE = 1000  # Rows per batch

# ==========================================
# SCHEMA INFORMATION (Reference Only)
# ==========================================

# Old Table Schema: {source_table}
OLD_SCHEMA = """
'''
        
        # Add old table schema
        for col in old_table.columns:
            script += f"  {col.name} {col.data_type}"
            if not col.nullable:
                script += " NOT NULL"
            if col.primary_key:
                script += " PRIMARY KEY"
            script += "\n"
        
        script += f'''"""

# New Table Schema: {target_table}_new
NEW_SCHEMA = """
'''
        
        # Add new table schema
        for col in new_table.columns:
            script += f"  {col.name} {col.data_type}"
            if not col.nullable:
                script += " NOT NULL"
            if col.primary_key:
                script += " PRIMARY KEY"
            script += "\n"
        
        script += f'''"""

# Sample Data (First 5 rows from old table)
# TODO: Run this query to see actual data:
# SELECT * FROM {source_table} LIMIT 5;


# ==========================================
# MIGRATION LOGIC
# ==========================================

def migrate_data():
    """Main migration function."""
    connection = None
    
    try:
        print("Connecting to database...")
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        # Step 1: Create new table
        print(f"Creating table {{target_table}}_new...")
        create_table_sql = """
'''
        
        # Generate CREATE TABLE
        columns = []
        primary_keys = []
        
        for col in new_table.columns:
            col_def = f"  `{col.name}` {col.data_type}"
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
        
        if primary_keys:
            columns.append(f"  PRIMARY KEY (`{'`, `'.join(primary_keys)}`)")
        
        script += f"CREATE TABLE IF NOT EXISTS `{target_table}_new` (\n"
        script += ",\n".join(columns)
        script += "\n);\n"
        
        script += f'''        """
        cursor.execute(create_table_sql)
        connection.commit()
        print("✓ Table created")
        
        # Step 2: Count total rows
        cursor.execute("SELECT COUNT(*) as cnt FROM `{source_table}`")
        total_rows = cursor.fetchone()['cnt']
        print(f"Total rows to migrate: {{total_rows}}")
        
        if total_rows == 0:
            print("No data to migrate")
            return
        
        # Step 3: Migrate data in batches
        offset = 0
        migrated = 0
        errors = 0
        
        while offset < total_rows:
            # Fetch batch
            cursor.execute(f"""
                SELECT * FROM `{source_table}`
                LIMIT {{offset}}, {{BATCH_SIZE}}
            """)
            rows = cursor.fetchall()
            
            if not rows:
                break
            
            print(f"Processing batch {{offset}}-{{offset + len(rows)}} of {{total_rows}}...")
            
            # Process each row
            for row in rows:
                try:
                    # TODO: Transform data as needed
                    transformed_row = transform_row(row)
                    
                    # Insert into new table
                    insert_sql = """
                        INSERT INTO `{target_table}_new` (
'''
        
        # Add column names
        insert_cols = [col.name for col in new_table.columns if not col.auto_increment]
        script += "                            " + ", ".join([f"`{col}`" for col in insert_cols]) + "\n"
        script += "                        ) VALUES (\n"
        script += "                            " + ", ".join(["%s"] * len(insert_cols)) + "\n"
        script += "                        )\n"
        script += "                    \"\"\"\n"
        
        script += f'''                    
                    values = [transformed_row[col] for col in {insert_cols}]
                    cursor.execute(insert_sql, values)
                    migrated += 1
                    
                except Exception as e:
                    errors += 1
                    print(f"  ✗ Error processing row: {{e}}")
                    print(f"    Row data: {{row}}")
                    # Decide: continue or abort?
                    # raise  # Uncomment to abort on first error
            
            connection.commit()
            offset += len(rows)
            
            # Progress update
            progress = (migrated / total_rows) * 100
            print(f"  Progress: {{progress:.1f}}% ({{migrated}}/{{total_rows}} rows, {{errors}} errors)")
        
        print(f"\\n✓ Migration completed!")
        print(f"  Rows migrated: {{migrated}}")
        print(f"  Errors: {{errors}}")
        
        # Step 4: Verify data
        cursor.execute(f"SELECT COUNT(*) as cnt FROM `{target_table}_new`")
        new_count = cursor.fetchone()['cnt']
        print(f"  Rows in new table: {{new_count}}")
        
        cursor.close()
        
    except Exception as e:
        print(f"\\n✗ Migration failed: {{e}}")
        if connection:
            connection.rollback()
        raise
    
    finally:
        if connection:
            connection.close()
            print("\\nDatabase connection closed")


def transform_row(row: dict) -> dict:
    """
    Transform a row from old schema to new schema.
    
    TODO: Implement your data transformations here!
    
    Args:
        row: Dict with old table column names as keys
    
    Returns:
        Dict with new table column names as keys
    """
    transformed = {{}}
    
    # TODO: Map and transform columns
    # Example transformations:
    
'''
        
        # Add column mapping examples
        col_mappings = mapping.column_mappings.get(target_table, [])
        if col_mappings:
            for cm in col_mappings[:5]:  # Show first 5
                script += f"    # {cm.source_column} -> {cm.target_column}\n"
                script += f"    transformed['{cm.target_column}'] = row.get('{cm.source_column}')\n"
        else:
            # Auto-mapping example
            for col in new_table.columns[:5]:
                script += f"    # TODO: Map to '{col.name}'\n"
                script += f"    transformed['{col.name}'] = row.get('{col.name}')  # May need transformation\n"
        
        script += f'''    
    # TODO: Add more column mappings above
    
    # TODO: Handle data type conversions
    # Example: Convert string to int
    # if transformed['age']:
    #     transformed['age'] = int(transformed['age'])
    
    # TODO: Handle NULL values
    # transformed['field'] = transformed.get('field') or 'default_value'
    
    # TODO: Apply business logic transformations
    
    return transformed


# ==========================================
# MAIN EXECUTION
# ==========================================

if __name__ == "__main__":
    print("=" * 60)
    print("Manual Migration Script")
    print("=" * 60)
    print(f"Source: {source_table}")
    print(f"Target: {target_table}_new")
    print("=" * 60)
    print()
    
    response = input("Have you reviewed and updated this script? (yes/no): ")
    if response.lower() != 'yes':
        print("Please review and update the script before running!")
        sys.exit(1)
    
    response = input("Have you backed up your database? (yes/no): ")
    if response.lower() != 'yes':
        print("ALWAYS back up your database before running migrations!")
        sys.exit(1)
    
    print()
    print("Starting migration...")
    print()
    
    start_time = datetime.now()
    migrate_data()
    end_time = datetime.now()
    
    duration = (end_time - start_time).total_seconds()
    print(f"\\nTotal time: {{duration:.2f}} seconds")
    print("\\nDone!")
'''
        
        # Write script to file
        with open(filepath, 'w') as f:
            f.write(script)
        
        logger.info(f"Generated manual script: {filepath}")
        return filepath
    
    @staticmethod
    def _generate_split_script(
        mapping: TableMapping,
        old_schema: Dict[str, TableDefinition],
        new_schemas: Dict[str, TableDefinition],
        connection_info: DatabaseConnection,
        output_dir: str
    ) -> str:
        """Generate script for split table migration."""
        # Similar structure but for multiple targets
        source_table = mapping.source_tables[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"manual_migration_split_{source_table}_{timestamp}.py"
        filepath = f"{output_dir}/{filename}"
        
        script = f'''#!/usr/bin/env python3
"""
Manual Split Migration Script
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Source Table: {source_table}
Target Tables: {', '.join([f"{t}_new" for t in mapping.target_tables])}

This script splits one table into multiple target tables.
Review and customize before running!
"""

# TODO: Implement split migration logic
# Similar to single migration but insert into multiple target tables

print("Split migration script generated. Please implement custom logic.")
'''
        
        with open(filepath, 'w') as f:
            f.write(script)
        
        return filepath
    
    @staticmethod
    def _generate_merge_script(
        mapping: TableMapping,
        old_schema: Dict[str, TableDefinition],
        new_schemas: Dict[str, TableDefinition],
        connection_info: DatabaseConnection,
        output_dir: str
    ) -> str:
        """Generate script for merge table migration."""
        target_table = mapping.target_tables[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"manual_migration_merge_{target_table}_{timestamp}.py"
        filepath = f"{output_dir}/{filename}"
        
        script = f'''#!/usr/bin/env python3
"""
Manual Merge Migration Script
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Source Tables: {', '.join(mapping.source_tables)}
Target Table: {target_table}_new

This script merges multiple tables with JOIN conditions.
Review and customize before running!
"""

# TODO: Implement merge migration logic with JOIN conditions
# Use the join_conditions and column_mappings from the mapping

print("Merge migration script generated. Please implement custom logic.")
'''
        
        with open(filepath, 'w') as f:
            f.write(script)
        
        return filepath
