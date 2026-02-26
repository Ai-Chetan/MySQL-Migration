"""
Schema Migration API Routes
REST API endpoints for schema migration tool.
"""
from fastapi import APIRouter, HTTPException, UploadFile, File, Body, Depends
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import uuid

from services.api.schema_migration.connection_manager import (
    get_connection_manager, DatabaseType, ConnectionManager
)
from services.api.schema_migration.schema_parser import SchemaParser, TableDefinition
from services.api.schema_migration.schema_comparator import SchemaComparator
from services.api.schema_migration.mapping_engine import MappingEngine, MappingType, ColumnMapping
from services.api.schema_migration.migration_executor import MigrationExecutor
from services.api.schema_migration.script_generator import ScriptGenerator
from services.api.schema_migration.data_type_analyzer import DataTypeAnalyzer
from shared.utils import setup_logger

logger = setup_logger(__name__)

router = APIRouter(prefix="/schema-migration", tags=["Schema Migration"])

# Global instances
mapping_engine = MappingEngine()


# ==========================================
# REQUEST/RESPONSE MODELS
# ==========================================

class ConnectionCreate(BaseModel):
    name: str
    db_type: DatabaseType
    host: str
    port: int
    database: str
    username: str
    password: str
    ssl: bool = False


class MappingSingle(BaseModel):
    source_table: str
    target_table: str
    column_mappings: Optional[Dict[str, str]] = None


class MappingSplit(BaseModel):
    source_table: str
    target_tables: List[str]
    column_mappings: Dict[str, Dict[str, str]]


class MappingMerge(BaseModel):
    source_tables: List[str]
    target_table: str
    column_mappings: List[Dict[str, Any]]
    join_conditions: List[str]


class MigrationExecuteRequest(BaseModel):
    conn_id: str
    mapping_id: str
    batch_size: int = 5000
    lossy_confirmed: bool = False


class DataExportRequest(BaseModel):
    conn_id: str
    table_name: str
    format: str = "json"  # json or csv
    limit: int = 1000


# ==========================================
# CONNECTION MANAGEMENT ENDPOINTS
# ==========================================

@router.post("/connections")
async def create_connection(conn: ConnectionCreate):
    """Create a new database connection."""
    try:
        conn_manager = get_connection_manager()
        conn_id = conn_manager.add_connection(
            name=conn.name,
            db_type=conn.db_type,
            host=conn.host,
            port=conn.port,
            database=conn.database,
            username=conn.username,
            password=conn.password,
            ssl=conn.ssl
        )
        
        return {
            "success": True,
            "connection_id": conn_id,
            "message": "Connection created successfully"
        }
    except Exception as e:
        logger.error(f"Failed to create connection: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/connections")
async def list_connections():
    """List all database connections."""
    conn_manager = get_connection_manager()
    return {
        "connections": conn_manager.list_connections()
    }


@router.post("/connections/{conn_id}/test")
async def test_connection(conn_id: str):
    """Test a database connection."""
    conn_manager = get_connection_manager()
    success, message = conn_manager.test_connection(conn_id)
    
    return {
        "success": success,
        "message": message
    }


@router.delete("/connections/{conn_id}")
async def delete_connection(conn_id: str):
    """Delete a database connection."""
    conn_manager = get_connection_manager()
    conn_manager.remove_connection(conn_id)
    
    return {
        "success": True,
        "message": "Connection deleted"
    }


@router.get("/connections/{conn_id}/tables")
async def get_tables(conn_id: str):
    """Get list of tables in a database."""
    try:
        conn_manager = get_connection_manager()
        tables = conn_manager.get_tables(conn_id)
        
        return {
            "tables": tables
        }
    except Exception as e:
        logger.error(f"Failed to get tables: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/connections/{conn_id}/tables/{table_name}/schema")
async def get_table_schema(conn_id: str, table_name: str):
    """Get schema for a specific table."""
    try:
        conn_manager = get_connection_manager()
        schema = conn_manager.get_table_schema(conn_id, table_name)
        
        return {
            "table_name": table_name,
            "schema": schema
        }
    except Exception as e:
        logger.error(f"Failed to get table schema: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/connections/{conn_id}/tables/{table_name}/data")
async def get_table_data(
    conn_id: str,
    table_name: str,
    limit: int = 100,
    offset: int = 0
):
    """Get data from a table with pagination."""
    try:
        conn_manager = get_connection_manager()
        rows, total_count = conn_manager.get_table_data(
            conn_id, table_name, limit, offset
        )
        
        return {
            "table_name": table_name,
            "rows": rows,
            "total_count": total_count,
            "limit": limit,
            "offset": offset
        }
    except Exception as e:
        logger.error(f"Failed to get table data: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# ==========================================
# SCHEMA PARSING & COMPARISON
# ==========================================

@router.post("/schema/parse")
async def parse_schema_file(file: UploadFile = File(...)):
    """Parse a schema definition file."""
    try:
        content = await file.read()
        schema_text = content.decode('utf-8')
        
        tables = SchemaParser.parse_schema_file(schema_text)
        
        return {
            "success": True,
            "tables": {name: table.to_dict() for name, table in tables.items()},
            "table_count": len(tables)
        }
    except Exception as e:
        logger.error(f"Failed to parse schema: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/schema/compare")
async def compare_schemas(
    conn_id: str = Body(...),
    old_table: str = Body(...),
    new_table: str = Body(...),
    new_schema_tables: Dict[str, Any] = Body(...),
    column_mappings: Optional[Dict[str, str]] = Body(None)
):
    """Compare old and new table schemas."""
    try:
        conn_manager = get_connection_manager()
        
        # Get old table schema from database
        old_schema_info = conn_manager.get_table_schema(conn_id, old_table)
        old_table_def = SchemaParser.from_database_schema(old_schema_info, old_table)
        
        # Get new table schema from parsed schema
        new_table_data = new_schema_tables.get(new_table.lower())
        if not new_table_data:
            raise HTTPException(status_code=404, detail=f"New table {new_table} not found in schema")
        
        # Convert dict back to TableDefinition
        from services.api.schema_migration.schema_parser import ColumnDefinition
        new_table_def = TableDefinition(name=new_table_data['name'])
        for col_data in new_table_data['columns']:
            col = ColumnDefinition(**col_data)
            new_table_def.columns.append(col)
        
        # Compare
        comparison = SchemaComparator.compare_tables(
            old_table_def,
            new_table_def,
            column_mappings
        )
        
        return {
            "success": True,
            "comparison": comparison.to_dict()
        }
    except Exception as e:
        logger.error(f"Schema comparison failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# ==========================================
# MAPPING MANAGEMENT
# ==========================================

@router.post("/mappings/single")
async def create_single_mapping(mapping: MappingSingle):
    """Create a single table mapping (1-to-1)."""
    try:
        mapping_id = str(uuid.uuid4())
        result = mapping_engine.add_single_mapping(
            mapping_id=mapping_id,
            source_table=mapping.source_table,
            target_table=mapping.target_table,
            column_mappings=mapping.column_mappings
        )
        
        return {
            "success": True,
            "mapping_id": mapping_id,
            "mapping": result.to_dict()
        }
    except Exception as e:
        logger.error(f"Failed to create mapping: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/mappings/split")
async def create_split_mapping(mapping: MappingSplit):
    """Create a split table mapping (1-to-many)."""
    try:
        mapping_id = str(uuid.uuid4())
        result = mapping_engine.add_split_mapping(
            mapping_id=mapping_id,
            source_table=mapping.source_table,
            target_tables=mapping.target_tables,
            column_mappings=mapping.column_mappings
        )
        
        return {
            "success": True,
            "mapping_id": mapping_id,
            "mapping": result.to_dict()
        }
    except Exception as e:
        logger.error(f"Failed to create split mapping: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/mappings/merge")
async def create_merge_mapping(mapping: MappingMerge):
    """Create a merge table mapping (many-to-1)."""
    try:
        mapping_id = str(uuid.uuid4())
        
        # Convert column mappings
        col_mappings = []
        for cm_dict in mapping.column_mappings:
            col_mappings.append(ColumnMapping(
                source_column=cm_dict['source_column'],
                target_column=cm_dict['target_column'],
                source_table=cm_dict.get('source_table'),
                transform=cm_dict.get('transform')
            ))
        
        result = mapping_engine.add_merge_mapping(
            mapping_id=mapping_id,
            source_tables=mapping.source_tables,
            target_table=mapping.target_table,
            column_mappings=col_mappings,
            join_conditions=mapping.join_conditions
        )
        
        return {
            "success": True,
            "mapping_id": mapping_id,
            "mapping": result.to_dict()
        }
    except Exception as e:
        logger.error(f"Failed to create merge mapping: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/mappings")
async def list_mappings():
    """List all mappings."""
    return {
        "mappings": mapping_engine.list_mappings()
    }


@router.get("/mappings/{mapping_id}")
async def get_mapping(mapping_id: str):
    """Get a specific mapping."""
    mapping = mapping_engine.get_mapping(mapping_id)
    
    if not mapping:
        raise HTTPException(status_code=404, detail="Mapping not found")
    
    return {
        "mapping": mapping.to_dict()
    }


@router.delete("/mappings/{mapping_id}")
async def delete_mapping(mapping_id: str):
    """Delete a mapping."""
    mapping_engine.remove_mapping(mapping_id)
    
    return {
        "success": True,
        "message": "Mapping deleted"
    }


# ==========================================
# MIGRATION EXECUTION
# ==========================================

@router.post("/migrate/execute")
async def execute_migration(request: MigrationExecuteRequest):
    """Execute a migration based on mapping."""
    try:
        conn_manager = get_connection_manager()
        mapping = mapping_engine.get_mapping(request.mapping_id)
        
        if not mapping:
            raise HTTPException(status_code=404, detail="Mapping not found")
        
        # TODO: Get new schemas (need to be stored in session or passed)
        new_schemas = {}  # This would come from parsed schema file
        
        executor = MigrationExecutor(conn_manager)
        result = executor.execute_migration(
            conn_id=request.conn_id,
            mapping=mapping,
            new_schemas=new_schemas,
            batch_size=request.batch_size,
            lossy_confirmed=request.lossy_confirmed
        )
        
        return result
    except Exception as e:
        logger.error(f"Migration execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/migrate/generate-script")
async def generate_migration_script(
    mapping_id: str = Body(...),
    conn_id: str = Body(...)
):
    """Generate a Python script for manual migration."""
    try:
        mapping = mapping_engine.get_mapping(mapping_id)
        if not mapping:
            raise HTTPException(status_code=404, detail="Mapping not found")
        
        conn_manager = get_connection_manager()
        if conn_id not in conn_manager.connections:
            raise HTTPException(status_code=404, detail="Connection not found")
        
        connection_info = conn_manager.connections[conn_id]
        
        # TODO: Get schemas (need schema storage)
        old_schema = {}
        new_schemas = {}
        
        script_path = ScriptGenerator.generate_manual_script(
            mapping=mapping,
            old_schema=old_schema,
            new_schemas=new_schemas,
            connection_info=connection_info,
            output_dir="."
        )
        
        return {
            "success": True,
            "script_path": script_path,
            "message": f"Script generated: {script_path}"
        }
    except Exception as e:
        logger.error(f"Script generation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# DATA TYPE ANALYSIS
# ==========================================

@router.post("/analyze/conversion")
async def analyze_conversion(
    source_type: str = Body(...),
    target_type: str = Body(...)
):
    """Analyze the safety of a data type conversion."""
    safety, reason = DataTypeAnalyzer.analyze_conversion(source_type, target_type)
    needs_cast, cast_expr = DataTypeAnalyzer.requires_cast(source_type, target_type)
    
    return {
        "source_type": source_type,
        "target_type": target_type,
        "safety": safety.value,
        "reason": reason,
        "needs_cast":  needs_cast,
        "cast_expression": cast_expr
    }


# ==========================================
# UTILITY ENDPOINTS
# ==========================================

@router.post("/export/data")
async def export_table_data(request: DataExportRequest):
    """Export table data to JSON or CSV."""
    try:
        conn_manager = get_connection_manager()
        rows, total_count = conn_manager.get_table_data(
            request.conn_id,
            request.table_name,
            limit=request.limit,
            offset=0
        )
        
        if request.format == "csv":
            # Convert to CSV
            import csv
            import io
            
            if not rows:
                return {"data": "", "format": "csv"}
            
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
            
            return {
                "data": output.getvalue(),
                "format": "csv",
                "row_count": len(rows)
            }
        else:
            # JSON format
            return {
                "data": rows,
                "format": "json",
                "row_count": len(rows),
                "total_count": total_count
            }
    
    except Exception as e:
        logger.error(f"Data export failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "schema-migration"
    }
