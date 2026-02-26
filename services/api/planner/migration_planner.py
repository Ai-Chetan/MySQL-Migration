"""
Chunk planner service for analyzing tables and creating migration plan.
"""
from uuid import UUID
from typing import List, Dict, Any, Tuple, Optional
import mysql.connector
from mysql.connector import Error as MySQLError

from shared.models import DatabaseConfig, TableMetadata, ChunkRange
from shared.chunking import ChunkPlanner, validate_primary_key
from shared.utils import setup_logger
from services.api.metadata.repository import MetadataRepository

logger = setup_logger(__name__)


class MigrationPlanner:
    """Handles migration planning and chunk creation."""
    
    def __init__(
        self,
        metadata_repo: MetadataRepository,
        chunk_size: int = 100000
    ):
        """
        Initialize migration planner.
        
        Args:
            metadata_repo: Metadata repository instance
            chunk_size: Target rows per chunk
        """
        self.metadata_repo = metadata_repo
        self.chunk_planner = ChunkPlanner(target_rows_per_chunk=chunk_size)
    
    def analyze_and_plan(
        self,
        job_id: UUID,
        source_config: DatabaseConfig
    ) -> List[UUID]:
        """
        Analyze source database and create migration plan.
        
        This will:
        1. Connect to source database
        2. Discover all tables
        3. For each table:
           - Detect primary key
           - Count rows
           - Calculate chunk ranges
           - Create metadata records
        
        Args:
            job_id: Migration job UUID
            source_config: Source database configuration
        
        Returns:
            List of created chunk UUIDs
        """
        logger.info(f"Starting migration planning for job {job_id}")
        
        conn = None
        chunk_ids = []
        
        try:
            # Connect to source database
            conn = mysql.connector.connect(
                host=source_config.host,
                port=source_config.port,
                user=source_config.user,
                password=source_config.password,
                database=source_config.database,
                charset=source_config.charset
            )
            
            cursor = conn.cursor()
            
            # Get list of tables
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            
            logger.info(f"Found {len(tables)} tables to analyze")
            
            total_tables = 0
            total_chunks = 0
            
            # Process each table
            for table_name in tables:
                try:
                    # Analyze table
                    table_meta = self._analyze_table(cursor, table_name)
                    
                    if not table_meta:
                        logger.warning(f"Skipping table {table_name} (no PK or empty)")
                        continue
                    
                    # Create table record
                    table_id = self.metadata_repo.create_table(
                        job_id=job_id,
                        table_name=table_name,
                        primary_key_column=table_meta.primary_key_column,
                        total_rows=table_meta.total_rows
                    )
                    
                    # Calculate chunks
                    chunk_ranges = self.chunk_planner.calculate_chunks(table_meta)
                    
                    # Create chunk records
                    table_chunk_ids = self._create_chunks(
                        job_id=job_id,
                        table_id=table_id,
                        table_name=table_name,
                        chunk_ranges=chunk_ranges
                    )
                    
                    chunk_ids.extend(table_chunk_ids)
                    total_tables += 1
                    total_chunks += len(chunk_ranges)
                    
                    logger.info(
                        f"Table {table_name}: {table_meta.total_rows} rows, "
                        f"{len(chunk_ranges)} chunks created"
                    )
                    
                except Exception as e:
                    logger.error(f"Failed to process table {table_name}: {e}")
                    # Continue with other tables
            
            # Update job totals
            self._update_job_totals(job_id, total_tables, total_chunks)
            
            logger.info(
                f"Planning complete: {total_tables} tables, "
                f"{total_chunks} chunks created"
            )
            
            return chunk_ids
            
        except MySQLError as e:
            logger.error(f"MySQL error during planning: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during planning: {e}")
            raise
        finally:
            if conn and conn.is_connected():
                conn.close()
    
    def _analyze_table(
        self,
        cursor,
        table_name: str
    ) -> Optional[TableMetadata]:
        """
        Analyze a single table.
        
        Args:
            cursor: MySQL cursor
            table_name: Table name to analyze
        
        Returns:
            TableMetadata or None if table should be skipped
        """
        # Detect primary key
        cursor.execute(
            f"SHOW KEYS FROM `{table_name}` WHERE Key_name = 'PRIMARY'"
        )
        pk_rows = cursor.fetchall()
        
        if not pk_rows:
            logger.warning(f"Table {table_name} has no primary key, skipping")
            return None
        
        # Assume single-column primary key for Phase 1
        primary_key_column = pk_rows[0][4]  # Column_name
        
        if not validate_primary_key(primary_key_column):
            return None
        
        # Get row count and PK range
        cursor.execute(
            f"""
            SELECT 
                COUNT(*) as total_rows,
                MIN(`{primary_key_column}`) as min_pk,
                MAX(`{primary_key_column}`) as max_pk
            FROM `{table_name}`
            """
        )
        
        result = cursor.fetchone()
        total_rows = result[0]
        min_pk = result[1]
        max_pk = result[2]
        
        if total_rows == 0 or min_pk is None or max_pk is None:
            logger.warning(f"Table {table_name} is empty, skipping")
            return None
        
        return TableMetadata(
            table_name=table_name,
            primary_key_column=primary_key_column,
            total_rows=total_rows,
            min_pk=int(min_pk),
            max_pk=int(max_pk),
            estimated_chunks=0  # Will be calculated
        )
    
    def _create_chunks(
        self,
        job_id: UUID,
        table_id: UUID,
        table_name: str,
        chunk_ranges: List[ChunkRange]
    ) -> List[UUID]:
        """
        Create chunk records in metadata database.
        
        Args:
            job_id: Job UUID
            table_id: Table UUID
            table_name: Table name
            chunk_ranges: List of chunk ranges
        
        Returns:
            List of created chunk UUIDs
        """
        chunk_ids = []
        
        for chunk_range in chunk_ranges:
            chunk_id = self.metadata_repo.create_chunk(
                job_id=job_id,
                table_id=table_id,
                table_name=table_name,
                pk_start=chunk_range.pk_start,
                pk_end=chunk_range.pk_end
            )
            chunk_ids.append(chunk_id)
        
        return chunk_ids
    
    def _update_job_totals(
        self,
        job_id: UUID,
        total_tables: int,
        total_chunks: int
    ):
        """
        Update job with total counts.
        
        Args:
            job_id: Job UUID
            total_tables: Total number of tables
            total_chunks: Total number of chunks
        """
        conn = self.metadata_repo.db.get_connection()
        
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE migration_jobs
                SET total_tables = %s, total_chunks = %s
                WHERE id = %s
                """,
                (total_tables, total_chunks, str(job_id))
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to update job totals: {e}")
            raise
        finally:
            self.metadata_repo.db.return_connection(conn)
