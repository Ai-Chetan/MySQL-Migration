"""
Chunking logic for splitting tables into processable ranges.
"""
from typing import List, Optional, Dict, Any
from shared.models import ChunkRange, TableMetadata
from shared.utils import setup_logger
import math

logger = setup_logger(__name__)


class ChunkPlanner:
    """Handles chunk range calculation for tables."""
    
    def __init__(self, target_rows_per_chunk: int = 100000):
        """
        Initialize chunk planner.
        
        Args:
            target_rows_per_chunk: Target number of rows per chunk
        """
        self.target_rows_per_chunk = target_rows_per_chunk
    
    def calculate_chunks(self, table_meta: TableMetadata) -> List[ChunkRange]:
        """
        Calculate chunk ranges for a table based on primary key range.
        
        Args:
            table_meta: Table metadata with PK range information
        
        Returns:
            List of chunk ranges
        """
        min_pk = table_meta.min_pk
        max_pk = table_meta.max_pk
        total_rows = table_meta.total_rows
        
        # Handle small tables
        if total_rows <= self.target_rows_per_chunk:
            logger.info(
                f"Table {table_meta.table_name} has {total_rows} rows, "
                f"creating single chunk"
            )
            return [ChunkRange(
                pk_start=min_pk,
                pk_end=max_pk,
                estimated_rows=total_rows
            )]
        
        # Calculate number of chunks needed
        num_chunks = math.ceil(total_rows / self.target_rows_per_chunk)
        
        # Calculate PK range per chunk
        # Note: This assumes fairly uniform PK distribution
        # For sparse PKs, chunks may have varying row counts
        pk_range = max_pk - min_pk
        range_per_chunk = math.ceil(pk_range / num_chunks)
        
        chunks = []
        current_start = min_pk
        
        while current_start <= max_pk:
            current_end = min(current_start + range_per_chunk - 1, max_pk)
            
            # Estimate rows in this chunk (rough approximation)
            estimated_rows = self._estimate_rows_in_range(
                current_start,
                current_end,
                min_pk,
                max_pk,
                total_rows
            )
            
            chunks.append(ChunkRange(
                pk_start=current_start,
                pk_end=current_end,
                estimated_rows=estimated_rows
            ))
            
            current_start = current_end + 1
        
        logger.info(
            f"Table {table_meta.table_name}: created {len(chunks)} chunks "
            f"from PK range [{min_pk}, {max_pk}]"
        )
        
        return chunks
    
    def _estimate_rows_in_range(
        self,
        start_pk: int,
        end_pk: int,
        min_pk: int,
        max_pk: int,
        total_rows: int
    ) -> int:
        """
        Estimate number of rows in a PK range.
        
        Assumes uniform distribution for estimation.
        Actual row count may vary, especially with sparse PKs.
        
        Args:
            start_pk: Range start
            end_pk: Range end
            min_pk: Table minimum PK
            max_pk: Table maximum PK
            total_rows: Total rows in table
        
        Returns:
            Estimated row count
        """
        if max_pk == min_pk:
            return total_rows
        
        range_covered = end_pk - start_pk + 1
        total_range = max_pk - min_pk + 1
        
        estimated = int((range_covered / total_range) * total_rows)
        return max(1, estimated)  # At least 1 row
    
    def adjust_chunk_size(
        self,
        current_size: int,
        performance_factor: float
    ) -> int:
        """
        Dynamically adjust chunk size based on performance.
        
        Args:
            current_size: Current chunk size
            performance_factor: Performance multiplier (>1 = increase, <1 = decrease)
        
        Returns:
            Adjusted chunk size
        """
        new_size = int(current_size * performance_factor)
        
        # Keep within reasonable bounds
        min_size = 10000
        max_size = 500000
        
        return max(min_size, min(new_size, max_size))


def validate_primary_key(pk_column: Optional[str]) -> bool:
    """
    Validate that a primary key exists for chunking.
    
    Args:
        pk_column: Primary key column name
    
    Returns:
        True if valid, False otherwise
    """
    return pk_column is not None and len(pk_column) > 0
