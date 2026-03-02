"""
S3-Compatible Object Storage for Migration Data
Stores chunks, checkpoints, and intermediate data
"""
import os
import logging
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from typing import Optional, BinaryIO, Dict, Any
from datetime import datetime, timedelta
from uuid import UUID

logger = logging.getLogger(__name__)


class S3Config:
    """S3/MinIO configuration."""
    
    ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
    ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
    SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
    BUCKET = os.getenv("S3_BUCKET", "migration-data")
    REGION = os.getenv("S3_REGION", "us-east-1")
    
    # Path prefixes
    CHUNKS_PREFIX = "chunks/"
    CHECKPOINTS_PREFIX = "checkpoints/"
    EXPORTS_PREFIX = "exports/"
    TEMP_PREFIX = "temp/"


class ObjectStorageService:
    """S3-compatible object storage for migration data."""
    
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            endpoint_url=S3Config.ENDPOINT,
            aws_access_key_id=S3Config.ACCESS_KEY,
            aws_secret_access_key=S3Config.SECRET_KEY,
            region_name=S3Config.REGION,
            config=Config(signature_version='s3v4')
        )
        
        self.bucket = S3Config.BUCKET
        self._ensure_bucket_exists()
        
        logger.info(f"Object Storage initialized: {S3Config.ENDPOINT}/{S3Config.BUCKET}")
    
    def _ensure_bucket_exists(self):
        """Create bucket if it doesn't exist."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
            logger.info(f"Bucket '{self.bucket}' exists")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.info(f"Creating bucket '{self.bucket}'...")
                try:
                    self.s3_client.create_bucket(Bucket=self.bucket)
                    logger.info(f"Bucket '{self.bucket}' created successfully")
                except ClientError as create_error:
                    logger.error(f"Failed to create bucket: {create_error}")
            else:
                logger.error(f"Error checking bucket: {e}")
    
    def store_chunk_data(self, job_id: str, chunk_id: str, data: bytes) -> bool:
        """
        Store chunk data in object storage.
        
        Args:
            job_id: Migration job ID
            chunk_id: Chunk identifier
            data: Binary chunk data
        
        Returns:
            True if successful
        """
        try:
            key = f"{S3Config.CHUNKS_PREFIX}{job_id}/{chunk_id}.dat"
            
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=data,
                ContentType='application/octet-stream',
                Metadata={
                    'job_id': job_id,
                    'chunk_id': chunk_id,
                    'timestamp': datetime.utcnow().isoformat()
                }
            )
            
            logger.info(f"Stored chunk {chunk_id} for job {job_id}: {len(data)} bytes")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to store chunk {chunk_id}: {e}")
            return False
    
    def retrieve_chunk_data(self, job_id: str, chunk_id: str) -> Optional[bytes]:
        """
        Retrieve chunk data from object storage.
        
        Args:
            job_id: Migration job ID
            chunk_id: Chunk identifier
        
        Returns:
            Binary chunk data or None if not found
        """
        try:
            key = f"{S3Config.CHUNKS_PREFIX}{job_id}/{chunk_id}.dat"
            
            response = self.s3_client.get_object(
                Bucket=self.bucket,
                Key=key
            )
            
            data = response['Body'].read()
            logger.info(f"Retrieved chunk {chunk_id}: {len(data)} bytes")
            return data
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.warning(f"Chunk {chunk_id} not found")
            else:
                logger.error(f"Failed to retrieve chunk {chunk_id}: {e}")
            return None
    
    def delete_chunk_data(self, job_id: str, chunk_id: str) -> bool:
        """Delete chunk data after successful processing."""
        try:
            key = f"{S3Config.CHUNKS_PREFIX}{job_id}/{chunk_id}.dat"
            
            self.s3_client.delete_object(
                Bucket=self.bucket,
                Key=key
            )
            
            logger.info(f"Deleted chunk {chunk_id}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to delete chunk {chunk_id}: {e}")
            return False
    
    def store_checkpoint(self, job_id: str, checkpoint_data: Dict[str, Any]) -> bool:
        """
        Store job checkpoint for resumability.
        
        Args:
            job_id: Migration job ID
            checkpoint_data: Checkpoint state dictionary
        
        Returns:
            True if successful
        """
        try:
            import json
            key = f"{S3Config.CHECKPOINTS_PREFIX}{job_id}/checkpoint.json"
            
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(checkpoint_data, default=str).encode('utf-8'),
                ContentType='application/json',
                Metadata={
                    'job_id': job_id,
                    'timestamp': datetime.utcnow().isoformat()
                }
            )
            
            logger.info(f"Stored checkpoint for job {job_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store checkpoint for job {job_id}: {e}")
            return False
    
    def retrieve_checkpoint(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve job checkpoint for resuming.
        
        Args:
            job_id: Migration job ID
        
        Returns:
            Checkpoint data dictionary or None
        """
        try:
            import json
            key = f"{S3Config.CHECKPOINTS_PREFIX}{job_id}/checkpoint.json"
            
            response = self.s3_client.get_object(
                Bucket=self.bucket,
                Key=key
            )
            
            data = json.loads(response['Body'].read().decode('utf-8'))
            logger.info(f"Retrieved checkpoint for job {job_id}")
            return data
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.info(f"No checkpoint found for job {job_id}")
            else:
                logger.error(f"Failed to retrieve checkpoint: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing checkpoint: {e}")
            return None
    
    def store_export(self, job_id: str, filename: str, data: BinaryIO) -> Optional[str]:
        """
        Store exported data file.
        
        Args:
            job_id: Migration job ID
            filename: Export filename
            data: File-like binary data
        
        Returns:
            Presigned URL for download or None
        """
        try:
            key = f"{S3Config.EXPORTS_PREFIX}{job_id}/{filename}"
            
            self.s3_client.upload_fileobj(
                data,
                self.bucket,
                key,
                ExtraArgs={
                    'ContentType': 'application/octet-stream',
                    'Metadata': {
                        'job_id': job_id,
                        'filename': filename,
                        'timestamp': datetime.utcnow().isoformat()
                    }
                }
            )
            
            # Generate presigned URL (valid for 24 hours)
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket, 'Key': key},
                ExpiresIn=86400  # 24 hours
            )
            
            logger.info(f"Stored export {filename} for job {job_id}")
            return url
            
        except Exception as e:
            logger.error(f"Failed to store export {filename}: {e}")
            return None
    
    def cleanup_job_data(self, job_id: str) -> bool:
        """
        Clean up all object storage data for a job.
        
        Args:
            job_id: Migration job ID
        
        Returns:
            True if successful
        """
        try:
            prefixes = [
                f"{S3Config.CHUNKS_PREFIX}{job_id}/",
                f"{S3Config.CHECKPOINTS_PREFIX}{job_id}/",
            ]
            
            deleted_count = 0
            for prefix in prefixes:
                # List all objects with prefix
                paginator = self.s3_client.get_paginator('list_objects_v2')
                pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)
                
                for page in pages:
                    if 'Contents' in page:
                        objects = [{'Key': obj['Key']} for obj in page['Contents']]
                        if objects:
                            self.s3_client.delete_objects(
                                Bucket=self.bucket,
                                Delete={'Objects': objects}
                            )
                            deleted_count += len(objects)
            
            logger.info(f"Cleaned up {deleted_count} objects for job {job_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to cleanup job {job_id}: {e}")
            return False
    
    def get_object_metadata(self, key: str) -> Optional[Dict[str, Any]]:
        """Get metadata for an object."""
        try:
            response = self.s3_client.head_object(
                Bucket=self.bucket,
                Key=key
            )
            return {
                'size': response['ContentLength'],
                'last_modified': response['LastModified'],
                'metadata': response.get('Metadata', {})
            }
        except ClientError as e:
            logger.error(f"Failed to get metadata for {key}: {e}")
            return None
    
    def list_job_chunks(self, job_id: str) -> list:
        """List all chunks for a job."""
        try:
            prefix = f"{S3Config.CHUNKS_PREFIX}{job_id}/"
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                return []
            
            chunks = []
            for obj in response['Contents']:
                chunk_id = obj['Key'].split('/')[-1].replace('.dat', '')
                chunks.append({
                    'chunk_id': chunk_id,
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat()
                })
            
            return chunks
            
        except Exception as e:
            logger.error(f"Failed to list chunks for job {job_id}: {e}")
            return []


# Global instance
_storage = None


def get_object_storage() -> ObjectStorageService:
    """Get global object storage instance."""
    global _storage
    if _storage is None:
        _storage = ObjectStorageService()
    return _storage
