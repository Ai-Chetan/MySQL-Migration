"""
Secrets Vault Service
Encrypted storage for database credentials and API keys
"""
import json
import base64
from typing import Dict, Any, Optional
from uuid import UUID
from datetime import datetime
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import os

from services.worker.db import MetadataConnection
from shared.utils import setup_logger

logger = setup_logger(__name__)


class SecretsVault:
    """Manage encrypted secrets storage."""
    
    def __init__(self, metadata_conn: MetadataConnection, master_key: str = None):
        self.metadata_conn = metadata_conn
        
        # Get master key from environment or parameter
        self.master_key = master_key or os.getenv('VAULT_MASTER_KEY', 'default-master-key-change-in-production')
        
        # Derive encryption key from master key
        self.cipher = self._get_cipher()
        self.key_id = "key-v1"  # For key rotation tracking
    
    def _get_cipher(self) -> Fernet:
        """Derive Fernet cipher from master key."""
        # Use PBKDF2 to derive a key from the master password
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=b'migration-platform-salt',  # In production, use random salt per key
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(self.master_key.encode()))
        return Fernet(key)
    
    def _log_structured(self, level: str, message: str, **kwargs):
        """Emit structured JSON log."""
        log_data = {
            "level": level.upper(),
            "service": "secrets_vault",
            "message": message,
            **kwargs
        }
        
        log_line = json.dumps(log_data)
        
        if level == "error":
            logger.error(log_line)
        elif level == "warning":
            logger.warning(log_line)
        else:
            logger.info(log_line)
    
    def store_secret(
        self,
        tenant_id: UUID,
        secret_name: str,
        secret_value: str,
        secret_type: str = 'database',
        user_id: UUID = None,
        expires_at: datetime = None,
        metadata: Dict[str, Any] = None
    ) -> UUID:
        """
        Store an encrypted secret.
        
        Args:
            tenant_id: Tenant ID
            secret_name: Unique name for the secret
            secret_value: Plain text secret to encrypt
            secret_type: Type of secret ('database', 'api_key', 'ssh_key')
            user_id: User who created the secret
            expires_at: Expiration timestamp (optional)
            metadata: Additional context
            
        Returns:
            Secret ID
        """
        cursor = self.metadata_conn.get_cursor()
        
        try:
            # Encrypt the secret
            encrypted_value = self.cipher.encrypt(secret_value.encode()).decode()
            
            # Insert into vault
            cursor.execute(
                """
                INSERT INTO secrets_vault
                (tenant_id, secret_type, secret_name, encrypted_value, encryption_key_id, 
                 created_by, expires_at, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tenant_id, secret_name) 
                DO UPDATE SET 
                    encrypted_value = EXCLUDED.encrypted_value,
                    encryption_key_id = EXCLUDED.encryption_key_id,
                    updated_at = NOW()
                RETURNING id
                """,
                (
                    str(tenant_id),
                    secret_type,
                    secret_name,
                    encrypted_value,
                    self.key_id,
                    str(user_id) if user_id else None,
                    expires_at,
                    json.dumps(metadata or {})
                )
            )
            
            result = cursor.fetchone()
            secret_id = result['id']
            
            self.metadata_conn.commit()
            
            self._log_structured(
                "info",
                "Secret stored",
                tenant_id=str(tenant_id),
                secret_name=secret_name,
                secret_type=secret_type
            )
            
            return secret_id
            
        except Exception as e:
            self.metadata_conn.rollback()
            self._log_structured(
                "error",
                "Failed to store secret",
                tenant_id=str(tenant_id),
                secret_name=secret_name,
                error=str(e)
            )
            raise
    
    def retrieve_secret(
        self,
        tenant_id: UUID,
        secret_name: str
    ) -> Optional[str]:
        """
        Retrieve and decrypt a secret.
        
        Args:
            tenant_id: Tenant ID
            secret_name: Name of the secret
            
        Returns:
            Decrypted secret value or None if not found
        """
        cursor = self.metadata_conn.get_cursor()
        
        try:
            cursor.execute(
                """
                SELECT id, encrypted_value, expires_at
                FROM secrets_vault
                WHERE tenant_id = %s AND secret_name = %s
                """,
                (str(tenant_id), secret_name)
            )
            
            result = cursor.fetchone()
            
            if not result:
                return None
            
            # Check expiration
            if result['expires_at'] and result['expires_at'] < datetime.now():
                self._log_structured(
                    "warning",
                    "Attempted to access expired secret",
                    tenant_id=str(tenant_id),
                    secret_name=secret_name
                )
                return None
            
            # Decrypt
            decrypted_value = self.cipher.decrypt(result['encrypted_value'].encode()).decode()
            
            # Update access tracking
            cursor.execute(
                """
                UPDATE secrets_vault
                SET last_accessed_at = NOW(), access_count = access_count + 1
                WHERE id = %s
                """,
                (result['id'],)
            )
            self.metadata_conn.commit()
            
            self._log_structured(
                "info",
                "Secret accessed",
                tenant_id=str(tenant_id),
                secret_name=secret_name
            )
            
            return decrypted_value
            
        except Exception as e:
            self._log_structured(
                "error",
                "Failed to retrieve secret",
                tenant_id=str(tenant_id),
                secret_name=secret_name,
                error=str(e)
            )
            return None
    
    def list_secrets(
        self,
        tenant_id: UUID,
        secret_type: str = None
    ) -> list:
        """
        List secrets for a tenant (metadata only, not values).
        
        Args:
            tenant_id: Tenant ID
            secret_type: Filter by type (optional)
            
        Returns:
            List of secret metadata
        """
        cursor = self.metadata_conn.get_cursor()
        
        query = """
            SELECT 
                id, secret_type, secret_name, last_accessed_at, 
                access_count, expires_at, created_at
            FROM secrets_vault
            WHERE tenant_id = %s
        """
        params = [str(tenant_id)]
        
        if secret_type:
            query += " AND secret_type = %s"
            params.append(secret_type)
        
        query += " ORDER BY created_at DESC"
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        return [
            {
                'id': str(row['id']),
                'type': row['secret_type'],
                'name': row['secret_name'],
                'last_accessed': row['last_accessed_at'].isoformat() if row['last_accessed_at'] else None,
                'access_count': row['access_count'],
                'expires_at': row['expires_at'].isoformat() if row['expires_at'] else None,
                'created_at': row['created_at'].isoformat()
            }
            for row in results
        ]
    
    def delete_secret(
        self,
        tenant_id: UUID,
        secret_name: str
    ) -> bool:
        """
        Delete a secret.
        
        Args:
            tenant_id: Tenant ID
            secret_name: Name of the secret
            
        Returns:
            True if deleted, False if not found
        """
        cursor = self.metadata_conn.get_cursor()
        
        try:
            cursor.execute(
                """
                DELETE FROM secrets_vault
                WHERE tenant_id = %s AND secret_name = %s
                RETURNING id
                """,
                (str(tenant_id), secret_name)
            )
            
            result = cursor.fetchone()
            self.metadata_conn.commit()
            
            if result:
                self._log_structured(
                    "info",
                    "Secret deleted",
                    tenant_id=str(tenant_id),
                    secret_name=secret_name
                )
                return True
            
            return False
            
        except Exception as e:
            self.metadata_conn.rollback()
            self._log_structured(
                "error",
                "Failed to delete secret",
                tenant_id=str(tenant_id),
                secret_name=secret_name,
                error=str(e)
            )
            return False
    
    def rotate_encryption_key(self):
        """
        Rotate encryption key (re-encrypt all secrets with new key).
        This is a sensitive operation and should be done during maintenance windows.
        """
        # Implementation for key rotation
        # 1. Generate new key
        # 2. Fetch all secrets
        # 3. Decrypt with old key
        # 4. Encrypt with new key
        # 5. Update database
        pass  # Complex operation - implement when needed


def get_secrets_vault() -> SecretsVault:
    """Get secrets vault instance."""
    metadata_conn = MetadataConnection()
    return SecretsVault(metadata_conn)
