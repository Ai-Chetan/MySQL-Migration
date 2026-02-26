"""
Authentication and User Management
"""
import os
import jwt
from datetime import datetime, timedelta
from typing import Optional
from passlib.context import CryptContext
from uuid import UUID, uuid4

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# JWT settings
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24  # 24 hours


def hash_password(password: str) -> str:
    """Hash a password. Truncate to 72 bytes for bcrypt compatibility."""
    # Bcrypt has a 72-byte limit, truncate if necessary
    password_bytes = password.encode('utf-8')[:72]
    truncated_password = password_bytes.decode('utf-8', errors='ignore')
    return pwd_context.hash(truncated_password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against a hash. Truncate to 72 bytes for bcrypt compatibility."""
    # Bcrypt has a 72-byte limit, truncate if necessary
    password_bytes = plain_password.encode('utf-8')[:72]
    truncated_password = password_bytes.decode('utf-8', errors='ignore')
    return pwd_context.verify(truncated_password, hashed_password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Create JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def decode_access_token(token: str) -> Optional[dict]:
    """Decode JWT access token."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.PyJWTError:
        return None


class UserRepository:
    """User database operations."""
    
    def __init__(self, db):
        self.db = db
    
    def create_user(self, email: str, password: str, tenant_id: UUID, role: str = "user") -> UUID:
        """Create a new user."""
        user_id = uuid4()
        hashed_password = hash_password(password)
        
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            INSERT INTO users (id, email, password_hash, tenant_id, role, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (str(user_id), email, hashed_password, str(tenant_id), role, datetime.utcnow())
        )
        
        conn.commit()
        self.db.return_connection(conn)
        
        return user_id
    
    def get_user_by_email(self, email: str) -> Optional[dict]:
        """Get user by email."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT * FROM users WHERE email = %s",
            (email,)
        )
        
        user = cursor.fetchone()
        self.db.return_connection(conn)
        
        return user
    
    def get_user_by_id(self, user_id: UUID) -> Optional[dict]:
        """Get user by ID."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT * FROM users WHERE id = %s",
            (str(user_id),)
        )
        
        user = cursor.fetchone()
        self.db.return_connection(conn)
        
        return user
    
    def authenticate_user(self, email: str, password: str) -> Optional[dict]:
        """Authenticate user with email and password."""
        user = self.get_user_by_email(email)
        if not user:
            return None
        
        if not verify_password(password, user['password_hash']):
            return None
        
        return user


class TenantRepository:
    """Tenant database operations."""
    
    def __init__(self, db):
        self.db = db
    
    def create_tenant(self, name: str, plan: str = "free") -> UUID:
        """Create a new tenant."""
        tenant_id = uuid4()
        
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            INSERT INTO tenants (id, name, plan, created_at, is_active)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (str(tenant_id), name, plan, datetime.utcnow(), True)
        )
        
        conn.commit()
        self.db.return_connection(conn)
        
        return tenant_id
    
    def get_tenant(self, tenant_id: UUID) -> Optional[dict]:
        """Get tenant by ID."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT * FROM tenants WHERE id = %s",
            (str(tenant_id),)
        )
        
        tenant = cursor.fetchone()
        self.db.return_connection(conn)
        
        return tenant
    
    def list_tenant_users(self, tenant_id: UUID):
        """List all users in a tenant."""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT id, email, role, created_at, last_login FROM users WHERE tenant_id = %s",
            (str(tenant_id),)
        )
        
        users = cursor.fetchall()
        self.db.return_connection(conn)
        
        return users
