"""
Authentication and tenant routes.
"""
from fastapi import APIRouter, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr
from uuid import UUID
from typing import Optional, List
from datetime import timedelta

from services.api.auth import (
    UserRepository,
    TenantRepository,
    create_access_token,
    decode_access_token,
    ACCESS_TOKEN_EXPIRE_MINUTES
)
from services.api.metadata import get_metadata_db
from shared.utils import setup_logger

logger = setup_logger(__name__)
router = APIRouter()
security = HTTPBearer()


# Request/Response Models
class SignupRequest(BaseModel):
    email: EmailStr
    password: str
    tenant_name: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str
    user_id: str
    tenant_id: str
    role: str


class UserResponse(BaseModel):
    id: UUID
    email: str
    role: str
    tenant_id: UUID


class TenantResponse(BaseModel):
    id: UUID
    name: str
    plan: str
    is_active: bool


class InviteUserRequest(BaseModel):
    email: EmailStr
    role: str = "user"


# Dependencies
def get_user_repo() -> UserRepository:
    """Get user repository."""
    return UserRepository(get_metadata_db())


def get_tenant_repo() -> TenantRepository:
    """Get tenant repository."""
    return TenantRepository(get_metadata_db())


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    user_repo: UserRepository = Depends(get_user_repo)
) -> dict:
    """Get current authenticated user."""
    token = credentials.credentials
    payload = decode_access_token(token)
    
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials"
        )
    
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload"
        )
    
    user = user_repo.get_user_by_id(UUID(user_id))
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found"
        )
    
    return user


async def get_current_tenant(
    current_user: dict = Depends(get_current_user),
    tenant_repo: TenantRepository = Depends(get_tenant_repo)
) -> dict:
    """Get current user's tenant."""
    tenant = tenant_repo.get_tenant(UUID(current_user['tenant_id']))
    if not tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Tenant not found"
        )
    
    if not tenant['is_active']:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Tenant account is not active"
        )
    
    return tenant


# Routes
@router.post("/auth/signup", response_model=TokenResponse, status_code=201)
async def signup(
    request: SignupRequest,
    user_repo: UserRepository = Depends(get_user_repo),
    tenant_repo: TenantRepository = Depends(get_tenant_repo)
):
    """
    Sign up a new user and create a new tenant.
    """
    try:
        # Check if user already exists
        existing_user = user_repo.get_user_by_email(request.email)
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already registered"
            )
        
        # Create tenant
        tenant_id = tenant_repo.create_tenant(request.tenant_name)
        
        # Create user as admin of the tenant
        user_id = user_repo.create_user(
            email=request.email,
            password=request.password,
            tenant_id=tenant_id,
            role="admin"
        )
        
        # Create access token
        access_token = create_access_token(
            data={"sub": str(user_id), "tenant_id": str(tenant_id), "role": "admin"},
            expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        )
        
        logger.info(f"New user signup: {request.email} for tenant {tenant_id}")
        
        return TokenResponse(
            access_token=access_token,
            token_type="bearer",
            user_id=str(user_id),
            tenant_id=str(tenant_id),
            role="admin"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Signup failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/auth/login", response_model=TokenResponse)
async def login(
    request: LoginRequest,
    user_repo: UserRepository = Depends(get_user_repo)
):
    """
    Login with email and password.
    """
    try:
        user = user_repo.authenticate_user(request.email, request.password)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password"
            )
        
        # Create access token
        access_token = create_access_token(
            data={
                "sub": str(user['id']),
                "tenant_id": str(user['tenant_id']),
                "role": user['role']
            },
            expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        )
        
        logger.info(f"User login: {request.email}")
        
        return TokenResponse(
            access_token=access_token,
            token_type="bearer",
            user_id=str(user['id']),
            tenant_id=str(user['tenant_id']),
            role=user['role']
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Login failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/auth/me", response_model=UserResponse)
async def get_me(current_user: dict = Depends(get_current_user)):
    """
    Get current user info.
    """
    return UserResponse(
        id=UUID(current_user['id']) if isinstance(current_user['id'], str) else current_user['id'],
        email=current_user['email'],
        role=current_user['role'],
        tenant_id=UUID(current_user['tenant_id']) if isinstance(current_user['tenant_id'], str) else current_user['tenant_id']
    )


@router.get("/tenant", response_model=TenantResponse)
async def get_tenant_info(current_tenant: dict = Depends(get_current_tenant)):
    """
    Get current tenant info.
    """
    return TenantResponse(
        id=UUID(current_tenant['id']) if isinstance(current_tenant['id'], str) else current_tenant['id'],
        name=current_tenant['name'],
        plan=current_tenant['plan'],
        is_active=current_tenant['is_active']
    )


@router.get("/tenant/users", response_model=List[UserResponse])
async def list_tenant_users(
    current_user: dict = Depends(get_current_user),
    tenant_repo: TenantRepository = Depends(get_tenant_repo)
):
    """
    List all users in the current tenant.
    Requires admin role.
    """
    if current_user['role'] != 'admin':
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    
    users = tenant_repo.list_tenant_users(UUID(current_user['tenant_id']))
    
    return [
        UserResponse(
            id=UUID(u['id']) if isinstance(u['id'], str) else u['id'],
            email=u['email'],
            role=u['role'],
            tenant_id=UUID(u['tenant_id']) if isinstance(u['tenant_id'], str) else u['tenant_id']
        )
        for u in users
    ]


@router.post("/tenant/invite", status_code=201)
async def invite_user(
    request: InviteUserRequest,
    current_user: dict = Depends(get_current_user),
    user_repo: UserRepository = Depends(get_user_repo)
):
    """
    Invite a new user to the tenant.
    Requires admin role.
    """
    if current_user['role'] != 'admin':
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    
    try:
        # Check if user already exists
        existing_user = user_repo.get_user_by_email(request.email)
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="User already exists"
            )
        
        # Create user with temporary password (should send email in production)
        temp_password = "changeme123"  # In production, generate and email this
        user_id = user_repo.create_user(
            email=request.email,
            password=temp_password,
            tenant_id=UUID(current_user['tenant_id']),
            role=request.role
        )
        
        logger.info(f"User invited: {request.email} to tenant {current_user['tenant_id']}")
        
        return {
            "message": "User invited successfully",
            "user_id": str(user_id),
            "temp_password": temp_password  # Remove in production, send via email
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"User invite failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
