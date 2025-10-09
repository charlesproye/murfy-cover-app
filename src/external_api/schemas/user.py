"""Schémas Pydantic pour les utilisateurs"""

from datetime import datetime
from pydantic import UUID4, BaseModel, EmailStr


# Base Models
class UserBase(BaseModel):
    """Schéma de base pour les utilisateurs"""

    email: EmailStr
    first_name: str
    last_name: str | None = None
    phone: str | None = None


class CompanyBase(BaseModel):
    """Schéma de base pour les entreprises"""

    name: str
    description: str | None = None


class RoleBase(BaseModel):
    """Schéma de base pour les rôles"""

    role_name: str


class ApiUserBase(BaseModel):
    """Schéma de base pour les utilisateurs API"""

    user_id: UUID4
    api_key: str
    is_active: bool = True


# Authentication Models
class UserLogin(BaseModel):
    email: EmailStr
    password: str


# User Models
class User(UserBase):
    """Schéma complet pour les utilisateurs"""

    id: UUID4
    last_connection: datetime | None = None
    role_id: UUID4 | None = None

    class Config:
        from_attributes = True


class UserCreate(UserBase):
    """Schéma pour la création d'utilisateur"""

    password: str
    role_id: UUID4 | None = None


class UserUpdate(BaseModel):
    """Schéma pour la mise à jour d'utilisateur"""

    email: EmailStr | None = None
    first_name: str | None = None
    last_name: str | None = None
    password: str | None = None
    phone: str | None = None
    role_id: UUID4 | None = None


class FleetInfo(BaseModel):
    """Schéma pour les informations de flotte"""

    id: UUID4
    name: str


class GlobalUser(BaseModel):
    """Schéma pour les utilisateurs globaux"""

    company_id: UUID4
    fleet_id: UUID4 | None = None
    fleet_ids: list[FleetInfo] | None = None
    first_name: str
    last_name: str
    last_connection: datetime | None = None
    email: str
    phone: str
    role_id: UUID4
    id: UUID4
    updated_at: datetime
    created_at: datetime

    class Config:
        from_attributes = True


class UserWithFleet(BaseModel):
    """Schéma pour les utilisateurs avec flotte"""

    company_id: UUID4
    id: UUID4
    role_id: UUID4
    email: str
    fleet_id: UUID4 | None
    fleet_ids: list[FleetInfo] | None


# Company Models
class Company(CompanyBase):
    """Schéma complet pour les entreprises"""

    id: UUID4

    class Config:
        from_attributes = True


# Role Models
class Role(RoleBase):
    """Schéma complet pour les rôles"""

    id: UUID4

    class Config:
        from_attributes = True


# API User Models
class ApiUser(ApiUserBase):
    """Schéma complet pour les utilisateurs API"""

    id: UUID4
    created_at: datetime
    last_access: datetime | None = None

    class Config:
        from_attributes = True


class LoginResponse(BaseModel):
    """Schéma pour la réponse d'authentification"""

    access_token: str
    refresh_token: str
    token_type: str
    user: "GlobalUser"
    company: "Company"

