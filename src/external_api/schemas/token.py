from pydantic import UUID4, BaseModel, Field


class Token(BaseModel):
    """Schéma pour le token d'accès"""

    access_token: str = Field(..., description="Token JWT d'accès")
    token_type: str = Field(..., description="Type de token (Bearer)")


class TokenPayload(BaseModel):
    """Schéma pour le payload du token JWT"""

    sub: str | None = Field(
        None, description="ID de l'utilisateur (subject) en format UUID"
    )
    exp: int | None = Field(None, description="Timestamp d'expiration")


class TokenData(BaseModel):
    """Schéma pour les données extraites du token"""

    user_id: UUID4 = Field(..., description="ID de l'utilisateur")

