"""Dependências de autenticação e validação de token JWT."""

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.sso import decode_token

_security = HTTPBearer(auto_error=False)


def obter_usuario(
    credentials: HTTPAuthorizationCredentials | None = Depends(_security),
) -> dict:
    """
    Extrai e valida o token JWT do header Authorization.
    Retorna o payload (sub, username, etc.) ou levanta 401.
    """
    if not credentials or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token de autenticação ausente ou inválido.",
        )
    payload = decode_token(credentials.credentials)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido ou expirado.",
        )
    return {
        "sub": payload.get("sub"),
        "username": payload.get("preferred_username"),
        "name": payload.get("name"),
        "email": payload.get("email"),
        "roles": [
            r for r in (payload.get("realm_access") or {}).get("roles") or []
            if isinstance(r, str)
        ],
    }
