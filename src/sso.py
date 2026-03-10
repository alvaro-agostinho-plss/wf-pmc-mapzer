"""Serviço de autenticação SSO (Keycloak) - equivalente ao SsoService em Node."""

import base64
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Carrega .env do diretório do projeto (independente do cwd)
load_dotenv(Path(__file__).resolve().parent.parent / ".env")
logger = logging.getLogger("mapzer")

# Mock: quando AUTH_MOCK=true, ignora SSO e aceita login direto
AUTH_MOCK = os.getenv("AUTH_MOCK", "").lower() in ("1", "true", "yes")


class SSOConfig(BaseSettings):
    """Configuração SSO via .env."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    token_url: str = Field(
        alias="SSO_TOKEN_URL",
        default="https://sso.castro.pr.gov.br/realms/master/protocol/openid-connect/token",
    )
    client_id: str = Field(alias="SSO_CLIENT_ID", default="")
    client_secret: str = Field(alias="SSO_CLIENT_SECRET", default="")


@dataclass
class SsoAuthResult:
    ok: bool
    error: str | None = None
    roles: list[str] | None = None
    access_token: str | None = None
    refresh_token: str | None = None
    expires_in: int | None = None
    refresh_expires_in: int | None = None
    token_type: str | None = None
    id_token: str | None = None
    sub: str | None = None
    username: str | None = None
    display_name: str | None = None
    email: str | None = None

    def to_dict(self) -> dict:
        if not self.ok:
            return {"ok": False, "error": self.error or "Erro de autenticação", "roles": self.roles or []}
        return {
            "ok": True,
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expires_in": self.expires_in,
            "refresh_expires_in": self.refresh_expires_in,
            "token_type": self.token_type,
            "id_token": self.id_token,
            "sub": self.sub,
            "username": self.username,
            "display_name": self.display_name,
            "email": self.email,
            "roles": self.roles or [],
        }


def _b64url_encode(data: dict) -> str:
    """Codifica dict em base64url para JWT."""
    s = json.dumps(data, separators=(",", ":"))
    return base64.urlsafe_b64encode(s.encode()).decode().rstrip("=")


def _mock_create_token(username: str, expires_in: int = 86400) -> str:
    """Cria JWT mock (payload compatível com decode_token)."""
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "sub": username,
        "preferred_username": username,
        "name": username,
        "exp": int(time.time()) + expires_in,
        "iat": int(time.time()),
    }
    return f"{_b64url_encode(header)}.{_b64url_encode(payload)}."


def _mock_authenticate(username: str, password: str) -> SsoAuthResult:
    """Autenticação mock: aceita qualquer usuário não vazio (senha ignorada)."""
    user = str(username or "").strip()
    if not user:
        return SsoAuthResult(ok=False, error="Usuário obrigatório.", roles=[])
    # Opcional: MOCK_USERS=user1:pass1,user2:pass2 para validar
    mock_users = os.getenv("MOCK_USERS", "")
    if mock_users:
        allowed = {}
        for pair in mock_users.split(","):
            p = pair.strip().split(":")
            if len(p) >= 2:
                allowed[p[0].strip()] = p[1].strip()
        if user not in allowed or allowed[user] != password:
            return SsoAuthResult(ok=False, error="Credenciais inválidas.", roles=[])

    access_token = _mock_create_token(user)
    refresh_token = base64.b64encode(json.dumps({"username": user}).encode()).decode()
    return SsoAuthResult(
        ok=True,
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=86400,
        refresh_expires_in=86400,
        token_type="Bearer",
        sub=user,
        username=user,
        display_name=user,
        roles=["mock_user"],
    )


def _mock_refresh(refresh_token: str) -> SsoAuthResult:
    """Renova token mock a partir do refresh_token mock."""
    if not refresh_token or not str(refresh_token).strip():
        return SsoAuthResult(ok=False, error="Refresh token ausente.", roles=[])
    try:
        data = json.loads(base64.b64decode(refresh_token).decode())
        user = data.get("username", "").strip()
        if not user:
            return SsoAuthResult(ok=False, error="Sessão expirada.", roles=[])
        access_token = _mock_create_token(user)
        return SsoAuthResult(
            ok=True,
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=86400,
            refresh_expires_in=86400,
            token_type="Bearer",
            sub=user,
            username=user,
            display_name=user,
            roles=["mock_user"],
        )
    except Exception:
        return SsoAuthResult(ok=False, error="Sessão expirada.", roles=[])


def _decode_jwt(token: str) -> dict[str, Any] | None:
    """Decodifica o payload do JWT sem validar assinatura (confiança no SSO)."""
    parts = token.split(".")
    if len(parts) < 2:
        return None
    payload = parts[1]
    if not payload:
        return None
    try:
        # Base64url -> base64
        normalized = payload.replace("-", "+").replace("_", "/")
        pad = 4 - len(normalized) % 4
        if pad != 4:
            normalized += "=" * pad
        decoded = base64.b64decode(normalized)
        return json.loads(decoded.decode("utf-8"))
    except Exception as e:
        logger.error("Erro ao decodificar JWT: %s", e)
        return None


def authenticate(username: str, password: str) -> SsoAuthResult:
    """
    Autentica no SSO (Keycloak) usando grant_type=password.
    Se AUTH_MOCK=true, ignora SSO e valida localmente.
    """
    if AUTH_MOCK:
        return _mock_authenticate(username, password)

    config = SSOConfig()
    if not config.client_id:
        raise ValueError("SSO_CLIENT_ID deve estar definido no .env")
    if not config.client_secret:
        raise ValueError("SSO_CLIENT_SECRET deve estar definido no .env")

    if not username or not str(username).strip() or not password:
        return SsoAuthResult(ok=False, error="Usuário e senha obrigatórios.", roles=[])

    body = {
        "client_id": config.client_id,
        "client_secret": config.client_secret,
        "grant_type": "password",
        "scope": "openid",
        "username": str(username).strip(),
        "password": password,
    }

    import urllib.request
    import urllib.error

    data = urllib.parse.urlencode(body).encode("utf-8")
    req = urllib.request.Request(
        config.token_url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            json_data = json.loads(raw)
    except urllib.error.HTTPError as e:
        body_text = ""
        try:
            body_text = e.read().decode("utf-8", errors="replace")[:2000]
        except Exception:
            pass
        logger.warning("SSO status %s body: %s", e.code, body_text)
        if e.code in (400, 401):
            return SsoAuthResult(ok=False, error="Credenciais inválidas.", roles=[])
        return SsoAuthResult(ok=False, error="Erro ao autenticar no serviço de autenticação.", roles=[])
    except urllib.error.URLError as e:
        logger.error("Erro ao conectar ao SSO: %s", e)
        return SsoAuthResult(ok=False, error="Não foi possível conectar ao serviço de autenticação.", roles=[])
    except Exception as e:
        logger.exception("Erro SSO: %s", e)
        return SsoAuthResult(ok=False, error="Erro ao autenticar no serviço de autenticação.", roles=[])

    access_token = json_data.get("access_token")
    if not access_token:
        logger.error("SSO retornou resposta sem access_token")
        return SsoAuthResult(ok=False, error="Resposta inválida do serviço de autenticação.", roles=[])

    profile = _decode_jwt(access_token)
    roles = []
    if profile:
        realm_roles = profile.get("realm_access") or {}
        roles = [r for r in (realm_roles.get("roles") or []) if isinstance(r, str)]

    return SsoAuthResult(
        ok=True,
        access_token=access_token,
        refresh_token=json_data.get("refresh_token"),
        expires_in=json_data.get("expires_in"),
        refresh_expires_in=json_data.get("refresh_expires_in"),
        token_type=json_data.get("token_type"),
        id_token=json_data.get("id_token"),
        sub=profile.get("sub") if profile else None,
        username=(profile or {}).get("preferred_username") or str(username).strip(),
        display_name=(profile or {}).get("name"),
        email=(profile or {}).get("email"),
        roles=roles,
    )


def refresh_access_token(refresh_token: str) -> SsoAuthResult:
    """
    Renova access_token usando refresh_token.
    Se AUTH_MOCK=true, usa refresh mock.
    """
    if AUTH_MOCK:
        return _mock_refresh(refresh_token)

    config = SSOConfig()
    if not config.client_id or not config.client_secret:
        return SsoAuthResult(ok=False, error="Configuração SSO incompleta.", roles=[])
    if not refresh_token or not str(refresh_token).strip():
        return SsoAuthResult(ok=False, error="Refresh token ausente.", roles=[])

    body = {
        "client_id": config.client_id,
        "client_secret": config.client_secret,
        "grant_type": "refresh_token",
        "refresh_token": str(refresh_token).strip(),
    }

    import urllib.request
    import urllib.error

    data = urllib.parse.urlencode(body).encode("utf-8")
    req = urllib.request.Request(
        config.token_url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            json_data = json.loads(raw)
    except urllib.error.HTTPError as e:
        body_text = ""
        try:
            body_text = e.read().decode("utf-8", errors="replace")[:500]
        except Exception:
            pass
        logger.warning("SSO refresh status %s: %s", e.code, body_text)
        return SsoAuthResult(ok=False, error="Sessão expirada.", roles=[])
    except Exception as e:
        logger.exception("Erro SSO refresh: %s", e)
        return SsoAuthResult(ok=False, error="Erro ao renovar sessão.", roles=[])

    access_token = json_data.get("access_token")
    if not access_token:
        return SsoAuthResult(ok=False, error="Sessão expirada.", roles=[])

    profile = _decode_jwt(access_token)
    roles = []
    if profile:
        realm_roles = profile.get("realm_access") or {}
        roles = [r for r in (realm_roles.get("roles") or []) if isinstance(r, str)]

    return SsoAuthResult(
        ok=True,
        access_token=access_token,
        refresh_token=json_data.get("refresh_token") or refresh_token,
        expires_in=json_data.get("expires_in"),
        refresh_expires_in=json_data.get("refresh_expires_in"),
        token_type=json_data.get("token_type"),
        id_token=json_data.get("id_token"),
        sub=profile.get("sub") if profile else None,
        username=(profile or {}).get("preferred_username"),
        display_name=(profile or {}).get("name"),
        email=(profile or {}).get("email"),
        roles=roles,
    )


def decode_token(token: str) -> dict[str, Any] | None:
    """Decodifica JWT e retorna payload. Retorna None se inválido ou expirado."""
    payload = _decode_jwt(token)
    if not payload:
        return None
    exp = payload.get("exp")
    if exp and isinstance(exp, (int, float)):
        import time
        if time.time() > exp:
            return None  # Token expirado
    return payload
