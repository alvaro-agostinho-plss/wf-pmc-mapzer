"""
Acesso público ao relatório por link com token.

Regra: GET /rel-ocorrencia?token=<hex> → consulta `envios_email.env_token`,
       confere expiração (`env_expires_at`), retorna `env_resultado` (HTML).
       Sem JWT, sem cookie — só o token gravado no envio do e-mail/WhatsApp.
"""

from __future__ import annotations

import re

from src.api.upload import obter_html_relatorio_por_token

# secrets.token_hex(16) → 32 chars; margem para evolução
_TOKEN_HEX = re.compile(r"^[0-9a-f]{32,128}$")


def normalizar_token(token: str | None) -> str | None:
    """Strip + lowercase; None se vazio ou formato inválido (só hex)."""
    if not token or not str(token).strip():
        return None
    t = str(token).strip().lower()
    if not _TOKEN_HEX.match(t):
        return None
    return t


def buscar_html_relatorio_token(token_bruto: str) -> str | None:
    """
    Valida formato do token e busca HTML em envios_email.
    Retorna None se formato inválido, token inexistente, expirado ou HTML vazio.
    """
    t = normalizar_token(token_bruto)
    if not t:
        return None
    return obter_html_relatorio_por_token(t)
