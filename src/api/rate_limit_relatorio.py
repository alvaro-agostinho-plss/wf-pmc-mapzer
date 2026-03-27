"""Rate limit por IP para GET /rel-ocorrencia (token público)."""

import time
from collections import defaultdict, deque

from fastapi import HTTPException, Request

from src.config import AppConfig

_buckets: defaultdict[str, deque[float]] = defaultdict(deque)
_WINDOW_S = 60.0


def verificar_rate_limit_relatorio(request: Request) -> None:
    lim = AppConfig().relatorio_rate_limit_per_minute
    if lim <= 0:
        return
    ip = (request.client.host if request.client else None) or "unknown"
    now = time.monotonic()
    dq = _buckets[ip]
    while dq and now - dq[0] > _WINDOW_S:
        dq.popleft()
    if len(dq) >= lim:
        raise HTTPException(
            status_code=429,
            detail="Muitas requisições a este endereço. Aguarde um minuto e tente novamente.",
        )
    dq.append(now)
