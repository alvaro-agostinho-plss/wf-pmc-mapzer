"""Configuração de logging para erros em arquivo."""

import logging
import sys
from pathlib import Path

LOGS_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_ERROS = LOGS_DIR / "erros.log"
LOG_APP = LOGS_DIR / "app.log"

_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def configurar_logging(
    nivel: int = logging.INFO,
    log_geral: bool = True,
) -> None:
    """
    Configura logging: erros em logs/erros.log, opcional app em logs/app.log.
    Erros incluem traceback completo.
    """
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    if root.handlers:
        return  # já configurado

    root.setLevel(nivel)
    formatter = logging.Formatter(_FORMAT)

    # Arquivo de erros (WARNING+ inclui 404; logger.exception() inclui traceback)
    fh_erros = logging.FileHandler(LOG_ERROS, encoding="utf-8")
    fh_erros.setLevel(logging.WARNING)
    fh_erros.setFormatter(logging.Formatter(_FORMAT))
    root.addHandler(fh_erros)

    # Arquivo geral (INFO+) - opcional
    if log_geral:
        fh_app = logging.FileHandler(LOG_APP, encoding="utf-8")
        fh_app.setLevel(logging.INFO)
        fh_app.setFormatter(formatter)
        root.addHandler(fh_app)

    # Console (INFO)
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    root.addHandler(ch)


