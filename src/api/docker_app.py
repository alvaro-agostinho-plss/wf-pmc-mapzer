"""Wrapper ASGI para rodar o Mapzer em /wfpmcmapzer/ (produção Docker)."""
import os
from fastapi import FastAPI
from src.api.main import app as mapzer_app

base = (os.getenv("BASE_PATH") or "/wfpmcmapzer").strip().rstrip("/")
parent = FastAPI(title="Mapzer", docs_url=None, redoc_url=None)
parent.mount(base, mapzer_app)
app = parent
