#!/usr/bin/env python3
# Use: python3 run_server.py  (ou python se disponível)
"""Inicia o servidor da API (interface web + upload + processamento)."""

from pathlib import Path

from dotenv import load_dotenv
import uvicorn

load_dotenv(Path(__file__).resolve().parent / ".env")

if __name__ == "__main__":
    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
