#!/usr/bin/env python3
"""
Remove dados importados (ocorrências, ordens de serviço, lotes, uploads).
Preserva setores, tipos e setores_tipos.
Uso: python scripts/limpar_dados_importados.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, text
from src.config import DatabaseConfig

def main():
    config = DatabaseConfig()
    engine = create_engine(config.connection_url)
    sql = """
        TRUNCATE TABLE lotes CASCADE;
        TRUNCATE TABLE ocorrencias RESTART IDENTITY CASCADE;
        TRUNCATE TABLE ordens_servico RESTART IDENTITY CASCADE;
        TRUNCATE TABLE uploads_planilha RESTART IDENTITY CASCADE;
    """
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    print("✓ Dados importados removidos (ocorrências, OS, lotes, uploads)")
    print("  Setores e tipos preservados.")

if __name__ == "__main__":
    main()
