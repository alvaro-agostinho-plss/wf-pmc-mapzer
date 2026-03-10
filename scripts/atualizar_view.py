#!/usr/bin/env python3
"""
Atualiza a view vw_ocorrencias_status no banco.
Uso: python scripts/atualizar_view.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, text
from src.config import DatabaseConfig

def main():
    sql_path = Path(__file__).parent / "view_ocorrencias_status.sql"
    content = sql_path.read_text(encoding="utf-8")
    # Executar cada statement (separados por ; no final de linha)
    # Extrair apenas CREATE VIEW (até ORDER BY inclusive, antes de COMMENT)
    idx = content.find("COMMENT ON VIEW")
    create_part = content[:idx].strip() if idx > 0 else content
    create_part = create_part.rstrip(";").strip()
    if not create_part.startswith("--"):
        engine = create_engine(DatabaseConfig().connection_url)
        with engine.connect() as conn:
            conn.execute(text(create_part))
            conn.commit()
    print("✓ View vw_ocorrencias_status atualizada.")

if __name__ == "__main__":
    main()
