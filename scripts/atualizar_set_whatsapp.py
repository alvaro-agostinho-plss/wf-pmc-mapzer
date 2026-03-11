#!/usr/bin/env python3
"""
Adiciona coluna set_whatsapp na tabela setores.
Números WhatsApp separados por ; (ex: 5542999999999;5542988888888)
Uso: python scripts/atualizar_set_whatsapp.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, text
from src.config import DatabaseConfig

def main():
    engine = create_engine(DatabaseConfig().connection_url)
    sql_path = Path(__file__).parent / "migrate_set_whatsapp.sql"
    content = sql_path.read_text(encoding="utf-8")
    with engine.connect() as conn:
        for stmt in content.split(";"):
            stmt = stmt.strip()
            # Remove linhas de comentário no início
            lines = [ln for ln in stmt.split("\n") if not ln.strip().startswith("--")]
            stmt = "\n".join(lines).strip()
            if stmt and any(stmt.upper().startswith(k) for k in ("ALTER", "COMMENT", "CREATE")):
                conn.execute(text(stmt))
        conn.commit()
    print("✓ Coluna set_whatsapp adicionada e view vw_ocorrencias_status atualizada.")

if __name__ == "__main__":
    main()
