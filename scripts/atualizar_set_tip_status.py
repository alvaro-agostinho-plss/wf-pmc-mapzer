#!/usr/bin/env python3
"""
Adiciona colunas set_status e tip_status (ATIVO/INATIVO).
Uso: python3 scripts/atualizar_set_tip_status.py
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
    sql_path = Path(__file__).parent / "migrate_set_tip_status.sql"
    content = sql_path.read_text(encoding="utf-8")
    with engine.connect() as conn:
        for stmt in content.split(";"):
            stmt = stmt.strip()
            lines = [ln for ln in stmt.split("\n") if not ln.strip().startswith("--")]
            stmt = "\n".join(lines).strip()
            if stmt and any(stmt.upper().startswith(k) for k in ("ALTER", "UPDATE", "CREATE")):
                conn.execute(text(stmt))
        conn.commit()
    print("✓ Colunas set_status e tip_status adicionadas; view atualizada.")


if __name__ == "__main__":
    main()
