#!/usr/bin/env python3
"""
Atualiza coluna set_email para VARCHAR(500) - suporta múltiplos e-mails.
Uso: python scripts/atualizar_set_email.py
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

    with engine.connect() as conn:
        conn.execute(text("DROP VIEW IF EXISTS vw_ocorrencias_status CASCADE"))
        conn.execute(text("ALTER TABLE setores ALTER COLUMN set_email TYPE VARCHAR(500)"))
        conn.commit()
    print("✓ Coluna set_email alterada para VARCHAR(500)")

    # Recria a view
    import importlib.util
    spec = importlib.util.spec_from_file_location("atualizar_view", Path(__file__).parent / "atualizar_view.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.main()

if __name__ == "__main__":
    main()
