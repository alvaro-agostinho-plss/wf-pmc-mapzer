#!/usr/bin/env python3
"""
Diagnóstico: ocorrências na view vs excluídas (tip_id NULL, sem setor).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, text
from src.config import DatabaseConfig

def main():
    cfg = DatabaseConfig()
    eng = create_engine(cfg.connection_url)
    with eng.connect() as conn:
        # Total ocorrências
        r = conn.execute(text("SELECT COUNT(*) FROM ocorrencias"))
        total = r.scalar() or 0

        # Com/sem tip_id
        r = conn.execute(text("SELECT COUNT(*) FROM ocorrencias WHERE tip_id IS NULL"))
        sem_tip_id = r.scalar() or 0
        com_tip_id = total - sem_tip_id

        # Com/sem ordem de serviço
        r = conn.execute(text("""
            SELECT COUNT(*) FROM ocorrencias
            WHERE oco_ordemservico IS NULL OR TRIM(COALESCE(oco_ordemservico::TEXT,'')) = ''
        """))
        sem_os = r.scalar() or 0
        com_os = total - sem_os

        # Sem OS E sem tip_id (excluídas da view)
        r = conn.execute(text("""
            SELECT COUNT(*) FROM ocorrencias
            WHERE (oco_ordemservico IS NULL OR TRIM(COALESCE(oco_ordemservico::TEXT,'')) = '')
              AND tip_id IS NULL
        """))
        sem_os_sem_tip = r.scalar() or 0

        # Na view (linhas podem duplicar se tipo em múltiplos setores)
        r = conn.execute(text("SELECT COUNT(DISTINCT oco_id) FROM vw_ocorrencias_status"))
        oco_ids_na_view = r.scalar() or 0

        # Tipos que ocorrências sem tip_id têm em oco_tipo
        r = conn.execute(text("""
            SELECT oco_tipo, COUNT(*) as qtd
            FROM ocorrencias WHERE tip_id IS NULL
            GROUP BY oco_tipo ORDER BY qtd DESC LIMIT 20
        """))
        tipos_sem_match = r.fetchall()

    print("=== DIAGNÓSTICO OCORRÊNCIAS ===\n")
    print(f"Total ocorrências: {total}")
    print(f"  Com tip_id: {com_tip_id} | Sem tip_id: {sem_tip_id}")
    print(f"  Com OS: {com_os} | Sem OS (em aberto): {sem_os}")
    print(f"  Sem OS E sem tip_id (excluídas): {sem_os_sem_tip}")
    print(f"\nOcorrências distintas na view: {oco_ids_na_view}")
    print(f"Excluídas (tip_id NULL ou tipo sem setor): {total - oco_ids_na_view}")
    if tipos_sem_match:
        print(f"\nTipos em oco_tipo (ocorrências sem tip_id) - top 20:")
        for row in tipos_sem_match:
            print(f"  {row[1]:>6} | {repr(row[0])}")

if __name__ == "__main__":
    main()
