#!/usr/bin/env python3
"""
Inspeciona colunas de uma planilha Excel Mapzer.
Use: python scripts/inspector_colunas.py caminho/para/arquivo.xlsx
Mostra os nomes originais e normalizados para ajustar o mapeamento no ETL.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.etl import ler_excel, normalizar_nome_coluna


def main():
    if len(sys.argv) < 2:
        print("Uso: python scripts/inspector_colunas.py <arquivo.xlsx> [header_row]")
        sys.exit(1)
    path = Path(sys.argv[1])
    header_row = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    df = ler_excel(path, header_row=header_row)
    print(f"Linhas: {len(df)}, Colunas: {len(df.columns)}\n")
    print("Original -> Normalizado (oco_*)")
    print("-" * 60)
    for c in df.columns:
        norm = normalizar_nome_coluna(str(c))
        print(f"  {c!r} -> {norm}")


if __name__ == "__main__":
    main()
