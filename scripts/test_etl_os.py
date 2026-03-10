#!/usr/bin/env python3
"""Script para testar importação da planilha OS. Uso: python scripts/test_etl_os.py <caminho.xlsx>"""

import sys
from pathlib import Path

# Adiciona raiz ao path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

def main():
    if len(sys.argv) < 2:
        print("Uso: python scripts/test_etl_os.py docs/relatorio_os_27022026_07032026.xlsx")
        sys.exit(1)
    path = Path(sys.argv[1])
    if not path.exists():
        print(f"Arquivo não encontrado: {path}")
        sys.exit(1)
    try:
        from src.etl_os import ler_planilha_os, preparar_ordens_servico, persistir_ordens_servico
        print("1. Lendo planilha...")
        df = ler_planilha_os(path)
        print(f"   Colunas: {list(df.columns)}")
        print(f"   Linhas: {len(df)}")
        print(f"   Amostra:\n{df.head(3).to_string()}")
        print("\n2. Preparando dados...")
        df2 = preparar_ordens_servico(df)
        print(f"   Colunas finais: {list(df2.columns)}")
        print(f"   Linhas: {len(df2)}")
        print(f"   Amostra:\n{df2.head(3).to_string()}")
        print("\n3. Persistindo no banco...")
        n = persistir_ordens_servico(df2, truncar_antes=True)
        print(f"   OK: {n} registros inseridos em ordens_servico")
    except Exception as e:
        print(f"ERRO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
