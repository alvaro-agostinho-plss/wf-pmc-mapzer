#!/usr/bin/env python3
"""
Diagnóstico: verifica relação ocorrencias <-> ordens_servico para tip_id.
Uso: python scripts/diagnostico_tip_id_os.py [caminho_planilha_os.xlsx]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main():
    from src.etl_os import diagnosticar_tip_id, ler_planilha_os, preparar_ordens_servico, _resolver_tip_id_por_ocorrencia
    from src.etl import obter_engine

    print("=== Diagnóstico tip_id para Ordens de Serviço ===\n")

    diag = diagnosticar_tip_id()
    if "erro" in diag:
        print(f"Erro: {diag['erro']}")
        sys.exit(1)

    print(f"ocorrencias: total={diag['ocorrencias_total']}, com oco_ordemservico={diag['ocorrencias_com_oco_ordemservico']}, com tip_id={diag['ocorrencias_com_tip_id']}")
    print("\nAmostra ocorrencias (10 últimos):")
    for r in diag.get("ocorrencias_amostra", []):
        print(f"  {r}")
    print("\nAmostra ordens_servico:")
    for r in diag.get("ordens_servico_amostra", []):
        print(f"  {r}")

    if len(sys.argv) >= 2:
        path = Path(sys.argv[1])
        if path.exists():
            print(f"\nResolvendo tip_id para cada ose_numos da planilha {path.name}:")
            engine = obter_engine()
            df = ler_planilha_os(path)
            df2 = preparar_ordens_servico(df)
            for _, row in df2.iterrows():
                num = row.get("ose_numos")
                tip_id = _resolver_tip_id_por_ocorrencia(num, engine)
                print(f"  ose_numos={num} -> tip_id={tip_id}")

    print("\n=== Fim (veja também GET /api/debug-tip-id e logs/app.log) ===")


if __name__ == "__main__":
    main()
