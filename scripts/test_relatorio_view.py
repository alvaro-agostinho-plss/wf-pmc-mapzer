#!/usr/bin/env python3
"""
Testa a query da view e a agregação para relatório.
Simula exatamente o fluxo de obter_dados_view + agregar_dados_para_relatorio.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
from sqlalchemy import create_engine, text
from src.config import DatabaseConfig
from src.reports import obter_dados_view, agregar_dados_para_relatorio

def main():
    cfg = DatabaseConfig()
    print("=== TESTE RELATÓRIO: VIEW + AGREGAÇÃO ===\n")

    # 1. Query exata que obter_dados_view usa
    sql = """
        SELECT set_id, set_nome, set_email, set_whatsapp, oco_bairro, tip_nome, status,
               oco_datahora
        FROM vw_ocorrencias_status
        ORDER BY set_id, oco_bairro, tip_nome
    """
    engine = create_engine(cfg.connection_url)
    print(f"Executando: {sql[:80]}...")
    df = pd.read_sql(text(sql), engine)
    print(f"\n1. View retornou: {len(df)} linhas")
    if df.empty:
        print("   Df vazio - nada para agregar.")
        return

    print(f"   Colunas: {list(df.columns)}")
    if "status" in df.columns:
        status_uniq = df["status"].unique().tolist()
        print(f"   Valores únicos de status: {status_uniq}")
        for s in status_uniq:
            n = (df["status"] == s).sum()
            print(f"     - {repr(s)}: {n} linhas")
    if "set_id" in df.columns:
        print(f"   Setores (set_id): {df['set_id'].nunique()} distintos")

    # 2. Debug: simular agregação passo a passo
    df2 = df.copy()
    for col in ["oco_bairro", "tip_nome"]:
        if col in df2.columns:
            df2[col] = df2[col].fillna("Não identificado")
    if "status" in df2.columns:
        df2["status"] = df2["status"].astype(str).str.strip().str.upper()
        df2["status"] = df2["status"].replace({"ABERTO": "EM_ABERTO", "NAN": "EM_ABERTO", "NONE": "EM_ABERTO", "": "EM_ABERTO"})
        mask = ~df2["status"].isin(["EM_ABERTO", "EM_TRATAMENTO", "SOLUCIONADO"])
        if mask.any():
            df2.loc[mask, "status"] = "EM_ABERTO"
    df2["_count"] = 1
    cols_aggr = ["set_id", "set_nome", "set_email", "oco_bairro", "tip_nome", "status"]
    if "set_whatsapp" in df2.columns:
        cols_aggr.insert(3, "set_whatsapp")
    agg = df2.groupby(cols_aggr, dropna=False).agg({"_count": "sum"}).reset_index()
    idx_cols = ["set_id", "set_nome", "set_email", "oco_bairro", "tip_nome"]
    if "set_whatsapp" in agg.columns:
        idx_cols.insert(3, "set_whatsapp")
    pivot = agg.pivot_table(index=idx_cols, columns="status", values="_count", aggfunc="sum", fill_value=0).reset_index()
    print(f"\n   [DEBUG] agg shape: {agg.shape}, pivot shape: {pivot.shape}")
    print(f"   [DEBUG] pivot colunas: {list(pivot.columns)}")
    if len(pivot) > 0:
        first = pivot.iloc[0]
        print(f"   [DEBUG] primeira linha pivot: set_id={first.get('set_id')}, EM_ABERTO={first.get('EM_ABERTO', 'N/A')}, tip_nome={first.get('tip_nome')}")

    # 3. Agregação completa (igual reports.py)
    dados = agregar_dados_para_relatorio(df)
    print(f"\n2. Após agregar:")
    print(f"   Periodo: {dados.get('periodo')}")
    print(f"   Total aberto: {dados.get('total_aberto')}")
    print(f"   Total tratamento: {dados.get('total_tratamento')}")
    print(f"   Total solucionado: {dados.get('total_solucionado')}")
    print(f"   Setores no relatório: {len(dados.get('setores', []))}")
    if dados.get("setores"):
        for i, s in enumerate(dados["setores"][:3]):
            print(f"     Setor {i+1}: {s['nome']} - aberto={s['total_aberto']}, trat={s['total_tratamento']}, sol={s['total_solucionado']}")
    else:
        print("   NENHUM SETOR na lista - investigar pivot/agregação")

if __name__ == "__main__":
    main()
