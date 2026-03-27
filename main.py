#!/usr/bin/env python3
"""
Script principal: ETL + Relatórios.
Uso:
  python main.py etl <caminho_excel.xlsx> [--truncar]
  python main.py relatorios
  python main.py all <caminho_excel.xlsx> [--truncar]
"""

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv

from src.config import DatabaseConfig, SMTPConfig
from src.etl import executar_etl
from src.reports import executar_relatorios


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="ETL e Automação de Relatórios")
    sub = parser.add_subparsers(dest="comando", required=True)
    # etl
    p_etl = sub.add_parser("etl", help="Executar ETL (ler Excel e persistir)")
    p_etl.add_argument("arquivo", type=Path, help="Caminho do arquivo .xlsx")
    p_etl.add_argument("--truncar", action="store_true", help="Truncar tabela antes de inserir")
    p_etl.add_argument("--usuario", default="sistema", help="Usuário para auditoria")
    p_etl.add_argument("--header-row", type=int, default=None, help="Linha dos cabeçalhos (0-index). Mapzer: 5")
    # relatorios (envio apenas via botão na interface; CLI só gera HTML)
    p_rep = sub.add_parser("relatorios", help="Gerar HTML do relatório (envio via interface web)")
    p_rep.add_argument("--enviar", action="store_true", help="Forçar envio por e-mail (padrão: só gera HTML)")
    # all
    p_all = sub.add_parser("all", help="ETL + Relatórios em sequência")
    p_all.add_argument("arquivo", type=Path, help="Caminho do arquivo .xlsx")
    p_all.add_argument("--truncar", action="store_true")
    p_all.add_argument("--usuario", default="sistema")
    p_all.add_argument("--header-row", type=int, default=None, help="Mapzer: 5")

    args = parser.parse_args()

    try:
        if args.comando == "etl":
            n = executar_etl(
                args.arquivo,
                usuario=args.usuario,
                truncar_antes=args.truncar,
                header_row=args.header_row,
            )
            print(f"ETL concluído: {n} ocorrências inseridas.")
        elif args.comando == "relatorios":
            if getattr(args, "enviar", False):
                r = executar_relatorios()
                for setor, ok in r.items():
                    if not isinstance(ok, bool):
                        continue
                    status = "✓" if ok else "✗"
                    print(f"  {status} {setor}")
            else:
                from src.reports import obter_dados_view, agregar_dados_para_relatorio, gerar_html_email_geral
                df = obter_dados_view()
                dados = agregar_dados_para_relatorio(df)
                html = gerar_html_email_geral(dados)
                out = "/tmp/relatorio_geral_mapzer.html"
                Path(out).write_text(html, encoding="utf-8")
                print(f"HTML salvo em {out} (envio apenas via botão na interface)")
                print(f"  Período: {dados.get('periodo', '-')} | Setores: {len(dados.get('setores', []))}")
        elif args.comando == "all":
            n = executar_etl(
                args.arquivo,
                usuario=args.usuario,
                truncar_antes=args.truncar,
                header_row=getattr(args, "header_row", None),
            )
            print(f"ETL: {n} ocorrências.")
            print("  Envio de e-mail: use o botão na interface web.")
        return 0
    except FileNotFoundError as e:
        print(f"Erro: {e}", file=sys.stderr)
        return 1
    except ConnectionError as e:
        print(f"Erro de conexão: {e}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"Erro de configuração: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
