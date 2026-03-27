"""Módulo de relatórios: agrupamento Setor->Bairro->Tipo, envio de e-mails."""

import html as html_module
import logging
import re
import secrets
from datetime import datetime
from pathlib import Path

import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from jinja2 import Environment, FileSystemLoader, select_autoescape
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.config import (
    AppConfig,
    DatabaseConfig,
    SMTPConfig,
    WhatsAppConfig,
    carregar_mapeamento,
    montar_url_relatorio_publico,
    normalizar_tipo,
)
from src.whatsapp_notify import (
    _parse_chat_ids,
    _whatsapp_habilitado,
    enviar_whatsapp_texto,
    montar_texto_whatsapp_geral,
    montar_texto_whatsapp_secretaria,
)

ROOT = Path(__file__).resolve().parent.parent
logger = logging.getLogger("mapzer")
TEMPLATE_DIR = ROOT / "config"
DEFAULT_TEMPLATE = "template_email.html"
TEMPLATE_EMAIL_GERAL = "email_geral.html"
TEMPLATE_EMAIL_SECRETARIA = "email_secretaria.html"


def _norm_bairro(s) -> str:
    """Normaliza bairro para comparação: acentos, lowercase, colapsa espaços."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = str(s).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return ""
    return " ".join(normalizar_tipo(s).split())


def _col(df: pd.DataFrame, *candidates: str) -> str | None:
    """Retorna primeira coluna que existe no DataFrame."""
    for c in candidates:
        if c in df.columns:
            return c
    for c in df.columns:
        for cand in candidates:
            if cand.replace("oco_", "").replace("_", " ") in c.lower():
                return c
    return None


def obter_ocorrencias(config: DatabaseConfig | None = None) -> pd.DataFrame:
    """Carrega todas as ocorrências do banco."""
    config = config or DatabaseConfig()
    engine = create_engine(config.connection_url)
    try:
        return pd.read_sql_table("ocorrencias", engine)
    except SQLAlchemyError as e:
        raise ConnectionError(f"Erro ao ler ocorrências do banco: {e}") from e


def obter_dados_view(
    dt_inicio: datetime | str | None = None,
    dt_fim: datetime | str | None = None,
    config: DatabaseConfig | None = None,
) -> pd.DataFrame:
    """
    Carrega dados da view vw_ocorrencias_status.
    Se dt_inicio/dt_fim não informados, usa todo o intervalo disponível.
    """
    config = config or DatabaseConfig()
    engine = create_engine(config.connection_url)
    sql = """
        SELECT set_id, set_nome, set_email, set_whatsapp, oco_bairro, tip_nome, status,
               oco_datahora
        FROM vw_ocorrencias_status
    """
    params = {}
    conditions = []
    if dt_inicio:
        conditions.append("oco_datahora >= :dt_inicio")
        params["dt_inicio"] = pd.to_datetime(dt_inicio)
    if dt_fim:
        conditions.append("oco_datahora <= :dt_fim")
        params["dt_fim"] = pd.to_datetime(dt_fim)
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += " ORDER BY set_id, oco_bairro, tip_nome"
    try:
        return pd.read_sql(text(sql), engine, params=params or None)
    except SQLAlchemyError as e:
        raise ConnectionError(f"Erro ao ler view: {e}") from e


def _get_status_count(row: pd.Series, key: str) -> int:
    """Obtém contagem de status do pivot, tentando variações de nome de coluna."""
    for name in [key, key.lower(), key.replace("_", " ").title()]:
        if name in row.index:
            v = row.get(name, 0)
            try:
                return int(v) if pd.notna(v) else 0
            except (TypeError, ValueError):
                return 0
    return 0


def agregar_dados_para_relatorio(
    df: pd.DataFrame,
    periodo_override: str | None = None,
) -> dict:
    """
    Agrega dados da view para estrutura dos templates.
    periodo_override: quando informado (ex: datas do modal), usa em vez de calcular do df.
    Retorna: {
        periodo, municipio,
        total_aberto, total_tratamento, total_solucionado,
        setores: [...], setores_por_id: {...},
        tipos_percentual: [{tipo, quantidade, percentual}, ...]
    }
    """
    if df.empty:
        return {
            "periodo": periodo_override or "",
            "municipio": AppConfig().municipio,
            "total_aberto": 0,
            "total_tratamento": 0,
            "total_solucionado": 0,
            "setores": [],
            "setores_por_id": {},
            "tipos_percentual": [],
        }

    if periodo_override:
        periodo = periodo_override
    else:
        col_data = "oco_datahora"
        if col_data in df.columns:
            vals = pd.to_datetime(df[col_data], errors="coerce").dropna()
            if not vals.empty:
                d_min, d_max = vals.min(), vals.max()
                periodo = f"{d_min.strftime('%d/%m/%Y')} a {d_max.strftime('%d/%m/%Y')}"
            else:
                periodo = ""
        else:
            periodo = ""

    # Agregação: set_id, set_nome, set_email, set_whatsapp, oco_bairro, tip_nome, status -> count
    df = df.copy()
    if AppConfig().omitir_sem_localizacao and "oco_bairro" in df.columns:
        _vazio = AppConfig().valores_sem_localizacao

        def _norm_bairro(s):
            """Normaliza bairro: acentos, lowercase, colapsa espaços."""
            if not s or not str(s).strip():
                return ""
            return " ".join(normalizar_tipo(str(s).strip()).split())

        _vazio_norm = frozenset(_norm_bairro(v) for v in _vazio)

        def _eh_sem_bairro(val):
            if pd.isna(val):
                return True
            s = str(val).strip()
            if not s or s.lower() in ("nan", "none", "null"):
                return True
            return _norm_bairro(s) in _vazio_norm

        mask_sem_bairro = df["oco_bairro"].apply(_eh_sem_bairro)
        df = df[~mask_sem_bairro].copy()
    for col in ["oco_bairro", "tip_nome"]:
        if col in df.columns:
            df[col] = df[col].fillna("Não identificado")
    # Normalizar status: garantir EM_ABERTO, EM_TRATAMENTO, SOLUCIONADO (view pode retornar variações)
    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.strip().str.upper()
        df["status"] = df["status"].replace({
            "ABERTO": "EM_ABERTO",
            "NAN": "EM_ABERTO",
            "NONE": "EM_ABERTO",
            "": "EM_ABERTO",
        })
        # Valores inválidos -> EM_ABERTO (fallback)
        mask_invalido = ~df["status"].isin(["EM_ABERTO", "EM_TRATAMENTO", "SOLUCIONADO"])
        if mask_invalido.any():
            logger.debug("Status inválidos normalizados para EM_ABERTO: %s", df.loc[mask_invalido, "status"].unique().tolist())
            df.loc[mask_invalido, "status"] = "EM_ABERTO"
    df["_count"] = 1
    # Não incluir set_whatsapp no groupby/pivot - NaN quebra pivot_table. Recupera depois.
    cols_aggr = ["set_id", "set_nome", "set_email", "oco_bairro", "tip_nome", "status"]
    agg = (
        df.groupby(cols_aggr, dropna=False)
        .agg({"_count": "sum"})
        .reset_index()
    )
    idx_cols = ["set_id", "set_nome", "set_email", "oco_bairro", "tip_nome"]
    pivot = agg.pivot_table(
        index=idx_cols,
        columns="status",
        values="_count",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()
    for c in ["EM_ABERTO", "EM_TRATAMENTO", "SOLUCIONADO"]:
        if c not in pivot.columns:
            pivot[c] = 0

    # set_whatsapp não está no pivot (removido por causa do NaN). Busca do df original.
    set_whatsapp_map = df.groupby("set_id")["set_whatsapp"].first().to_dict() if "set_whatsapp" in df.columns else {}

    setores_list = []
    setores_por_id = {}
    ordem = 1
    for set_id, grp in pivot.groupby("set_id"):
        set_nome = grp["set_nome"].iloc[0]
        set_email = str(grp["set_email"].iloc[0] or "").strip()
        set_whatsapp = str(set_whatsapp_map.get(set_id, "") or "").strip()
        blocos_bairro = []
        set_total_aberto = 0
        set_total_tratamento = 0
        set_total_solucionado = 0

        _vazio_norm = frozenset(_norm_bairro(v) for v in AppConfig().valores_sem_localizacao)
        for bairro, grp_b in grp.groupby("oco_bairro"):
            bairro_str = str(bairro).strip() if pd.notna(bairro) and str(bairro) != "nan" else "Não identificado"
            if AppConfig().omitir_sem_localizacao and (not bairro_str or _norm_bairro(bairro_str) in _vazio_norm):
                continue
            linhas = []
            for _, row in grp_b.iterrows():
                em_ab = _get_status_count(row, "EM_ABERTO")
                em_tr = _get_status_count(row, "EM_TRATAMENTO")
                sol = _get_status_count(row, "SOLUCIONADO")
                # Omitir linhas com todas as colunas zero
                if em_ab + em_tr + sol == 0:
                    continue
                linhas.append({
                    "tipo": str(row["tip_nome"]),
                    "em_aberto": em_ab,
                    "em_tratamento": em_tr,
                    "solucionado": sol,
                })
                set_total_aberto += em_ab
                set_total_tratamento += em_tr
                set_total_solucionado += sol
            # Omitir bairros sem linhas (após filtrar zeros)
            if not linhas:
                continue
            blocos_bairro.append({"bairro": bairro_str, "linhas": linhas})

        # Omitir setores sem blocos (vilas) ou com totais zero
        if not blocos_bairro or (set_total_aberto + set_total_tratamento + set_total_solucionado) == 0:
            continue

        setor_item = {
            "ordem": ordem,
            "nome": set_nome,
            "email": set_email,
            "whatsapp": set_whatsapp,
            "blocos_bairro": blocos_bairro,
            "total_aberto": set_total_aberto,
            "total_tratamento": set_total_tratamento,
            "total_solucionado": set_total_solucionado,
        }
        setores_list.append(setor_item)
        setores_por_id[set_id] = {
            "setor_nome": set_nome,
            "email": set_email,
            "whatsapp": set_whatsapp,
            "periodo": periodo,
            "total_aberto": set_total_aberto,
            "total_tratamento": set_total_tratamento,
            "total_solucionado": set_total_solucionado,
            "blocos_bairro": blocos_bairro,
        }
        ordem += 1

    # Totais globais = soma do que foi listado (após filtrar zeros/vazios)
    total_aberto = sum(s["total_aberto"] for s in setores_list)
    total_tratamento = sum(s["total_tratamento"] for s in setores_list)
    total_solucionado = sum(s["total_solucionado"] for s in setores_list)

    # Participação % por tipo de ocorrência (cada linha da view = 1 ocorrência, mesmo recorte do relatório)
    tipos_percentual: list[dict] = []
    if "tip_nome" in df.columns and len(df) > 0:
        s_tip = (
            df["tip_nome"]
            .fillna("Não identificado")
            .astype(str)
            .str.strip()
            .replace({"nan": "Não identificado", "none": "Não identificado", "": "Não identificado"})
        )
        vc = s_tip.value_counts(dropna=False)
        tot_tip = int(vc.sum())
        for tipo, q in vc.items():
            q = int(q)
            pct = round(100.0 * q / tot_tip, 1) if tot_tip else 0.0
            tipos_percentual.append({"tipo": str(tipo), "quantidade": q, "percentual": pct})

    return {
        "periodo": periodo,
        "municipio": AppConfig().municipio,
        "total_aberto": total_aberto,
        "total_tratamento": total_tratamento,
        "total_solucionado": total_solucionado,
        "setores": setores_list,
        "setores_por_id": setores_por_id,
        "tipos_percentual": tipos_percentual,
    }


def gerar_html_email_geral(dados: dict, template_path: Path | str | None = None) -> str:
    """Gera HTML do e-mail geral (todos os setores) usando email_geral.html."""
    path = Path(template_path) if template_path else TEMPLATE_DIR / TEMPLATE_EMAIL_GERAL
    if not path.exists():
        return "<html><body>Template email_geral.html não encontrado.</body></html>"
    env = Environment(
        loader=FileSystemLoader(str(path.parent)),
        autoescape=select_autoescape(["html"]),
    )
    template = env.get_template(path.name)
    return template.render(
        periodo=dados.get("periodo", ""),
        municipio=dados.get("municipio", ""),
        total_aberto=dados.get("total_aberto", 0),
        total_tratamento=dados.get("total_tratamento", 0),
        total_solucionado=dados.get("total_solucionado", 0),
        setores=dados.get("setores", []),
        tipos_percentual=dados.get("tipos_percentual") or [],
    )


def gerar_html_email_secretaria(dados_setor: dict, template_path: Path | str | None = None) -> str:
    """Gera HTML do e-mail por secretaria usando email_secretaria.html."""
    path = Path(template_path) if template_path else TEMPLATE_DIR / TEMPLATE_EMAIL_SECRETARIA
    if not path.exists():
        return "<html><body>Template email_secretaria.html não encontrado.</body></html>"
    env = Environment(
        loader=FileSystemLoader(str(path.parent)),
        autoescape=select_autoescape(["html"]),
    )
    template = env.get_template(path.name)
    return template.render(
        setor_nome=dados_setor.get("setor_nome", ""),
        periodo=dados_setor.get("periodo", ""),
        total_aberto=dados_setor.get("total_aberto", 0),
        total_tratamento=dados_setor.get("total_tratamento", 0),
        total_solucionado=dados_setor.get("total_solucionado", 0),
        blocos_bairro=dados_setor.get("blocos_bairro", []),
    )


def _extrair_conteudo_body(html: str) -> str:
    m = re.search(r"<body[^>]*>(.*)</body>", html, re.DOTALL | re.IGNORECASE)
    return (m.group(1) if m else html).strip()


def montar_html_relatorio_completo_publico(dados: dict) -> str:
    """HTML único para página pública: mesmo conteúdo dos e-mails (geral + cada secretaria)."""
    partes = [_extrair_conteudo_body(gerar_html_email_geral(dados))]
    periodo = dados.get("periodo", "")
    for s in dados.get("setores") or []:
        ds = {
            "setor_nome": s.get("nome", ""),
            "periodo": periodo,
            "total_aberto": s.get("total_aberto", 0),
            "total_tratamento": s.get("total_tratamento", 0),
            "total_solucionado": s.get("total_solucionado", 0),
            "blocos_bairro": s.get("blocos_bairro", []),
        }
        partes.append(_extrair_conteudo_body(gerar_html_email_secretaria(ds)))
    inner = '<hr style="margin:2.5rem 0;border:none;border-top:2px solid #2e7d32">'.join(p for p in partes if p)
    titulo = re.sub(r"<[^>]+>", "", periodo or "Relatório")[:120]
    return (
        "<!DOCTYPE html>\n<html lang=\"pt-BR\">\n<head>"
        '<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
        f"<title>Relatório Mapzer — {titulo}</title></head>\n"
        '<body style="margin:0;padding:16px;background:#f5f5f5">'
        f"{inner}</body></html>"
    )


def _anexar_link_relatorio_html(html: str, url: str) -> str:
    """Insere bloco com link público pessoal antes de </body>."""
    if not url or not (html or "").strip():
        return html
    href = html_module.escape(url, quote=True)
    bloco = (
        '<p style="margin-top:24px;padding:12px;background:#e8f5e9;border-left:4px solid #2e7d32;font-size:13px;">'
        "Visualize este relatório na web (link pessoal e por tempo limitado): "
        f'<a href="{href}">{href}</a></p>'
    )
    low = html.lower()
    idx = low.rfind("</body>")
    if idx >= 0:
        return html[:idx] + bloco + html[idx:]
    return html + bloco


def extrair_periodo(df: pd.DataFrame) -> str:
    """Extrai período do relatório (primeiro e último registro da coluna Data/Hora)."""
    col_data = _col(df, "oco_datahora", "oco_data_hora", "oco_data")
    for c in df.columns:
        if "data" in c.lower() and "hora" in c.lower():
            col_data = c
            break
    if col_data is None:
        return ""
    vals = pd.to_datetime(df[col_data], errors="coerce").dropna()
    if vals.empty:
        return ""
    d_min, d_max = vals.min(), vals.max()
    return f"{d_min.strftime('%d/%m/%Y')} a {d_max.strftime('%d/%m/%Y')}"


def aplicar_mapeamento_setor(df: pd.DataFrame, mapeamento: dict) -> pd.DataFrame:
    """Adiciona coluna setor e email baseado no mapeamento."""
    df = df.copy()
    col_tipo = _col(df, "oco_tipo_mapeamento", "oco_tipo")
    if col_tipo is None:
        df["setor"] = "Não mapeado"
        df["email"] = ""
        return df

    def _buscar(x):
        key = normalizar_tipo(x.strip()) if isinstance(x, str) else ""
        return mapeamento.get(key, {})

    df["setor"] = df[col_tipo].astype(str).map(lambda x: _buscar(x).get("setor", "Não mapeado"))
    df["email"] = df[col_tipo].astype(str).map(lambda x: _buscar(x).get("email", ""))
    return df


def gerar_resumo_setor_bairro_tipo(df: pd.DataFrame) -> dict:
    """
    Agrupa por Setor -> Bairro -> Tipo.
    Calcula total por tipo no bairro e quantidade com OS preenchida.
    Retorna: {setor: {periodo, total, total_com_os, email, blocos_bairro: [...]}}
    """
    col_bairro = _col(df, "oco_bairro")
    col_tipo = _col(df, "oco_tipo_mapeamento", "oco_tipo")
    col_os = _col(df, "oco_ordemservico", "oco_ordem_de_servico")
    if not col_os:
        for c in df.columns:
            if "ordem" in c.lower() and "servico" in c.lower():
                col_os = c
                break

    periodo = extrair_periodo(df)
    resultados = {}
    omitir_sem_loc = AppConfig().omitir_sem_localizacao

    for setor, grp in df.groupby("setor"):
        _col_b = col_bairro if col_bairro else "_bairro"
        _grp = grp.copy()
        if not col_bairro:
            if omitir_sem_loc:
                continue  # Sem coluna bairro = tudo "Não identificado" -> não contar
            _grp["_bairro"] = "Não identificado"
        elif omitir_sem_loc and col_bairro in _grp.columns:
            _vazio = AppConfig().valores_sem_localizacao
            _vazio_norm = frozenset(_norm_bairro(v) for v in _vazio)
            mask_sem_bairro = _grp[col_bairro].apply(
                lambda x: pd.isna(x) or _norm_bairro(x) in _vazio_norm
            )
            _grp = _grp[~mask_sem_bairro].copy()
        if _grp.empty:
            continue
        email = _grp["email"].iloc[0] if "email" in _grp.columns and len(_grp) else ""
        total = len(_grp)
        total_com_os = 0
        if col_os:
            total_com_os = _grp[col_os].apply(
                lambda x: 1 if x and str(x).strip() and str(x).lower() not in ("nan", "none", "") else 0
            ).sum()

        blocos_bairro = []
        for bairro, grp_b in _grp.groupby(_col_b):
            bairro_str = str(bairro).strip() if bairro and str(bairro) != "nan" else "Não identificado"
            _vazio_norm = frozenset(_norm_bairro(v) for v in AppConfig().valores_sem_localizacao)
            if omitir_sem_loc and (not bairro_str or _norm_bairro(bairro_str) in _vazio_norm):
                continue
            por_tipo = grp_b.groupby(col_tipo if col_tipo else "setor").size()
            com_os_por_tipo = {}
            if col_os:
                for tipo in por_tipo.index:
                    sub = grp_b[grp_b[col_tipo] == tipo] if col_tipo else grp_b
                    com_os_por_tipo[tipo] = sub[col_os].apply(
                        lambda x: 1 if x and str(x).strip() and str(x).lower() not in ("nan", "none", "") else 0
                    ).sum()

            linhas = [
                {"tipo": str(t), "qtd": int(q), "com_os": int(com_os_por_tipo.get(t, 0))}
                for t, q in por_tipo.items()
            ]
            subtotal = sum(l["qtd"] for l in linhas)
            subtotal_os = sum(l["com_os"] for l in linhas)
            blocos_bairro.append({
                "bairro": bairro_str,
                "linhas": linhas,
                "subtotal": subtotal,
                "subtotal_os": subtotal_os,
            })

        resultados[setor] = {
            "periodo": periodo,
            "total": total,
            "total_com_os": int(total_com_os),
            "email": email.strip() if email else "",
            "blocos_bairro": blocos_bairro,
        }
    return resultados


def html_resumo_setor(
    setor: str,
    dados: dict,
    template_path: Path | str | None = None,
) -> str:
    """Gera HTML usando template Jinja2. Variáveis: setor, periodo, total, total_com_os, blocos_bairro."""
    if template_path is None:
        custom = AppConfig().email_template
        template_path = Path(custom) if custom else TEMPLATE_DIR / DEFAULT_TEMPLATE
    else:
        template_path = Path(template_path)
    if not template_path.exists():
        return _html_resumo_fallback(setor, dados)
    env = Environment(
        loader=FileSystemLoader(str(template_path.parent)),
        autoescape=select_autoescape(["html"]),
    )
    template = env.get_template(template_path.name)
    corpo = template.render(
        setor=setor,
        periodo=dados.get("periodo", ""),
        total=dados.get("total", 0),
        total_com_os=dados.get("total_com_os", 0),
        blocos_bairro=dados.get("blocos_bairro", []),
    )
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"></head><body>{corpo}</body></html>"""


def _html_resumo_fallback(setor: str, dados: dict) -> str:
    """Fallback quando template não existe."""
    blocos = dados.get("blocos_bairro", [])
    html = f"<h2>Relatório - {setor}</h2><p>Período: {dados.get('periodo', '-')}</p>"
    html += f"<p>Total: {dados.get('total', 0)} | Com OS: {dados.get('total_com_os', 0)}</p>"
    for b in blocos:
        html += f"<h4>{b['bairro']}</h4><ul>"
        for l in b["linhas"]:
            html += f"<li>{l['tipo']}: {l['qtd']} (OS: {l['com_os']})</li>"
        html += f"</ul><p>Subtotal: {b['subtotal']}</p>"
    return f"<!DOCTYPE html><html><body>{html}</body></html>"


def enviar_email(
    destinos: list[str],
    assunto: str,
    corpo_html: str,
    config: SMTPConfig | None = None,
) -> None:
    """Envia e-mail HTML via SMTP. Porta 465 usa SSL; 587 usa STARTTLS."""
    config = config or SMTPConfig()
    if not config.smtp_user or not config.smtp_password:
        raise ValueError("SMTP_USER e SMTP_PASSWORD devem estar configurados no .env")
    msg = MIMEMultipart("alternative")
    msg["Subject"] = assunto
    # Servidor PLSS exige From = usuário autenticado
    from_addr = config.smtp_user
    msg["From"] = from_addr
    msg["To"] = ", ".join(destinos)
    msg.attach(MIMEText(corpo_html, "html"))
    try:
        if config.smtp_port == 465:
            with smtplib.SMTP_SSL(config.smtp_host, config.smtp_port) as server:
                server.login(config.smtp_user, config.smtp_password)
                server.sendmail(from_addr, destinos, msg.as_string())
        else:
            with smtplib.SMTP(config.smtp_host, config.smtp_port) as server:
                server.starttls()
                server.login(config.smtp_user, config.smtp_password)
                server.sendmail(from_addr, destinos, msg.as_string())
    except smtplib.SMTPException as e:
        logger.exception("SMTP enviar_email: %s", e)
        raise ConnectionError(f"Erro ao enviar e-mail: {e}") from e


def executar_relatorios(
    mapeamento_path: str | Path | None = None,
    db_config: DatabaseConfig | None = None,
    smtp_config: SMTPConfig | None = None,
    dt_inicio: datetime | str | None = None,
    dt_fim: datetime | str | None = None,
    usar_view: bool = True,
) -> dict:
    """
    Executa relatórios e envia e-mails.
    Se usar_view=True (padrão): consulta vw_ocorrencias_status e usa templates email_geral/email_secretaria.
    Se usar_view=False: usa lógica legada (ocorrencias + mapeamento JSON).
    Com usar_view=True, retorna __registros_envio: um item por destinatário (e-mail ou WhatsApp), com token/HTML próprios.
    """
    if usar_view:
        return _executar_relatorios_view(db_config, smtp_config, dt_inicio, dt_fim)

    # Legado
    mapeamento = carregar_mapeamento(mapeamento_path)
    df = obter_ocorrencias(db_config)
    df = aplicar_mapeamento_setor(df, mapeamento)
    resumos = gerar_resumo_setor_bairro_tipo(df)
    return _enviar_relatorios_legado(resumos, smtp_config)


def _parse_emails(valor: str) -> list[str]:
    """Extrai lista de e-mails de string com separadores ; ou ,"""
    if not valor or not str(valor).strip():
        return []
    emails = []
    for e in str(valor).replace(";", ",").split(","):
        e = e.strip()
        if e and e not in emails:
            emails.append(e)
    return emails


def _destinatarios_relatorio_geral(smtp: SMTPConfig) -> list[str]:
    """Retorna lista de e-mails para o relatório geral (EMAIL_RELTORIO_TOTAL + EMAIL_COPIA)."""
    emails = []
    for val in (smtp.email_relatorio_total, smtp.email_copia, smtp.email_prefeito):
        emails.extend(_parse_emails(val or ""))
        emails = list(dict.fromkeys(emails))  # mantém ordem, remove dup
    return emails


def _executar_relatorios_view(
    db_config: DatabaseConfig | None = None,
    smtp_config: SMTPConfig | None = None,
    dt_inicio: datetime | str | None = None,
    dt_fim: datetime | str | None = None,
) -> dict:
    """Usa view vw_ocorrencias_status e templates email_geral/email_secretaria. Um token por destinatário."""
    config = db_config or DatabaseConfig()
    df = obter_dados_view(dt_inicio, dt_fim, config)
    logger.info("Relatório: view retornou %d linhas, cols=%s", len(df), list(df.columns) if not df.empty else [])
    periodo_override = None
    if dt_inicio and dt_fim:
        try:
            d_i = pd.to_datetime(dt_inicio)
            d_f = pd.to_datetime(dt_fim)
            periodo_override = f"{d_i.strftime('%d/%m/%Y')} a {d_f.strftime('%d/%m/%Y')}"
        except Exception:
            pass
    dados = agregar_dados_para_relatorio(df, periodo_override=periodo_override)
    smtp = smtp_config or SMTPConfig()
    wa_cfg = WhatsAppConfig()
    resultados: dict = {}
    wa_auditoria: dict = {"geral": None, "setores": {}}
    periodo = dados.get("periodo", "")
    registros: list[dict] = []

    def _novo_token_e_url() -> tuple[str, str]:
        t = secrets.token_hex(16)
        return t, montar_url_relatorio_publico(t)

    html_geral_base = gerar_html_email_geral(dados)

    # 1. Relatório geral: um e-mail por destino (link e token únicos)
    dest_geral = _destinatarios_relatorio_geral(smtp)
    if dest_geral:
        ok_algum = False
        for email in dest_geral:
            try:
                tok, link = _novo_token_e_url()
                html = _anexar_link_relatorio_html(html_geral_base, link)
                enviar_email(
                    [email],
                    f"Relatório Geral de Ocorrências - {periodo}",
                    html,
                    smtp,
                )
                registros.append({
                    "token": tok,
                    "html": html,
                    "destinatario": email,
                    "env_meta": {"canal": "email", "tipo": "relatorio_geral"},
                })
                ok_algum = True
            except Exception as e:
                logger.exception("Envio relatório geral para %s: %s", email, e)
        resultados["_relatorio_geral"] = ok_algum

    # 1b. WhatsApp relatório geral: WHATSAPP_CHAT_IDS_RELATORIO_GERAL; texto = mesmo modelo da secretaria (totais consolidados)
    if _whatsapp_habilitado(wa_cfg) and dados.get("setores"):
        ids_geral = _parse_chat_ids(wa_cfg.whatsapp_chat_ids_relatorio_geral)
        if not ids_geral:
            logger.warning(
                "WhatsApp relatório geral: defina WHATSAPP_CHAT_IDS_RELATORIO_GERAL no .env (chatIds separados por vírgula)."
            )
        if ids_geral:
            ok_geral = True
            for cid in ids_geral:
                try:
                    tok, link = _novo_token_e_url()
                    texto_wa = montar_texto_whatsapp_geral(
                        dados, wa_cfg, link_relatorio_override=link
                    )
                    enviar_whatsapp_texto(cid, texto_wa, wa_cfg)
                    html_pub = _anexar_link_relatorio_html(html_geral_base, link)
                    registros.append({
                        "token": tok,
                        "html": html_pub,
                        "destinatario": f"wa:{cid}",
                        "env_meta": {"canal": "whatsapp", "tipo": "relatorio_geral"},
                    })
                    logger.info("WhatsApp relatório geral enviado (chatId=%s)", cid[:4] + "***")
                except Exception as e:
                    logger.exception("WhatsApp relatório geral chatId=%s: %s", cid, e)
                    ok_geral = False
            wa_auditoria["geral"] = ok_geral

    # 2. E-mail / WhatsApp por setor
    for _set_id, dados_setor in dados.get("setores_por_id", {}).items():
        total = (
            dados_setor.get("total_aberto", 0)
            + dados_setor.get("total_tratamento", 0)
            + dados_setor.get("total_solucionado", 0)
        )
        if total == 0:
            continue
        destinos = _parse_emails(dados_setor.get("email", ""))
        nome_setor = dados_setor.get("setor_nome", "")
        html_sec_base = gerar_html_email_secretaria(dados_setor)

        ok_email = False
        for email in destinos:
            try:
                tok, link = _novo_token_e_url()
                html = _anexar_link_relatorio_html(html_sec_base, link)
                enviar_email(
                    [email],
                    f"Relatório de Ocorrências - {dados_setor['setor_nome']} ({periodo})",
                    html,
                    smtp,
                )
                registros.append({
                    "token": tok,
                    "html": html,
                    "destinatario": email,
                    "env_meta": {
                        "canal": "email",
                        "tipo": "relatorio_secretaria",
                        "setor": nome_setor,
                    },
                })
                ok_email = True
            except Exception as e:
                logger.exception("Envio e-mail setor=%s dest=%s: %s", nome_setor, email, e)

        ok_wa_setor = False
        if _whatsapp_habilitado(wa_cfg):
            ids_setor = _parse_chat_ids(dados_setor.get("whatsapp") or "")
            if ids_setor:
                ok_sec = True
                for cid in ids_setor:
                    try:
                        tok, link = _novo_token_e_url()
                        texto_sec = montar_texto_whatsapp_secretaria(
                            dados_setor, wa_cfg, link_relatorio_override=link
                        )
                        enviar_whatsapp_texto(cid, texto_sec, wa_cfg)
                        html_pub = _anexar_link_relatorio_html(html_sec_base, link)
                        registros.append({
                            "token": tok,
                            "html": html_pub,
                            "destinatario": f"wa:{cid}",
                            "env_meta": {
                                "canal": "whatsapp",
                                "tipo": "relatorio_secretaria",
                                "setor": nome_setor,
                            },
                        })
                        ok_wa_setor = True
                        logger.info("WhatsApp setor=%s enviado (chatId=%s)", nome_setor, cid[:4] + "***")
                    except Exception as e:
                        logger.exception("WhatsApp setor=%s chatId=%s: %s", nome_setor, cid, e)
                        ok_sec = False
                wa_auditoria["setores"][nome_setor] = ok_sec

        resultados[nome_setor] = ok_email or ok_wa_setor

    if wa_auditoria["geral"] is not None or wa_auditoria["setores"]:
        resultados["whatsapp"] = wa_auditoria

    resultados["__registros_envio"] = registros
    return resultados


def _enviar_relatorios_legado(resumos: dict, smtp_config: SMTPConfig | None) -> dict[str, bool]:
    """Envio com lógica legada (template_email.html)."""
    smtp = smtp_config or SMTPConfig()
    periodo = next((r["periodo"] for r in resumos.values() if r.get("periodo")), "")
    resultados = {}
    for setor, dados in resumos.items():
        destinos = _parse_emails(dados.get("email", ""))
        if not destinos or setor == "Não mapeado":
            resultados[setor] = False
            continue
        try:
            html = html_resumo_setor(setor, dados)
            enviar_email(destinos, f"Relatório de Ocorrências - {setor} ({periodo})", html, smtp)
            resultados[setor] = True
        except Exception as e:
            logger.exception("Envio e-mail setor=%s: %s", setor, e)
            resultados[setor] = False
    email_prefeito = smtp.email_prefeito
    if email_prefeito and email_prefeito.strip():
        try:
            html_mestre = _gerar_html_relatorio_mestre(resumos)
            enviar_email(
                [e.strip() for e in email_prefeito.split(",")],
                f"Relatório Geral de Ocorrências - {periodo}",
                html_mestre,
                smtp,
            )
            resultados["_prefeito"] = True
        except Exception as e:
            logger.exception("Envio e-mail Prefeito/Cópia: %s", e)
            resultados["_prefeito"] = False
    return resultados


def _gerar_html_relatorio_mestre(resumos: dict) -> str:
    """Gera HTML com todas as seções de todos os setores para o Prefeito/Cópia (legado)."""
    template_path = TEMPLATE_DIR / DEFAULT_TEMPLATE
    if not template_path.exists():
        return "<html><body>Template não encontrado.</body></html>"
    env = Environment(
        loader=FileSystemLoader(str(template_path.parent)),
        autoescape=select_autoescape(["html"]),
    )
    template = env.get_template(template_path.name)
    secoes_html = []
    for setor, dados in resumos.items():
        if setor == "Não mapeado":
            continue
        sec = template.render(
            setor=setor,
            periodo=dados.get("periodo", ""),
            total=dados.get("total", 0),
            total_com_os=dados.get("total_com_os", 0),
            blocos_bairro=dados.get("blocos_bairro", []),
        )
        secoes_html.append(f'<div style="margin-bottom: 40px; border-bottom: 2px solid #4a90d9; padding-bottom: 20px;">{sec}</div>')
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"></head><body><h1 style="color: #1a73e8;">Relatório Geral de Ocorrências</h1>{"".join(secoes_html)}</body></html>"""
