"""Envio de texto via API WhatsApp (sendText) e formatação de mensagens de relatório."""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

from src.config import WhatsAppConfig

ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT / "config"
logger = logging.getLogger("mapzer")

_jinja_whatsapp: Environment | None = None


def _env_whatsapp_templates() -> Environment:
    global _jinja_whatsapp
    if _jinja_whatsapp is None:
        _jinja_whatsapp = Environment(
            loader=FileSystemLoader([str(CONFIG_DIR), str(ROOT)]),
            autoescape=False,
            auto_reload=True,
            trim_blocks=True,
            lstrip_blocks=True,
        )
    return _jinja_whatsapp


def _resolver_caminho_template(nome_ou_caminho: str) -> Path:
    s = (nome_ou_caminho or "").strip()
    if not s:
        return CONFIG_DIR / "whatsapp_mensagem_geral.txt"
    p = Path(s)
    if p.is_absolute():
        return p
    cand = ROOT / s
    if cand.is_file():
        return cand
    return CONFIG_DIR / p.name


def periodo_inicial_final(periodo: str) -> tuple[str, str]:
    """Extrai 'DD/MM/AAAA a DD/MM/AAAA' -> (inicial, final)."""
    p = (periodo or "").strip()
    if " a " in p:
        a, b = p.split(" a ", 1)
        return a.strip(), b.strip()
    if p:
        return p, p
    return "—", "—"


def link_relatorio_detalhado(cfg: WhatsAppConfig) -> str:
    """URL do relatório; opcional ?token= ou &token= a partir de MAPZER_RELATORIO_TOKEN."""
    url = (cfg.mapzer_relatorio_url or "").strip()
    if not url:
        base = (cfg.mapzer_portal_url or "").strip().rstrip("/")
        if base.endswith("/login"):
            base = base[: -len("/login")].rstrip("/")
        url = f"{base}/rel-ocorrencia" if base else ""
    tok = (cfg.mapzer_relatorio_token or "").strip()
    if not url or not tok:
        return url
    sep = "&" if "?" in url else "?"
    if "token=" in url:
        return url
    return f"{url}{sep}token={tok}"


def _render_whatsapp_arquivo(caminho: Path, **kwargs: Any) -> str:
    if not caminho.is_file():
        logger.warning("Template WhatsApp não encontrado: %s", caminho)
        return ""
    env = _env_whatsapp_templates()
    resolved = caminho.resolve()
    for base in (CONFIG_DIR.resolve(), ROOT.resolve()):
        try:
            rel = resolved.relative_to(base)
            tpl = env.get_template(rel.as_posix())
            return tpl.render(**kwargs).strip()
        except ValueError:
            continue
    src = caminho.read_text(encoding="utf-8")
    return env.from_string(src).render(**kwargs).strip()


def _whatsapp_habilitado(cfg: WhatsAppConfig) -> bool:
    return bool((cfg.whatsapp_server_url or "").strip() and (cfg.whatsapp_server_token or "").strip())


def _parse_chat_ids(valor: str | None) -> list[str]:
    if not valor or not str(valor).strip():
        return []
    ids: list[str] = []
    for parte in str(valor).replace(";", ",").replace("\n", ",").split(","):
        cid = normalizar_chat_id(parte)
        if cid and cid not in ids:
            ids.append(cid)
    return ids


def normalizar_chat_id(valor: str) -> str:
    """
    Só dígitos; se não começar com 55 (Brasil), prefixa 55.
    Ex.: 42 99946-1801 -> 5542999461801; 5542999461801 inalterado.
    """
    d = "".join(c for c in str(valor or "").strip() if c.isdigit())
    if not d:
        return ""
    if not d.startswith("55"):
        d = "55" + d
    return d


def enviar_whatsapp_texto(
    chat_id: str,
    text: str,
    cfg: WhatsAppConfig | None = None,
    timeout_s: int = 60,
) -> dict[str, Any]:
    """
    POST /api/sendText. Espera 201 (ou 200) de sucesso.
    Retorna o JSON decodificado da API.
    """
    cfg = cfg or WhatsAppConfig()
    if not _whatsapp_habilitado(cfg):
        raise ValueError("WHATSAPP_SERVER_URL e WHATSAPP_SERVER_TOKEN são obrigatórios para enviar WhatsApp")

    base = cfg.whatsapp_server_url.strip().rstrip("/")
    path = (cfg.whatsapp_url_sendtext or "/api/sendText").strip()
    if not path.startswith("/"):
        path = "/" + path
    url = f"{base}{path}"

    body = {
        "chatId": normalizar_chat_id(chat_id),
        "text": text,
        "session": (cfg.whatsapp_session or "default").strip() or "default",
        "linkPreview": bool(cfg.whatsapp_link_preview),
    }
    if not body["chatId"]:
        raise ValueError("chatId vazio após normalização")

    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={
            "X-Api-Key": cfg.whatsapp_server_token.strip(),
            "Content-Type": "application/json; charset=utf-8",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            code = resp.getcode()
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace") if e.fp else ""
        logger.error("WhatsApp HTTP %s: %s", e.code, err_body[:500])
        raise ConnectionError(f"WhatsApp API retornou HTTP {e.code}") from e
    except urllib.error.URLError as e:
        logger.exception("WhatsApp URL error: %s", e)
        raise ConnectionError(f"Erro de rede ao chamar API WhatsApp: {e}") from e

    if code not in (200, 201):
        raise ConnectionError(f"WhatsApp API status inesperado: {code}")

    if not raw.strip():
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Resposta WhatsApp não é JSON: %s", raw[:200])
        return {"_raw": raw}


def _tabela_monoespacada(headers: tuple[str, ...], linhas: list[tuple[str, ...]]) -> str:
    if not linhas:
        return ""
    ncol = len(headers)
    cols: list[list[str]] = [list(headers)]
    for row in linhas:
        cols.append([str(row[i]) if i < len(row) else "" for i in range(ncol)])
    widths = [max(len(cols[r][c]) for r in range(len(cols))) for c in range(ncol)]

    def fmt_row(r: int) -> str:
        return " | ".join(cols[r][c].ljust(widths[c]) for c in range(ncol))

    sep = "-+-".join("-" * w for w in widths)
    out = [fmt_row(0), sep]
    out.extend(fmt_row(i) for i in range(1, len(cols)))
    return "\n".join(out)


def _tabela_setores_de_dados(dados_lista_setores: list) -> str:
    linhas_tb: list[tuple[str, ...]] = []
    for s in dados_lista_setores:
        nome = str(s.get("nome", ""))[:48]
        linhas_tb.append(
            (
                nome,
                str(int(s.get("total_aberto", 0))),
                str(int(s.get("total_tratamento", 0))),
                str(int(s.get("total_solucionado", 0))),
            )
        )
    if not linhas_tb:
        return "(nenhum setor com dados no período)"
    return _tabela_monoespacada(
        ("Setor", "Em Aberto", "Em Tratamento", "Solucionada"),
        linhas_tb,
    )


def montar_texto_whatsapp_geral(
    dados: dict,
    cfg: WhatsAppConfig | None = None,
    link_relatorio_override: str | None = None,
) -> str:
    """
    Relatório geral no WhatsApp: mesmo modelo de secretaria (whatsapp_mensagem_secretaria.txt),
    com totais consolidados e rótulo fixo no departamento.
    """
    consolidado = {
        "periodo": dados.get("periodo", ""),
        "setor_nome": "Visão geral (todos os setores)",
        "total_aberto": int(dados.get("total_aberto", 0)),
        "total_tratamento": int(dados.get("total_tratamento", 0)),
        "total_solucionado": int(dados.get("total_solucionado", 0)),
    }
    return montar_texto_whatsapp_secretaria(
        consolidado, cfg, link_relatorio_override=link_relatorio_override
    )


def montar_texto_whatsapp_secretaria(
    dados_setor: dict,
    cfg: WhatsAppConfig | None = None,
    link_relatorio_override: str | None = None,
) -> str:
    """Mensagem por secretaria: totais em linhas (modelo em whatsapp_mensagem_secretaria.txt)."""
    cfg = cfg or WhatsAppConfig()
    periodo = dados_setor.get("periodo", "")
    p_ini, p_fim = periodo_inicial_final(periodo)
    setor = str(dados_setor.get("setor_nome", "")).strip()[:120]
    ta = int(dados_setor.get("total_aberto", 0))
    tt = int(dados_setor.get("total_tratamento", 0))
    ts = int(dados_setor.get("total_solucionado", 0))
    link = (link_relatorio_override or "").strip() or link_relatorio_detalhado(cfg)
    path_tpl = _resolver_caminho_template(cfg.whatsapp_template_secretaria)
    texto = _render_whatsapp_arquivo(
        path_tpl,
        periodo_inicial=p_ini,
        periodo_final=p_fim,
        periodo_completo=periodo,
        nome_setor=setor,
        total_aberto=ta,
        total_tratamento=tt,
        total_solucionada=ts,
        link_relatorio=link or (cfg.mapzer_portal_url or "").strip(),
    )
    if texto:
        return texto
    return (
        f"Olá, resumo Mapzer {p_ini} à {p_fim} — *{setor}*.\n\n*Ocorrências*\n"
        f"Em Aberto: {ta}\nEm Tratamento: {tt}\nSolucionada: {ts}\n\n{link or ''}".strip()
    )
