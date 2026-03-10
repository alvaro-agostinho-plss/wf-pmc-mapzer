"""API FastAPI - upload de planilhas e processamento."""

import logging
import os
from pathlib import Path

from fastapi import Depends, FastAPI, File, HTTPException, Query, UploadFile
from pydantic import BaseModel
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from src.api.auth import obter_usuario
from src.api.upload import (
    UPLOADS_DIR,
    criar_lote,
    enviar_emails,
    enviar_emails_por_lote,
    excluir_lote,
    excluir_upload,
    listar_lotes,
    listar_uploads,
    listar_uploads_pendentes,
    obter_upload,
    processar_lote_por_id,
    processar_upload,
    processar_upload_os,
    upload_ou_substituir,
    validar_envio_email,
)

ROOT = Path(__file__).resolve().parent.parent.parent
logger = logging.getLogger("mapzer")


def _log_erro(e: Exception, contexto: str = ""):
    logger.exception("%s: %s", contexto or "Erro", e)


app = FastAPI(title="ETL Ocorrências Mapzer", version="0.1.0")
app.mount("/static", StaticFiles(directory=str(ROOT / "static")), name="static")


@app.on_event("startup")
def startup():
    """Inicialização do servidor. NÃO envia e-mails — envio apenas via POST /api/enviar-emails."""
    from src.log_config import configurar_logging
    from src.api.upload import _get_uploads_dir
    configurar_logging()
    _get_uploads_dir().mkdir(parents=True, exist_ok=True)


def _base_path() -> str:
    return (os.getenv("BASE_PATH") or "").rstrip("/")


def _read_html(name: str) -> str:
    html_path = ROOT / "static" / name
    if not html_path.exists():
        return ""
    return html_path.read_text(encoding="utf-8").replace(
        "__BASE_PATH__", _base_path()
    )


@app.get("/health")
def health():
    """Health check para Docker/K8s."""
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def index():
    """Serve a interface web."""
    html = _read_html("index.html")
    return html or "<h1>ETL Ocorrências</h1><p>Interface em static/index.html</p>"


@app.get("/login", response_class=HTMLResponse)
def login_page():
    """Página de login SSO."""
    html = _read_html("login.html")
    return html or "<h1>Login</h1><p>static/login.html não encontrado</p>"


class LoginBody(BaseModel):
    username: str
    password: str


@app.post("/api/login")
def login_api(body: LoginBody):
    """Autentica via SSO e retorna token JWT."""
    username, password = body.username, body.password
    try:
        from src.sso import authenticate
        result = authenticate(username, password)
        if not result.ok:
            raise HTTPException(401, result.error)
        return result.to_dict()
    except ValueError as e:
        raise HTTPException(500, str(e)) from e
    except HTTPException:
        raise
    except Exception as e:
        _log_erro(e, "Login SSO")
        raise HTTPException(500, "Erro ao autenticar.") from e


class RefreshBody(BaseModel):
    refresh_token: str


@app.post("/api/refresh")
def refresh_api(body: RefreshBody):
    """Renova access_token usando refresh_token. Estende a sessão."""
    try:
        from src.sso import refresh_access_token
        result = refresh_access_token(body.refresh_token)
        if not result.ok:
            raise HTTPException(401, result.error)
        return result.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        _log_erro(e, "Refresh token")
        raise HTTPException(500, "Erro ao renovar sessão.") from e


@app.get("/api/auth/me")
def auth_me(usuario: dict = Depends(obter_usuario)):
    """Retorna dados do usuário autenticado."""
    return usuario


def _username(usuario: dict) -> str:
    """Extrai username do payload JWT para auditoria."""
    return (usuario.get("username") or usuario.get("sub") or "sistema")[:255]


@app.post("/api/upload")
def upload_planilha(
    usuario: dict = Depends(obter_usuario),
    arquivo: UploadFile = File(...),
    tipo_esperado: str | None = Query(None, description="ocorrencias | os - valida tipo do arquivo"),
):
    """Recebe planilha .xlsx e registra no banco. Opcional: tipo_esperado para validar zona."""
    if not arquivo.filename:
        raise HTTPException(400, "Nome do arquivo não informado")
    if not arquivo.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(400, "Formato inválido. Use .xlsx")
    try:
        resultado = upload_ou_substituir(arquivo, tipo_esperado=tipo_esperado, usuario=_username(usuario))
        msg = "Arquivo substituído" if resultado.get("substituiu") else "Upload realizado"
        return {
            "id": resultado["id"],
            "nome": resultado["nome"],
            "status": "uploaded",
            "mensagem": msg,
            "tipo": resultado.get("tipo"),
        }
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    except Exception as e:
        _log_erro(e, "Upload")
        raise HTTPException(500, str(e)) from e


@app.get("/api/uploads")
def listar_uploads_api(_: dict = Depends(obter_usuario)):
    """Lista histórico de uploads (mais recentes primeiro)."""
    try:
        return listar_uploads()
    except Exception as e:
        _log_erro(e, "Listar uploads")
        raise HTTPException(500, str(e)) from e


@app.delete("/api/uploads/{upload_id}")
def excluir_upload_api(upload_id: str, _: dict = Depends(obter_usuario)):
    """Exclui o upload: remove arquivo do disco e registro do banco."""
    try:
        if not excluir_upload(upload_id):
            raise HTTPException(404, "Upload não encontrado")
        return {"ok": True, "mensagem": "Upload excluído"}
    except HTTPException:
        raise
    except Exception as e:
        _log_erro(e, "Excluir upload")
        raise HTTPException(500, str(e)) from e


@app.get("/api/uploads/{upload_id}")
def detalhe_upload(upload_id: str, _: dict = Depends(obter_usuario)):
    """Detalhes de um upload específico."""
    try:
        u = obter_upload(upload_id)
        if not u:
            raise HTTPException(404, "Upload não encontrado")
        return u
    except HTTPException:
        raise
    except Exception as e:
        _log_erro(e, "Detalhe upload")
        raise HTTPException(500, str(e)) from e


@app.post("/api/processar/{upload_id}")
def processar(upload_id: str, truncar: bool = False, usuario: dict = Depends(obter_usuario)):
    """Executa ETL de ocorrências (carrega dados da planilha no banco)."""
    try:
        return processar_upload(upload_id, truncar_antes=truncar, usuario=_username(usuario))
    except FileNotFoundError as e:
        raise HTTPException(404, str(e)) from e
    except Exception as e:
        _log_erro(e, "Processar ETL")
        raise HTTPException(500, str(e)) from e


@app.post("/api/processar-os/{upload_id}")
def processar_os(upload_id: str, truncar: bool = False, usuario: dict = Depends(obter_usuario)):
    """Executa ETL de Ordem de Serviço (planilha com aba 'Ordem de Serviço')."""
    try:
        return processar_upload_os(upload_id, truncar_antes=truncar, usuario=_username(usuario))
    except FileNotFoundError as e:
        raise HTTPException(404, str(e)) from e
    except Exception as e:
        _log_erro(e, "Processar OS")
        raise HTTPException(500, str(e)) from e


@app.get("/api/debug-tip-id")
def debug_tip_id_api(_: dict = Depends(obter_usuario)):
    """Diagnóstico: ocorrencias (oco_ordemservico, tip_id) e ordens_servico para debug de tip_id."""
    try:
        from src.etl_os import diagnosticar_tip_id
        return diagnosticar_tip_id()
    except Exception as e:
        _log_erro(e, "Debug tip_id")
        raise HTTPException(500, str(e)) from e


@app.get("/api/validar-envio")
def validar_envio_api(_: dict = Depends(obter_usuario)):
    """Retorna se pode enviar e-mail (requer ocorrências e OS processados)."""
    try:
        return validar_envio_email()
    except Exception as e:
        _log_erro(e, "Validar envio")
        raise HTTPException(500, str(e)) from e


@app.get("/api/uploads-pendentes")
def listar_pendentes_api(_: dict = Depends(obter_usuario)):
    """Uploads pendentes (não vinculados a lote) por tipo."""
    try:
        return listar_uploads_pendentes()
    except Exception as e:
        _log_erro(e, "Listar pendentes")
        raise HTTPException(500, str(e)) from e


@app.get("/api/lotes")
def listar_lotes_api(_: dict = Depends(obter_usuario)):
    """Lista lotes processados."""
    try:
        return listar_lotes()
    except Exception as e:
        _log_erro(e, "Listar lotes")
        raise HTTPException(500, str(e)) from e


class CriarLoteBody(BaseModel):
    upl_id_ocorrencias: str
    upl_id_os: str


@app.post("/api/lotes")
def criar_lote_api(body: CriarLoteBody, usuario: dict = Depends(obter_usuario)):
    """Cria lote vinculando os dois uploads (ocorrências + OS), sem processar."""
    try:
        return criar_lote(body.upl_id_ocorrencias, body.upl_id_os)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    except Exception as e:
        _log_erro(e, "Criar lote")
        raise HTTPException(500, str(e)) from e


@app.post("/api/lotes/{lot_id}/processar")
def processar_lote_api(lot_id: str, usuario: dict = Depends(obter_usuario)):
    """Processa um lote específico."""
    try:
        return processar_lote_por_id(lot_id, usuario=_username(usuario))
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    except FileNotFoundError as e:
        raise HTTPException(404, str(e)) from e
    except Exception as e:
        _log_erro(e, "Processar lote")
        raise HTTPException(500, str(e)) from e


@app.post("/api/lotes/{lot_id}/enviar-email")
def enviar_email_lote_api(lot_id: str, usuario: dict = Depends(obter_usuario)):
    """Envia relatórios por e-mail e registra envio no lote."""
    try:
        return enviar_emails_por_lote(lot_id)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    except Exception as e:
        _log_erro(e, "Enviar e-mail")
        raise HTTPException(500, str(e)) from e


@app.delete("/api/lotes/{lot_id}")
def excluir_lote_api(lot_id: str, _: dict = Depends(obter_usuario)):
    """Exclui lote e os dois arquivos (upload + disco)."""
    try:
        if not excluir_lote(lot_id):
            raise HTTPException(404, "Lote não encontrado")
        return {"ok": True, "mensagem": "Lote excluído (arquivos removidos)"}
    except HTTPException:
        raise
    except Exception as e:
        _log_erro(e, "Excluir lote")
        raise HTTPException(500, str(e)) from e


@app.post("/api/enviar-emails")
def enviar_emails_api(_: dict = Depends(obter_usuario)):
    """Envia relatórios por e-mail. Obrigatório ter ocorrências e OS processados."""
    try:
        return enviar_emails()
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    except Exception as e:
        _log_erro(e, "Enviar e-mails")
        raise HTTPException(500, str(e)) from e
