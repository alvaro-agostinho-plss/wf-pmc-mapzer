"""Lógica de upload e processamento de planilhas."""

import logging
import uuid
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from fastapi import UploadFile
from sqlalchemy import text

from src.config import AppConfig, DatabaseConfig
from src.etl import executar_etl
from src.etl_os import executar_etl_os, validar_os_contra_ocorrencias
from src.reports import executar_relatorios

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent.parent
APP_CONFIG = AppConfig()
logger = logging.getLogger("mapzer")
DB_CONFIG = DatabaseConfig()


def _get_uploads_dir() -> Path:
    """Retorna dir de uploads, usando ROOT/uploads se o configurado não for gravável."""
    if APP_CONFIG.dir_uploads:
        p = Path(APP_CONFIG.dir_uploads)
        try:
            p.mkdir(parents=True, exist_ok=True)
            return p
        except PermissionError:
            pass
    p = ROOT / "uploads"
    p.mkdir(parents=True, exist_ok=True)
    return p


UPLOADS_DIR = _get_uploads_dir()


def _engine():
    from sqlalchemy import create_engine
    return create_engine(DB_CONFIG.connection_url)


def _buscar_upload_por_nome(nome_arquivo: str) -> dict | None:
    """Retorna upload existente com mesmo nome de arquivo (mais recente)."""
    eng = _engine()
    with eng.connect() as conn:
        r = conn.execute(
            text("""
                SELECT upl_id, upl_nomearquivo, upl_caminhoarmazenado
                FROM uploads_planilha
                WHERE upl_nomearquivo = :nome
                ORDER BY update_at DESC
                LIMIT 1
            """),
            {"nome": nome_arquivo},
        )
        row = r.fetchone()
    if not row:
        return None
    return {"id": str(row[0]), "nome_arquivo": row[1], "caminho_armazenado": row[2]}


def _garantir_coluna_upl_tipo():
    """Adiciona coluna upl_tipo se não existir (migração)."""
    try:
        eng = _engine()
        with eng.connect() as conn:
            r = conn.execute(text("""
                SELECT 1 FROM information_schema.columns
                WHERE table_name='uploads_planilha' AND column_name='upl_tipo'
            """))
            if r.fetchone():
                return
            conn.execute(text("ALTER TABLE uploads_planilha ADD COLUMN upl_tipo VARCHAR(20)"))
            conn.commit()
    except Exception:
        pass


def _garantir_tabela_lotes():
    """Cria tabela lotes se não existir (migração)."""
    try:
        eng = _engine()
        with eng.connect() as conn:
            r = conn.execute(text("""
                SELECT 1 FROM information_schema.tables WHERE table_name = 'lotes'
            """))
            if r.fetchone():
                return
            sql = (ROOT / "scripts" / "migrate_lotes.sql").read_text(encoding="utf-8")
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if stmt and not stmt.startswith("--"):
                    conn.execute(text(stmt))
            conn.commit()
    except Exception:
        pass


def salvar_arquivo_upload(arquivo: UploadFile, caminho_existente: str | None = None) -> tuple[str, int]:
    """
    Salva arquivo em uploads. Se caminho_existente informado, sobrescreve o arquivo (substituição).
    Retorna (caminho_relativo, tamanho).
    """
    uploads = _get_uploads_dir()
    conteudo = arquivo.file.read()
    tamanho = len(conteudo)
    if tamanho > 50 * 1024 * 1024:  # 50MB
        raise ValueError("Arquivo muito grande (máx 50MB)")
    if caminho_existente:
        caminho_full = uploads / caminho_existente
        caminho_full.parent.mkdir(parents=True, exist_ok=True)
        caminho_full.write_bytes(conteudo)
        return caminho_existente, tamanho
    pasta = uploads / datetime.now().strftime("%Y-%m")
    pasta.mkdir(parents=True, exist_ok=True)
    nome_salvo = f"{uuid.uuid4().hex}.xlsx"
    caminho_full = pasta / nome_salvo
    caminho_full.write_bytes(conteudo)
    return f"{pasta.name}/{nome_salvo}", tamanho


def substituir_upload_existente(upload_id: str, caminho: str, tamanho: int, usuario: str = "sistema") -> None:
    """Atualiza registro de upload com novo arquivo (substituição)."""
    eng = _engine()
    with eng.connect() as conn:
        conn.execute(
            text("""
                UPDATE uploads_planilha
                SET upl_tamanhobytes = :tam, upl_status = 'uploaded',
                    upl_totalregistros = NULL, upl_mensagemerro = NULL,
                    updated_by = :usuario, update_at = CURRENT_TIMESTAMP
                WHERE upl_id = :id
            """),
            {"id": upload_id, "tam": tamanho, "usuario": usuario},
        )
        conn.commit()


def _detectar_e_atualizar_tipo(upload_id: str, caminho_rel: str) -> str | None:
    """Detecta tipo da planilha e atualiza registro. Retorna upl_tipo."""
    from src.etl import identificar_tipo_planilha
    _garantir_coluna_upl_tipo()
    uploads = _get_uploads_dir()
    caminho_full = uploads / caminho_rel
    if not caminho_full.exists():
        return None
    tipo = identificar_tipo_planilha(caminho_full)
    eng = _engine()
    with eng.connect() as conn:
        conn.execute(
            text("UPDATE uploads_planilha SET upl_tipo = :tipo WHERE upl_id = :id"),
            {"tipo": tipo if tipo != "desconhecido" else None, "id": upload_id},
        )
        conn.commit()
    return tipo


def upload_ou_substituir(
    arquivo: UploadFile,
    tipo_esperado: str | None = None,
    usuario: str = "sistema",
) -> dict:
    """
    Faz upload do arquivo. Se já existe upload com o mesmo nome, substitui o arquivo e o registro.
    Identifica tipo (ocorrencias/os) pelos títulos das colunas.
    Retorna {id, nome, status, substituiu: bool, tipo?: str}.
    """
    nome = arquivo.filename or ""
    existente = _buscar_upload_por_nome(nome)
    if existente:
        caminho, tamanho = salvar_arquivo_upload(arquivo, caminho_existente=existente["caminho_armazenado"])
        substituir_upload_existente(existente["id"], caminho, tamanho, usuario=usuario)
        tipo = _detectar_e_atualizar_tipo(existente["id"], caminho)
        if tipo_esperado and (tipo or "").lower() != tipo_esperado.lower():
            raise ValueError(f"Arquivo não é do tipo esperado ({tipo_esperado}). Detectado: {tipo or 'desconhecido'}.")
        return {"id": existente["id"], "nome": nome, "status": "uploaded", "substituiu": True, "tipo": tipo}
    caminho, tamanho = salvar_arquivo_upload(arquivo)
    from src.etl import identificar_tipo_planilha
    uploads = _get_uploads_dir()
    tipo = identificar_tipo_planilha(uploads / caminho)
    if tipo_esperado:
        tipo_ok = (tipo or "").lower() == tipo_esperado.lower()
        if not tipo_ok:
            # Remove arquivo salvo (registro ainda não existe)
            (uploads / caminho).unlink(missing_ok=True)
            raise ValueError(f"Arquivo não é do tipo esperado ({tipo_esperado}). Detectado: {tipo or 'desconhecido'}.")
    registro = criar_registro_upload(
        nome_arquivo=nome,
        caminho=caminho,
        tamanho_bytes=tamanho,
        upl_tipo=tipo if tipo and tipo != "desconhecido" else None,
        usuario=usuario,
    )
    return {"id": registro["id"], "nome": nome, "status": "uploaded", "substituiu": False, "tipo": tipo}


def criar_registro_upload(
    nome_arquivo: str,
    caminho: str,
    tamanho_bytes: int,
    upl_tipo: str | None = None,
    usuario: str = "sistema",
) -> dict:
    """Insere registro em uploads_planilha e retorna o registro criado."""
    eng = _engine()
    _criar_tabela_se_nao_existir(eng)
    _garantir_coluna_upl_tipo()
    uid = str(uuid.uuid4())
    with eng.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO uploads_planilha
                (upl_id, upl_nomearquivo, upl_caminhoarmazenado, upl_tamanhobytes, upl_status, upl_tipo, create_by, updated_by)
                VALUES (:id, :nome, :caminho, :tam, 'uploaded', :tipo, :create_by, :updated_by)
            """),
            {
                "id": uid,
                "nome": nome_arquivo,
                "caminho": caminho,
                "tam": tamanho_bytes,
                "tipo": upl_tipo,
                "create_by": usuario,
                "updated_by": usuario,
            },
        )
        conn.commit()
    return {"id": uid, "nome_arquivo": nome_arquivo, "caminho_armazenado": caminho}


def _criar_tabela_se_nao_existir(engine):
    """Cria tabela uploads_planilha se não existir (scripts/init_uploads.sql)."""
    from sqlalchemy import inspect
    insp = inspect(engine)
    if "uploads_planilha" in insp.get_table_names():
        return
    sql = Path(ROOT / "scripts" / "init_uploads.sql").read_text(encoding="utf-8")
    with engine.connect() as conn:
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt and not stmt.startswith("--"):
                conn.execute(text(stmt))
        conn.commit()


def listar_uploads(limite: int = 50) -> list[dict]:
    """Lista uploads ordenados por create_at DESC."""
    _garantir_coluna_upl_tipo()
    eng = _engine()
    with eng.connect() as conn:
        r = conn.execute(
            text("""
                SELECT upl_id, upl_nomearquivo, upl_caminhoarmazenado, upl_tamanhobytes,
                       upl_totalregistros, upl_status, upl_mensagemerro, create_at, upl_tipo
                FROM uploads_planilha
                ORDER BY create_at DESC
                LIMIT :lim
            """),
            {"lim": limite},
        )
        rows = r.fetchall()
    result = []
    for row in rows:
        item = {
            "id": str(row[0]),
            "nome_arquivo": row[1],
            "caminho_armazenado": row[2],
            "tamanho_bytes": row[3],
            "total_registros": row[4],
            "status": row[5],
            "mensagem_erro": row[6],
            "create_at": row[7].isoformat() if row[7] else None,
        }
        item["tipo"] = row[8] if len(row) > 8 else None
        result.append(item)
    return result


def excluir_upload(upload_id: str) -> bool:
    """
    Remove upload: exclui o arquivo do disco e o registro do banco.
    Retorna True se excluiu, False se não encontrou.
    """
    u = obter_upload(upload_id)
    if not u:
        return False
    eng = _engine()
    # 1. Excluir arquivo do diretório
    uploads = _get_uploads_dir()
    caminho_full = uploads / u["caminho_armazenado"]
    if caminho_full.exists():
        try:
            caminho_full.unlink()
        except OSError as e:
            logger.warning("Não foi possível excluir arquivo %s: %s", caminho_full, e)
    # 2. Excluir registro do banco
    with eng.connect() as conn:
        conn.execute(text("DELETE FROM uploads_planilha WHERE upl_id = :id"), {"id": upload_id})
        conn.commit()
    return True


def obter_upload(upload_id: str) -> dict | None:
    """Retorna um upload por ID."""
    _garantir_coluna_upl_tipo()
    eng = _engine()
    with eng.connect() as conn:
        r = conn.execute(
            text("""
                SELECT upl_id, upl_nomearquivo, upl_caminhoarmazenado, upl_tamanhobytes,
                       upl_totalregistros, upl_status, upl_mensagemerro, create_at, upl_tipo
                FROM uploads_planilha WHERE upl_id = :id
            """),
            {"id": upload_id},
        )
        row = r.fetchone()
    if not row:
        return None
    out = {
        "id": str(row[0]),
        "nome_arquivo": row[1],
        "caminho_armazenado": row[2],
        "tamanho_bytes": row[3],
        "total_registros": row[4],
        "status": row[5],
        "mensagem_erro": row[6],
        "create_at": row[7].isoformat() if row[7] else None,
    }
    if len(row) > 8:
        out["tipo"] = row[8]
    return out


def _atualizar_status(
    upload_id: str,
    status: str,
    total: int | None = None,
    erro: str | None = None,
    usuario: str = "sistema",
):
    eng = _engine()
    with eng.connect() as conn:
        if total is not None:
            conn.execute(
                text("""
                    UPDATE uploads_planilha
                    SET upl_status = :status, upl_totalregistros = :total,
                        upl_mensagemerro = :erro, updated_by = :usuario, update_at = CURRENT_TIMESTAMP
                    WHERE upl_id = :id
                """),
                {"id": upload_id, "status": status, "total": total, "erro": erro, "usuario": usuario},
            )
        else:
            conn.execute(
                text("""
                    UPDATE uploads_planilha
                    SET upl_status = :status, upl_mensagemerro = :erro, updated_by = :usuario, update_at = CURRENT_TIMESTAMP
                    WHERE upl_id = :id
                """),
                {"id": upload_id, "status": status, "erro": erro, "usuario": usuario},
            )
        conn.commit()


def processar_upload_os(upload_id: str, truncar_antes: bool = False, usuario: str = "sistema") -> dict:
    """
    Executa ETL de Ordem de Serviço (planilha com aba 'Ordem de Serviço').
    Persiste em ordens_servico.
    """
    u = obter_upload(upload_id)
    if not u:
        raise FileNotFoundError(f"Upload {upload_id} não encontrado")
    caminho_full = _get_uploads_dir() / u["caminho_armazenado"]
    if not caminho_full.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_full}")
    _atualizar_status(upload_id, "processando", usuario=usuario)
    try:
        n = executar_etl_os(caminho_full, truncar_antes=truncar_antes, usuario=usuario)
        _atualizar_status(upload_id, "processado", total=n, usuario=usuario)
        return {"total_registros": n, "status": "processado", "tipo": "ordens_servico"}
    except Exception as e:
        logger.exception("ETL OS upload_id=%s: %s", upload_id, e)
        err_msg = str(e)
        if len(err_msg) > 500:
            idx = max(err_msg.find("ERRO:"), err_msg.find("viola"), 0)
            err_msg = err_msg[idx:idx + 480] + "…"
        _atualizar_status(upload_id, "erro", erro=err_msg, usuario=usuario)
        raise


def processar_upload(upload_id: str, truncar_antes: bool = False, usuario: str = "sistema") -> dict:
    """
    Executa apenas o ETL (leitura Excel -> persistência em ocorrencias).
    header_row=5 (Mapzer). Não envia e-mails.
    """
    u = obter_upload(upload_id)
    if not u:
        raise FileNotFoundError(f"Upload {upload_id} não encontrado")
    caminho_full = _get_uploads_dir() / u["caminho_armazenado"]
    if not caminho_full.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho_full}")
    _atualizar_status(upload_id, "processando", usuario=usuario)
    try:
        n = executar_etl(caminho_full, usuario=usuario, truncar_antes=truncar_antes)
        _atualizar_status(upload_id, "processado", total=n, usuario=usuario)
        return {"total_registros": n, "status": "processado"}
    except Exception as e:
        logger.exception("ETL upload_id=%s: %s", upload_id, e)
        err_msg = str(e)
        if len(err_msg) > 500:
            idx = max(err_msg.find("ERRO:"), err_msg.find("viola"), 0)
            err_msg = err_msg[idx:idx + 480] + "…"
        _atualizar_status(upload_id, "erro", erro=err_msg, usuario=usuario)
        raise


def validar_envio_email() -> dict:
    """
    Valida se há dados suficientes para enviar e-mail.
    Obrigatório: ocorrencias e ordens_servico com pelo menos 1 registro cada.
    Retorna: { "pode_enviar": bool, "mensagem": str, "faltando": list }
    """
    eng = _engine()
    faltando = []
    try:
        with eng.connect() as conn:
            r = conn.execute(text("SELECT COUNT(*) FROM ocorrencias"))
            if (r.scalar() or 0) < 1:
                faltando.append("ocorrencias")
            r = conn.execute(text("SELECT COUNT(*) FROM ordens_servico"))
            if (r.scalar() or 0) < 1:
                faltando.append("os")
    except Exception:
        return {"pode_enviar": False, "mensagem": "Erro ao validar banco.", "faltando": ["ocorrencias", "os"]}
    if not faltando:
        return {"pode_enviar": True, "mensagem": "", "faltando": []}
    msgs = []
    if "ocorrencias" in faltando:
        msgs.append("upload e processamento da planilha de ocorrências")
    if "os" in faltando:
        msgs.append("upload e processamento da planilha de OS")
    return {
        "pode_enviar": False,
        "mensagem": f"Para enviar e-mail é obrigatório fazer {', '.join(msgs)}.",
        "faltando": faltando,
    }


def _ids_em_lotes() -> tuple[set[str], set[str]]:
    """Retorna (ids_ocorr_em_lote, ids_os_em_lote)."""
    _garantir_tabela_lotes()
    eng = _engine()
    with eng.connect() as conn:
        r = conn.execute(text("SELECT upl_id_ocorrencias, upl_id_os FROM lotes"))
        rows = r.fetchall()
    ids_ocorr = {str(row[0]) for row in rows}
    ids_os = {str(row[1]) for row in rows}
    return ids_ocorr, ids_os


def listar_uploads_pendentes() -> dict:
    """
    Retorna uploads ainda não vinculados a nenhum lote.
    { "ocorrencias": [...], "os": [...] }
    """
    _garantir_tabela_lotes()
    _garantir_coluna_upl_tipo()
    ids_ocorr, ids_os = _ids_em_lotes()
    all_items = listar_uploads(limite=100)
    ocorr = [u for u in all_items if (u.get("tipo") or "").lower() == "ocorrencias" and u["id"] not in ids_ocorr]
    os_list = [u for u in all_items if (u.get("tipo") or "").lower() == "os" and u["id"] not in ids_os]
    return {"ocorrencias": ocorr, "os": os_list}


def criar_lote(upl_id_ocorrencias: str, upl_id_os: str) -> dict:
    """
    Cria lote vinculando os dois uploads, sem processar.
    Valida se os uploads existem e são dos tipos corretos.
    """
    _garantir_tabela_lotes()
    u_ocorr = obter_upload(upl_id_ocorrencias)
    u_os = obter_upload(upl_id_os)
    if not u_ocorr or not u_os:
        raise ValueError("Upload não encontrado.")
    if (u_ocorr.get("tipo") or "").lower() != "ocorrencias":
        raise ValueError("O primeiro upload deve ser de ocorrências.")
    if (u_os.get("tipo") or "").lower() != "os":
        raise ValueError("O segundo upload deve ser de Ordem de Serviço.")
    lot_id = str(uuid.uuid4())
    eng = _engine()
    with eng.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO lotes (lot_id, upl_id_ocorrencias, upl_id_os, lot_data_processamento)
                VALUES (:id, :ocorr, :os, NULL)
            """),
            {"id": lot_id, "ocorr": upl_id_ocorrencias, "os": upl_id_os},
        )
        conn.commit()
    return {"lot_id": lot_id, "upl_id_ocorrencias": upl_id_ocorrencias, "upl_id_os": upl_id_os}


def processar_lote_por_id(lot_id: str, usuario: str = "sistema") -> dict:
    """Processa um lote específico (ocorrências + OS)."""
    _garantir_tabela_lotes()
    eng = _engine()
    with eng.connect() as conn:
        r = conn.execute(
            text("SELECT upl_id_ocorrencias, upl_id_os FROM lotes WHERE lot_id = :id"),
            {"id": lot_id},
        )
        row = r.fetchone()
    if not row:
        raise ValueError("Lote não encontrado.")
    upl_ocorr, upl_os = str(row[0]), str(row[1])
    res_ocorr = processar_upload(upl_ocorr, truncar_antes=True, usuario=usuario)
    u_os = obter_upload(upl_os)
    caminho_os = _get_uploads_dir() / u_os["caminho_armazenado"]
    if not caminho_os.exists():
        raise FileNotFoundError(f"Arquivo OS não encontrado: {caminho_os}")
    ok, faltando = validar_os_contra_ocorrencias(caminho_os)
    if not ok:
        amostra = faltando[:15]
        suf = f" (e mais {len(faltando) - 15})" if len(faltando) > 15 else ""
        raise ValueError(
            f"Inconsistência: {len(faltando)} OS não encontradas na planilha de ocorrências: "
            f"{amostra}{suf}. Verifique se os arquivos são do mesmo período."
        )
    res_os = processar_upload_os(upl_os, truncar_antes=True, usuario=usuario)
    with eng.connect() as conn:
        conn.execute(
            text("""
                UPDATE lotes SET lot_data_processamento = CURRENT_TIMESTAMP WHERE lot_id = :id
            """),
            {"id": lot_id},
        )
        conn.commit()
    return {
        "lot_id": lot_id,
        "total_ocorrencias": res_ocorr["total_registros"],
        "total_os": res_os["total_registros"],
    }


def enviar_emails_por_lote(lot_id: str) -> dict:
    """Envia relatórios por e-mail e atualiza lot_data_envio_email do lote."""
    val = validar_envio_email()
    if not val["pode_enviar"]:
        raise ValueError(val["mensagem"])
    result = executar_relatorios()
    _garantir_tabela_lotes()
    eng = _engine()
    with eng.connect() as conn:
        conn.execute(
            text("UPDATE lotes SET lot_data_envio_email = CURRENT_TIMESTAMP WHERE lot_id = :id"),
            {"id": lot_id},
        )
        conn.commit()
    return result


def obter_pendentes_para_processar() -> dict | None:
    """
    Retorna o par (ocorr mais recente, os mais recente) pendentes.
    None se faltar algum.
    """
    pend = listar_uploads_pendentes()
    ocorr = pend["ocorrencias"]
    os_list = pend["os"]
    if not ocorr or not os_list:
        return None
    # Mais recente = primeiro (listar_uploads já ordena por create_at DESC)
    return {"upl_id_ocorrencias": ocorr[0]["id"], "upl_id_os": os_list[0]["id"]}


def processar_lote(usuario: str = "sistema") -> dict:
    """
    Processa o par pendente (ocorrências + OS) e cria registro em lotes.
    Retorna o lote criado.
    """
    par = obter_pendentes_para_processar()
    if not par:
        raise ValueError("É necessário ter upload de ocorrências e de OS pendentes para processar.")
    upl_ocorr = par["upl_id_ocorrencias"]
    upl_os = par["upl_id_os"]
    _garantir_tabela_lotes()
    logger.info("Processar lote: upl_ocorr=%s, upl_os=%s", upl_ocorr, upl_os)
    # 1. Processar ocorrências (primeiro, para tip_id na OS)
    logger.info("Processando ocorrências...")
    res_ocorr = processar_upload(upl_ocorr, truncar_antes=True, usuario=usuario)
    logger.info("Ocorrências processadas: %s registros", res_ocorr["total_registros"])
    # 2. Validar consistência: todas as OS devem existir em ocorrencias
    u_os = obter_upload(upl_os)
    caminho_os = _get_uploads_dir() / u_os["caminho_armazenado"]
    if not caminho_os.exists():
        raise FileNotFoundError(f"Arquivo OS não encontrado: {caminho_os}")
    ok, faltando = validar_os_contra_ocorrencias(caminho_os)
    if not ok:
        amostra = faltando[:15]
        suf = f" (e mais {len(faltando) - 15})" if len(faltando) > 15 else ""
        raise ValueError(
            f"Inconsistência: {len(faltando)} OS não encontradas na planilha de ocorrências: "
            f"{amostra}{suf}. Verifique se os arquivos são do mesmo período."
        )
    # 3. Processar OS
    logger.info("Processando OS...")
    res_os = processar_upload_os(upl_os, truncar_antes=True, usuario=usuario)
    # 4. Criar lote
    eng = _engine()
    import uuid
    lot_id = str(uuid.uuid4())
    with eng.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO lotes (lot_id, upl_id_ocorrencias, upl_id_os, lot_data_processamento)
                VALUES (:id, :ocorr, :os, CURRENT_TIMESTAMP)
            """),
            {"id": lot_id, "ocorr": upl_ocorr, "os": upl_os},
        )
        conn.commit()
    return {
        "lot_id": lot_id,
        "total_ocorrencias": res_ocorr["total_registros"],
        "total_os": res_os["total_registros"],
        "upl_id_ocorrencias": upl_ocorr,
        "upl_id_os": upl_os,
    }


def listar_lotes(limite: int = 50) -> list[dict]:
    """Lista lotes com dados dos uploads."""
    _garantir_tabela_lotes()
    eng = _engine()
    with eng.connect() as conn:
        r = conn.execute(
            text("""
                SELECT l.lot_id, l.upl_id_ocorrencias, l.upl_id_os,
                       l.lot_data_processamento, l.lot_data_envio_email,
                       o.upl_nomearquivo, os_f.upl_nomearquivo
                FROM lotes l
                JOIN uploads_planilha o ON o.upl_id = l.upl_id_ocorrencias
                JOIN uploads_planilha os_f ON os_f.upl_id = l.upl_id_os
                ORDER BY COALESCE(l.lot_data_processamento, l.create_at) DESC
                LIMIT :lim
            """),
            {"lim": limite},
        )
        rows = r.fetchall()
    result = []
    for row in rows:
        result.append({
            "id": str(row[0]),
            "upl_id_ocorrencias": str(row[1]),
            "upl_id_os": str(row[2]),
            "data_processamento": row[3].isoformat() if row[3] else None,
            "data_envio_email": row[4].isoformat() if row[4] else None,
            "nome_ocorrencias": row[5],
            "nome_os": row[6],
        })
    return result


def excluir_lote(lot_id: str) -> bool:
    """
    Exclui o lote e os dois arquivos (upload + arquivo em disco).
    Retorna True se excluiu, False se não encontrou.
    """
    _garantir_tabela_lotes()
    eng = _engine()
    with eng.connect() as conn:
        r = conn.execute(
            text("SELECT upl_id_ocorrencias, upl_id_os FROM lotes WHERE lot_id = :id"),
            {"id": lot_id},
        )
        row = r.fetchone()
    if not row:
        return False
    upl_ocorr, upl_os = str(row[0]), str(row[1])
    # 1. Excluir lote (libera FK antes de excluir uploads)
    with eng.connect() as conn:
        conn.execute(text("DELETE FROM lotes WHERE lot_id = :id"), {"id": lot_id})
        conn.commit()
    # 2. Excluir os dois uploads (arquivo + registro)
    excluir_upload(upl_ocorr)
    excluir_upload(upl_os)
    return True


def enviar_emails() -> dict:
    """
    Envia relatórios por e-mail com base nos dados já carregados em ocorrencias.
    Obrigatório ter ocorrencias E ordens_servico com dados.
    Atualiza lot_data_envio_email do lote mais recente.
    """
    val = validar_envio_email()
    if not val["pode_enviar"]:
        raise ValueError(val["mensagem"])
    result = executar_relatorios()
    # Atualiza data de envio no lote mais recente
    _garantir_tabela_lotes()
    try:
        eng = _engine()
        with eng.connect() as conn:
            conn.execute(
                text("""
                    UPDATE lotes SET lot_data_envio_email = CURRENT_TIMESTAMP
                    WHERE lot_id = (SELECT lot_id FROM lotes ORDER BY lot_data_processamento DESC LIMIT 1)
                """),
            )
            conn.commit()
    except Exception:
        pass
    return result
