"""CRUD de setores."""

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.config import DatabaseConfig


def _engine():
    return create_engine(DatabaseConfig().connection_url)


def listar_setores() -> list[dict]:
    """Lista todos os setores ordenados por nome, com tip_nomes vinculados."""
    eng = _engine()
    with eng.connect() as conn:
        r = conn.execute(text("""
            SELECT s.set_id, s.set_nome, s.set_email, s.set_whatsapp, COALESCE(s.set_status, 'ATIVO'),
                   s.create_at, s.update_at,
                   (SELECT COALESCE(array_agg(t.tip_nome ORDER BY t.tip_nome), ARRAY[]::text[])
                    FROM setores_tipos st JOIN tipos t ON t.tip_id = st.stp_tipid
                    WHERE st.stp_setid = s.set_id) AS tip_nomes
            FROM setores s
            ORDER BY s.set_nome
        """))
        rows = r.fetchall()
    return [
        {
            "id": row[0],
            "set_nome": row[1],
            "set_email": row[2] or "",
            "set_whatsapp": row[3] or "",
            "set_status": row[4] or "ATIVO",
            "create_at": row[5].isoformat() if row[5] else None,
            "update_at": row[6].isoformat() if row[6] else None,
            "tip_nomes": list(row[7]) if row[7] else [],
        }
        for row in rows
    ]


def obter_setor(set_id: int) -> dict | None:
    """Retorna setor por ID ou None, incluindo tip_ids vinculados."""
    eng = _engine()
    with eng.connect() as conn:
        r = conn.execute(
            text("SELECT set_id, set_nome, set_email, set_whatsapp, COALESCE(set_status, 'ATIVO'), create_at, update_at FROM setores WHERE set_id = :id"),
            {"id": set_id},
        )
        row = r.fetchone()
        if not row:
            return None
        rt = conn.execute(
            text("SELECT stp_tipid FROM setores_tipos WHERE stp_setid = :id ORDER BY stp_tipid"),
            {"id": set_id},
        )
        tip_ids = [r[0] for r in rt.fetchall()]
    return {
        "id": row[0],
        "set_nome": row[1],
        "set_email": row[2] or "",
        "set_whatsapp": row[3] or "",
        "set_status": row[4] or "ATIVO",
        "tip_ids": tip_ids,
        "create_at": row[5].isoformat() if row[5] else None,
        "update_at": row[6].isoformat() if row[6] else None,
    }


def _salvar_setores_tipos(conn, set_id: int, tip_ids: list[int], usuario: str) -> None:
    """Remove vínculos existentes e insere os novos."""
    conn.execute(text("DELETE FROM setores_tipos WHERE stp_setid = :id"), {"id": set_id})
    for tip_id in tip_ids or []:
        conn.execute(
            text("INSERT INTO setores_tipos (stp_setid, stp_tipid, create_by, updated_by) VALUES (:setid, :tipid, :user, :user)"),
            {"setid": set_id, "tipid": tip_id, "user": usuario},
        )


def criar_setor(set_nome: str, set_email: str, set_whatsapp: str | None, usuario: str, tip_ids: list[int] | None = None, set_status: str = "ATIVO") -> dict:
    """Cria setor e retorna o registro criado."""
    set_whatsapp = (set_whatsapp or "").strip() or None
    eng = _engine()
    with eng.connect() as conn:
        st = "ATIVO" if set_status == "ATIVO" else "INATIVO"
        r = conn.execute(
            text("""
                INSERT INTO setores (set_nome, set_email, set_whatsapp, set_status, create_by, updated_by)
                VALUES (:nome, :email, :whatsapp, :status, :user, :user)
                RETURNING set_id, set_nome, set_email, set_whatsapp, set_status, create_at
            """),
            {"nome": set_nome.strip(), "email": set_email.strip(), "whatsapp": set_whatsapp, "status": st, "user": usuario},
        )
        row = r.fetchone()
        set_id = row[0]
        _salvar_setores_tipos(conn, set_id, tip_ids or [], usuario)
        conn.commit()
    return {
        "id": row[0],
        "set_nome": row[1],
        "set_email": row[2] or "",
        "set_whatsapp": row[3] or "",
        "set_status": row[4] or "ATIVO",
        "create_at": row[5].isoformat() if row[5] else None,
    }


def atualizar_setor(set_id: int, set_nome: str, set_email: str, set_whatsapp: str | None, usuario: str, tip_ids: list[int] | None = None, set_status: str = "ATIVO") -> dict | None:
    """Atualiza setor. Retorna o registro ou None se não existir."""
    set_whatsapp = (set_whatsapp or "").strip() or None
    eng = _engine()
    with eng.connect() as conn:
        st = "ATIVO" if set_status == "ATIVO" else "INATIVO"
        r = conn.execute(
            text("""
                UPDATE setores
                SET set_nome = :nome, set_email = :email, set_whatsapp = :whatsapp, set_status = :status,
                    updated_by = :user, update_at = CURRENT_TIMESTAMP
                WHERE set_id = :id
                RETURNING set_id, set_nome, set_email, set_whatsapp, set_status, update_at
            """),
            {"id": set_id, "nome": set_nome.strip(), "email": set_email.strip(), "whatsapp": set_whatsapp, "status": st, "user": usuario},
        )
        row = r.fetchone()
        if row:
            _salvar_setores_tipos(conn, set_id, tip_ids or [], usuario)
        conn.commit()
    if not row:
        return None
    return {
        "id": row[0],
        "set_nome": row[1],
        "set_email": row[2] or "",
        "set_whatsapp": row[3] or "",
        "set_status": row[4] or "ATIVO",
        "update_at": row[5].isoformat() if row[5] else None,
    }


def excluir_setor(set_id: int) -> bool:
    """Exclui setor. Retorna True se excluiu."""
    eng = _engine()
    with eng.connect() as conn:
        r = conn.execute(text("DELETE FROM setores WHERE set_id = :id RETURNING 1"), {"id": set_id})
        deleted = r.fetchone()
        conn.commit()
    return deleted is not None
