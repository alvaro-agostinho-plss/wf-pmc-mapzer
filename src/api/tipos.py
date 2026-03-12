"""CRUD de tipos de ocorrência."""

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from src.config import DatabaseConfig


def _engine():
    return create_engine(DatabaseConfig().connection_url)


def listar_tipos_sem_setor() -> list[dict]:
    """Lista tipos que não possuem nenhuma associação com setor."""
    eng = _engine()
    with eng.connect() as conn:
        r = conn.execute(text("""
            SELECT t.tip_id, t.tip_nome
            FROM tipos t
            WHERE NOT EXISTS (SELECT 1 FROM setores_tipos st WHERE st.stp_tipid = t.tip_id)
            ORDER BY t.tip_nome
        """))
        rows = r.fetchall()
    return [{"id": row[0], "tip_nome": row[1]} for row in rows]


def listar_tipos_com_multiplos_setores() -> list[dict]:
    """Lista tipos que possuem vínculo com mais de um setor."""
    eng = _engine()
    with eng.connect() as conn:
        r = conn.execute(text("""
            SELECT t.tip_id, t.tip_nome, COUNT(st.stp_setid) AS qtd_setores
            FROM tipos t
            JOIN setores_tipos st ON st.stp_tipid = t.tip_id
            GROUP BY t.tip_id, t.tip_nome
            HAVING COUNT(st.stp_setid) > 1
            ORDER BY t.tip_nome
        """))
        rows = r.fetchall()
    return [{"id": row[0], "tip_nome": row[1], "qtd_setores": row[2]} for row in rows]


def listar_tipos() -> list[dict]:
    """Lista todos os tipos de ocorrência ordenados por nome."""
    eng = _engine()
    with eng.connect() as conn:
        r = conn.execute(text("""
            SELECT tip_id, tip_nome, COALESCE(tip_status, 'ATIVO'), create_at, update_at
            FROM tipos
            ORDER BY tip_nome
        """))
        rows = r.fetchall()
    return [
        {
            "id": row[0],
            "tip_nome": row[1],
            "tip_status": row[2] or "ATIVO",
            "create_at": row[3].isoformat() if row[3] else None,
            "update_at": row[4].isoformat() if row[4] else None,
        }
        for row in rows
    ]


def obter_tipo(tip_id: int) -> dict | None:
    """Retorna tipo por ID ou None."""
    eng = _engine()
    with eng.connect() as conn:
        r = conn.execute(
            text("SELECT tip_id, tip_nome, COALESCE(tip_status, 'ATIVO'), create_at, update_at FROM tipos WHERE tip_id = :id"),
            {"id": tip_id},
        )
        row = r.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "tip_nome": row[1],
        "tip_status": row[2] or "ATIVO",
        "create_at": row[3].isoformat() if row[3] else None,
        "update_at": row[4].isoformat() if row[4] else None,
    }


def criar_tipo(tip_nome: str, usuario: str, tip_status: str = "ATIVO") -> dict:
    """Cria tipo de ocorrência. Retorna o registro criado."""
    try:
        return _criar_tipo_impl(tip_nome, usuario, tip_status)
    except IntegrityError:
        raise ValueError(f"Já existe um tipo com o nome '{tip_nome.strip()}'")


def _criar_tipo_impl(tip_nome: str, usuario: str, tip_status: str = "ATIVO") -> dict:
    st = "ATIVO" if tip_status == "ATIVO" else "INATIVO"
    eng = _engine()
    with eng.connect() as conn:
        r = conn.execute(
            text("""
                INSERT INTO tipos (tip_nome, tip_status, create_by, updated_by)
                VALUES (:nome, :status, :user, :user)
                RETURNING tip_id, tip_nome, tip_status, create_at
            """),
            {"nome": tip_nome.strip(), "status": st, "user": usuario},
        )
        row = r.fetchone()
        conn.commit()
    return {
        "id": row[0],
        "tip_nome": row[1],
        "tip_status": row[2] or "ATIVO",
        "create_at": row[3].isoformat() if row[3] else None,
    }


def atualizar_tipo(tip_id: int, tip_nome: str, usuario: str, tip_status: str = "ATIVO") -> dict | None:
    """Atualiza tipo. Retorna o registro ou None se não existir."""
    try:
        return _atualizar_tipo_impl(tip_id, tip_nome, usuario, tip_status)
    except IntegrityError:
        raise ValueError(f"Já existe um tipo com o nome '{tip_nome.strip()}'")


def _atualizar_tipo_impl(tip_id: int, tip_nome: str, usuario: str, tip_status: str = "ATIVO") -> dict | None:
    st = "ATIVO" if tip_status == "ATIVO" else "INATIVO"
    eng = _engine()
    with eng.connect() as conn:
        r = conn.execute(
            text("""
                UPDATE tipos
                SET tip_nome = :nome, tip_status = :status, updated_by = :user, update_at = CURRENT_TIMESTAMP
                WHERE tip_id = :id
                RETURNING tip_id, tip_nome, tip_status, update_at
            """),
            {"id": tip_id, "nome": tip_nome.strip(), "status": st, "user": usuario},
        )
        row = r.fetchone()
        conn.commit()
    if not row:
        return None
    return {
        "id": row[0],
        "tip_nome": row[1],
        "tip_status": row[2] or "ATIVO",
        "update_at": row[3].isoformat() if row[3] else None,
    }


def excluir_tipo(tip_id: int) -> bool:
    """Exclui tipo. Retorna True se excluiu."""
    eng = _engine()
    with eng.connect() as conn:
        r = conn.execute(text("DELETE FROM tipos WHERE tip_id = :id RETURNING 1"), {"id": tip_id})
        deleted = r.fetchone()
        conn.commit()
    return deleted is not None
