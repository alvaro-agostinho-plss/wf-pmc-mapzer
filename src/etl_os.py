"""ETL para planilhas de Ordem de Serviço (Mapzer)."""

import logging
import re
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.config import DatabaseConfig, normalizar_tipo
from src.etl import ler_excel, obter_engine, _tabela_existe

logger = logging.getLogger("mapzer")

# Planilha OS: cabeçalhos Id, Data, Status... (linha varia 11-13)
SHEET_OS = "Ordem de Serviço"

# Mapeamento camelCase da planilha -> tip_nome na tabela tipos
MAPEAMENTO_OCORRENCIA_TIPO = {
    "rachadura": "Rachadura",
    "lixoirregular": "Lixo Irregular",
    "matoalto": "Mato",
    "terrenomato": "Terreno Irregular",
    "sinalizacaoirregular": "Sinalização Irregular",
    "sinalizacao inexistente": "Sinalização Inexistente",
}


def _tem_tipo_na_ocorrencias(val: str) -> bool:
    """Retorna True se o valor da coluna Ocorrências indica um tipo (ex: Fale156, Fale156: -, rachadura)."""
    return _extrair_tipo_ocorrencia(val) is not None


def _extrair_tipo_ocorrencia(val: str) -> str | None:
    """
    Extrai tipo único do campo Ocorrências.
    - camelCase (rachadura, lixoIrregular) -> mapeia para tip_nome
    - "Fale156: -" ou "Fale156" -> retorna "Fale156" para lookup LIKE
    - "X ocorrências" (múltiplas) -> retorna None (tip_id NULL)
    """
    if pd.isna(val) or not str(val).strip():
        return None
    s = str(val).strip()
    # Múltiplas ocorrências: "5 ocorrências", "2 ocorrência"
    if re.search(r"^\d+\s*ocorr[eê]ncias?$", s, re.I):
        return None
    # Formato "Fale156: -" ou "Fale156: algo" -> parte antes do ":"
    if ":" in s:
        parte = s.split(":")[0].strip()
        if parte:
            # Primeiro tenta mapeamento; senão retorna a parte para LIKE
            key = normalizar_tipo(parte).replace(" ", "")
            return MAPEAMENTO_OCORRENCIA_TIPO.get(key) or parte
    # camelCase -> chave para lookup
    key = normalizar_tipo(s).replace(" ", "")
    return MAPEAMENTO_OCORRENCIA_TIPO.get(key) or (s if s else None)


def _resolver_tip_id_por_ocorrencia(ose_numos: int | float, engine) -> int | None:
    """
    Busca tip_id em ocorrencias pela relação com a OS.
    Tenta: 1) oco_ordemservico LIKE '%num%'  2) oco_numero = ose_numos.
    """
    if ose_numos is None or (isinstance(ose_numos, float) and pd.isna(ose_numos)):
        return None
    try:
        num_int = int(float(ose_numos))
    except (ValueError, TypeError):
        return None
    num_str = str(num_int)
    if not num_str:
        return None
    try:
        with engine.connect() as conn:
            # 1) Por oco_ordemservico: LIKE '%num%'
            r = conn.execute(
                text("""
                    SELECT tip_id FROM ocorrencias
                    WHERE tip_id IS NOT NULL AND oco_ordemservico IS NOT NULL
                      AND oco_ordemservico::TEXT LIKE :num_like
                    LIMIT 1
                """),
                {"num_like": f"%{num_str}%"},
            )
            row = r.fetchone()
            if row:
                return row[0]
            # 2) Fallback: oco_numero = ose_numos
            r = conn.execute(
                text("""
                    SELECT tip_id FROM ocorrencias
                    WHERE tip_id IS NOT NULL AND oco_numero = :num_int
                    LIMIT 1
                """),
                {"num_int": num_int},
            )
            row = r.fetchone()
            if row:
                return row[0]
            _log_debug_tip_id(conn, num_str, num_int)  # sempre loga quando não encontra
        return None
    except SQLAlchemyError as e:
        logger.debug("Busca tip_id por ocorrencia (ose_numos=%s): %s", num_str, e)
        return None


_debug_tip_id_logged = False


def _log_debug_tip_id(conn, num_str: str, num_int: int) -> None:
    """Log diagnóstico quando tip_id não é encontrado (amostra só na primeira vez)."""
    global _debug_tip_id_logged
    try:
        r = conn.execute(text("SELECT COUNT(*) FROM ocorrencias"))
        total = r.scalar() or 0
        r = conn.execute(text("SELECT COUNT(*) FROM ocorrencias WHERE oco_ordemservico IS NOT NULL"))
        com_ordem = r.scalar() or 0
        r = conn.execute(text("SELECT COUNT(*) FROM ocorrencias WHERE tip_id IS NOT NULL"))
        com_tip = r.scalar() or 0
        msg = f"tip_id NÃO encontrado | ose_numos={num_str} | ocorrencias: total={total}, com oco_ordemservico={com_ordem}, com tip_id={com_tip}"
        if not _debug_tip_id_logged:
            _debug_tip_id_logged = True
            r = conn.execute(text("""
                SELECT oco_ordemservico, oco_numero, tip_id FROM ocorrencias
                ORDER BY oco_id DESC LIMIT 10
            """))
            rows = r.fetchall()
            amostra = [{"oco_ordemservico": repr(row[0]), "oco_numero": row[1], "tip_id": row[2]} for row in rows]
            msg += f" | amostra(10 últimos)={amostra}"
        logger.warning("DEBUG tip_id: %s", msg)
    except Exception as e:
        logger.warning("DEBUG tip_id: %s", e)


def diagnosticar_tip_id(config=None) -> dict:
    """
    Retorna diagnóstico para debug de tip_id: contagens e amostras de ocorrencias/ordens_servico.
    """
    from src.etl import obter_engine
    engine = obter_engine(config)
    out = {}
    try:
        with engine.connect() as conn:
            r = conn.execute(text("SELECT COUNT(*) FROM ocorrencias"))
            out["ocorrencias_total"] = r.scalar() or 0
            r = conn.execute(text("SELECT COUNT(*) FROM ocorrencias WHERE oco_ordemservico IS NOT NULL AND oco_ordemservico != ''"))
            out["ocorrencias_com_oco_ordemservico"] = r.scalar() or 0
            r = conn.execute(text("SELECT COUNT(*) FROM ocorrencias WHERE tip_id IS NOT NULL"))
            out["ocorrencias_com_tip_id"] = r.scalar() or 0
            r = conn.execute(text("""
                SELECT oco_id, oco_ordemservico, oco_numero, tip_id FROM ocorrencias
                ORDER BY oco_id DESC LIMIT 15
            """))
            out["ocorrencias_amostra"] = [
                {"oco_id": row[0], "oco_ordemservico": repr(row[1]), "oco_numero": row[2], "tip_id": row[3]}
                for row in r.fetchall()
            ]
            r = conn.execute(text("SELECT ose_numos, tip_id FROM ordens_servico ORDER BY ose_id DESC LIMIT 15"))
            out["ordens_servico_amostra"] = [{"ose_numos": row[0], "tip_id": row[1]} for row in r.fetchall()]
    except Exception as e:
        out["erro"] = str(e)
    return out


def _resolver_tip_id_os(val_ocorrencias: str, ose_numos: int | float | None, engine) -> int | None:
    """
    Resolve tip_id para ordens_servico.
    1) Busca em ocorrencias: oco_ordemservico = ose_numos (relação OS <-> ocorrência)
    2) Fallback: mapeamento da coluna Ocorrências da planilha.
    """
    # 1. Prioridade: buscar em ocorrencias pela relação oco_ordemservico = ose_numos
    tip_id = _resolver_tip_id_por_ocorrencia(ose_numos, engine)
    if tip_id is not None:
        return tip_id
    # 2. Fallback: mapeamento coluna Ocorrências (rachadura, lixoIrregular, Fale156, Fale156: -, etc.)
    tip_nome = _extrair_tipo_ocorrencia(val_ocorrencias)
    if tip_nome:
        try:
            with engine.connect() as conn:
                # 2a) Match exato
                r = conn.execute(
                    text("SELECT tip_id FROM tipos WHERE tip_nome = :nome"),
                    {"nome": tip_nome},
                )
                row = r.fetchone()
                if row:
                    return row[0]
                # 2b) LIKE no tipo (ex: Fale156 -> tip_nome ILIKE '%Fale156%')
                r = conn.execute(
                    text("SELECT tip_id FROM tipos WHERE tip_nome ILIKE :pattern LIMIT 1"),
                    {"pattern": f"%{tip_nome}%"},
                )
                row = r.fetchone()
                if row:
                    return row[0]
        except SQLAlchemyError:
            pass
    return None


def _detectar_linha_cabecalho(path: Path, sheet: str | int) -> int:
    """Encontra a linha (0-indexed) que contém Id e Data como cabeçalhos."""
    df_raw = pd.read_excel(path, sheet_name=sheet, header=None)
    for row_idx in range(min(15, len(df_raw))):
        row = df_raw.iloc[row_idx]
        vals = [str(c).strip().lower() for c in row.values if pd.notna(c)]
        if any("id" in v and "tip" not in v for v in vals) and any("data" in v for v in vals):
            return row_idx
    return 12  # fallback


def ler_planilha_os(caminho: str | Path) -> pd.DataFrame:
    """
    Lê planilha Excel de Ordem de Serviço.
    Tenta aba 'Ordem de Serviço' ou primeira aba. Detecta linha de cabeçalho automaticamente.
    """
    path = Path(caminho)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    # Tenta nome da aba e índice 0; alguns exports usam nomes levemente diferentes
    xl = pd.ExcelFile(path)
    sheets_to_try = [SHEET_OS, "Ordem de servico", 0]
    for sheet in sheets_to_try:
        if isinstance(sheet, str) and sheet not in xl.sheet_names:
            continue
        try:
            header_row = _detectar_linha_cabecalho(path, sheet)
            df = ler_excel(path, header_row=header_row, sheet_name=sheet)
            cols_lower = [str(c).lower() for c in df.columns]
            if any("id" in c for c in cols_lower) and any("data" in c for c in cols_lower):
                logger.info("Planilha OS: aba=%s, header_row=%s, cols=%s", sheet, header_row, list(df.columns))
                return df
        except Exception as e:
            logger.debug("Tentativa sheet=%s falhou: %s", sheet, e)
            continue
    raise ValueError("Planilha não parece ser de Ordem de Serviço (faltam colunas Id/Data)")


def preparar_ordens_servico(
    df: pd.DataFrame,
    config: DatabaseConfig | None = None,
    usuario: str = "sistema",
) -> pd.DataFrame:
    """
    Prepara DataFrame para inserção em ordens_servico.
    Mapeia: Id->ose_numos, Data->ose_data, Status->ose_status, etc.
    """
    df = df.copy()
    # Normalizar nomes das colunas (Id, Data, Status, Status Histórico, Ocorrências, Departamento, Endereço)
    rename_map = {}
    for c in df.columns:
        cstr = str(c).strip()
        low = cstr.lower()
        if "hist" in low or "histórico" in low:
            rename_map[c] = "ose_statushistorico"
        elif (low == "id" or (low.startswith("id") and "tip" not in low)) and "ose_numos" not in rename_map.values():
            rename_map[c] = "ose_numos"
        elif "data" in low and "hora" not in low and "ose_data" not in rename_map.values():
            rename_map[c] = "ose_data"
        elif "status" in low and "ose_status" not in rename_map.values():
            rename_map[c] = "ose_status"
        elif "ocorr" in low:
            rename_map[c] = "ose_ocorrencias"
        elif "depart" in low:
            rename_map[c] = "ose_departamento"
        elif "endere" in low:
            rename_map[c] = "ose_endereco"
    df = df.rename(columns=rename_map)
    # Fallback: primeira coluna numérica como ose_numos se não mapeamos Id
    if "ose_numos" not in df.columns and len(df.columns) > 0:
        first_col = df.columns[0]
        if pd.api.types.is_numeric_dtype(df[first_col]):
            df = df.rename(columns={first_col: "ose_numos"})

    # Limpar strings
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].apply(
            lambda x: None if pd.isna(x) or not str(x).strip() else str(x).strip()
        )

    # Converter data
    if "ose_data" in df.columns:
        df["ose_data"] = pd.to_datetime(df["ose_data"], errors="coerce")

    # ose_numos: garantir numérico (Excel pode trazer float ou string)
    if "ose_numos" in df.columns:
        df["ose_numos"] = pd.to_numeric(df["ose_numos"], errors="coerce")

    # Resolver tip_id: 1) mapeamento Ocorrências; 2) fallback em ocorrencias (oco_ordemservico = ose_numos)
    engine = obter_engine(config)
    col_ocorr = "ose_ocorrencias" if "ose_ocorrencias" in df.columns else None
    col_numos = "ose_numos" if "ose_numos" in df.columns else None
    if col_ocorr or col_numos:
        def _resolver(row):
            v = row[col_ocorr] if col_ocorr else None
            num = row[col_numos] if col_numos else None
            return _resolver_tip_id_os(v, num, engine)
        df["tip_id"] = df.apply(_resolver, axis=1)
    else:
        df["tip_id"] = None

    # Colunas finais + auditoria
    now = datetime.utcnow()
    df["create_by"] = usuario
    df["updated_by"] = usuario
    df["create_at"] = now
    df["update_at"] = now
    colunas_finais = [
        "ose_numos", "tip_id", "ose_data", "ose_status", "ose_statushistorico",
        "ose_ocorrencias", "ose_departamento", "ose_endereco",
        "create_by", "updated_by", "create_at", "update_at",
    ]
    for c in colunas_finais:
        if c not in df.columns:
            df[c] = None
    return df[[c for c in colunas_finais if c in df.columns]]


def _criar_indice_unique_ose_numos(engine):
    """Garante índice único para UPSERT em ordens_servico."""
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_ordens_servico_ose_numos
            ON ordens_servico (ose_numos) WHERE ose_numos IS NOT NULL
        """))
        conn.commit()


def persistir_ordens_servico(
    df: pd.DataFrame,
    config: DatabaseConfig | None = None,
    truncar_antes: bool = False,
    lot_id: uuid.UUID | str | None = None,
) -> int:
    """
    Persiste DataFrame na tabela ordens_servico.
    - truncar_antes=True: substitui toda a tabela.
    - truncar_antes=False: UPSERT por ose_numos (atualiza se existe, insere se não).
    - lot_id: vincula registros ao lote (ON DELETE CASCADE ao excluir lote).
    """
    config = config or DatabaseConfig()
    engine = obter_engine(config)
    df = df.copy()

    if lot_id is not None:
        df["lot_id"] = str(lot_id) if isinstance(lot_id, uuid.UUID) else lot_id

    if len(df) == 0:
        logger.warning("DataFrame vazio - nada a persistir em ordens_servico")
        return 0

    for col in df.select_dtypes(include=["datetime64"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    cols = [c for c in df.columns if c in [
        "ose_numos", "tip_id", "ose_data", "ose_status", "ose_statushistorico",
        "ose_ocorrencias", "ose_departamento", "ose_endereco",
        "create_by", "updated_by", "create_at", "update_at", "lot_id",
    ]]
    df = df[[c for c in cols if c in df.columns]]
    for col in df.select_dtypes(include=["datetime64"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    try:
        if not _tabela_existe(engine, "ordens_servico"):
            df.to_sql("ordens_servico", engine, if_exists="append", index=False, method="multi", chunksize=500)
            return len(df)

        with engine.connect() as conn:
            if truncar_antes:
                conn.execute(text("TRUNCATE TABLE ordens_servico RESTART IDENTITY CASCADE"))
                conn.commit()
                df.to_sql("ordens_servico", engine, if_exists="append", index=False, method="multi", chunksize=500)
                return len(df)

            # UPSERT: 1 operação atômica, mais performático que DELETE + INSERT
            _criar_indice_unique_ose_numos(engine)
            upd_cols = [c for c in cols if c != "ose_numos"]
            upd_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in upd_cols)
            stmt = text(f"""
                INSERT INTO ordens_servico ({", ".join(cols)})
                VALUES ({", ".join([f":{c}" for c in cols])})
                ON CONFLICT (ose_numos) WHERE ose_numos IS NOT NULL DO UPDATE SET {upd_str}
            """)
            for _, row in df.iterrows():
                params = {c: (None if pd.isna(row[c]) else row[c]) for c in cols}
                conn.execute(stmt, params)
            conn.commit()
        return len(df)
    except SQLAlchemyError as e:
        logger.exception("Persistir ordens de serviço: %s", e)
        raise ConnectionError(f"Erro ao persistir no banco: {e}") from e


def validar_os_contra_ocorrencias(
    caminho_os: str | Path,
    config: DatabaseConfig | None = None,
) -> tuple[bool, list[int]]:
    """
    Verifica se todos os ose_numos da planilha OS existem na tabela ocorrencias.
    Um OS é considerado encontrado se: oco_ordemservico LIKE '%num%' OU oco_numero = num.
    Se a OS NÃO está em ocorrencias mas a coluna Ocorrências tem um tipo (ex: Fale156: -),
    permite a importação (não adiciona a faltando).
    Retorna (ok, lista de ose_numos NÃO encontrados).
    """
    config = config or DatabaseConfig()
    engine = obter_engine(config)
    df = ler_planilha_os(caminho_os)
    df = preparar_ordens_servico(df)
    if "ose_numos" not in df.columns or len(df) == 0:
        return True, []
    # Mapa: ose_numos -> True se alguma linha tem tipo na coluna Ocorrências
    col_ocorr = "ose_ocorrencias" if "ose_ocorrencias" in df.columns else None
    nums_com_tipo = set()
    if col_ocorr:
        for _, row in df.iterrows():
            try:
                num = int(float(row["ose_numos"]))
            except (ValueError, TypeError):
                continue
            if _tem_tipo_na_ocorrencias(row.get(col_ocorr)):
                nums_com_tipo.add(num)
    nums = []
    for x in df["ose_numos"].dropna().unique():
        try:
            nums.append(int(float(x)))
        except (ValueError, TypeError):
            continue
    nums = list(dict.fromkeys(nums))
    if not nums:
        return True, []
    faltando = []
    for num in nums:
        num_str = str(num)
        try:
            with engine.connect() as conn:
                r = conn.execute(
                    text("""
                        SELECT 1 FROM ocorrencias
                        WHERE (oco_ordemservico IS NOT NULL AND oco_ordemservico::TEXT LIKE :like)
                           OR oco_numero = :num
                        LIMIT 1
                    """),
                    {"like": f"%{num_str}%", "num": num},
                )
                if r.fetchone() is None:
                    if num not in nums_com_tipo:
                        faltando.append(num)
        except SQLAlchemyError as e:
            logger.debug("validar_os_contra_ocorrencias num=%s: %s", num, e)
            if num not in nums_com_tipo:
                faltando.append(num)
    return len(faltando) == 0, faltando


def executar_etl_os(
    caminho_excel: str | Path,
    truncar_antes: bool = False,
    usuario: str = "sistema",
    lot_id: uuid.UUID | str | None = None,
) -> int:
    """
    Fluxo completo: ler Excel OS -> preparar -> persistir em ordens_servico.
    Retorna quantidade de registros inseridos.
    """
    df = ler_planilha_os(caminho_excel)
    df = preparar_ordens_servico(df, usuario=usuario)
    return persistir_ordens_servico(df, truncar_antes=truncar_antes, lot_id=lot_id)
