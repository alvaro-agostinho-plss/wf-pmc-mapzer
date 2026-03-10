"""Módulo ETL: leitura Excel, tratamento e persistência em PostgreSQL."""

import logging
import re
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.config import DatabaseConfig, carregar_mapeamento, normalizar_tipo
from src.models import AUDIT_COLUMNS

logger = logging.getLogger("mapzer")
# Planilha Mapzer: linha 6 = títulos (0-indexed: 5), dados a partir da linha 7
HEADER_ROW_MAPZER = 5


def normalizar_nome_coluna(nome: str) -> str:
    """
    Normaliza nome de coluna para formato oco_nomecampo.
    Remove underscores extras e garante padrão consistente.
    """
    if not nome or not str(nome).strip():
        return f"oco_campo_{uuid.uuid4().hex[:8]}"
    # Lowercase, remove acentos básicos, substitui não-alfanuméricos por underscore
    s = str(nome).strip().lower()
    s = re.sub(r"[àáâãäå]", "a", s)
    s = re.sub(r"[èéêë]", "e", s)
    s = re.sub(r"[ìíîï]", "i", s)
    s = re.sub(r"[òóôõö]", "o", s)
    s = re.sub(r"[ùúûü]", "u", s)
    s = re.sub(r"[ç]", "c", s)
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        return f"oco_campo_{uuid.uuid4().hex[:8]}"
    return f"oco_{s}" if not s.startswith("oco_") else s


def tratar_tipos(df: pd.DataFrame) -> pd.DataFrame:
    """Garante tipos de dados corretos nos DataFrames."""
    for col in df.columns:
        if df[col].dtype == "object":
            # Tenta converter datas
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            except Exception:
                pass
        # Converte NaN para None (SQL NULL)
    return df


def identificar_tipo_planilha(caminho: str | Path) -> str:
    """
    Identifica o tipo da planilha pelo conteúdo dos títulos das colunas.
    Retorna 'ocorrencias' | 'os' | 'desconhecido'.

    Ocorrências: Tipo(s) Ocorrência(s), Bairro, SubTipo, Ocorrência (numero).
    OS: Status Histórico, Departamento, Id, Ocorrências (coluna de tipos).
    """
    path = Path(caminho)
    if not path.exists() or path.suffix.lower() not in (".xlsx", ".xls"):
        return "desconhecido"
    try:
        df_raw = pd.read_excel(path, header=None, sheet_name=0)
        colunas = set()
        for i in range(min(20, len(df_raw))):
            for v in df_raw.iloc[i].values:
                if pd.notna(v) and str(v).strip():
                    c = str(v).strip().lower()
                    colunas.add(c)
        # Normalizar para busca (remove acentos)
        def n(s):
            s = s.lower()
            for a, b in [("áàâã", "a"), ("éèê", "e"), ("íìî", "i"), ("óòôõ", "o"), ("úùû", "u"), ("ç", "c")]:
                for x in a:
                    s = s.replace(x, b)
            return s

        col_norm = {n(c): c for c in colunas}
        # OS: Status Histórico + Departamento (exclusivos da planilha OS)
        if any("status" in k and "historico" in k for k in col_norm):
            return "os"
        if any("departamento" in k for k in col_norm) and any("id" == k or (len(k) <= 3 and "id" in k) for k in col_norm):
            return "os"
        # Ocorrências: Tipo(s) Ocorrência(s) ou SubTipo ou Bairro (exclusivos)
        if any("tipo" in k and "ocorrencia" in k for k in col_norm):
            return "ocorrencias"
        if any("subtipo" in k for k in col_norm):
            return "ocorrencias"
        if any("bairro" in k for k in col_norm) and any("ocorrencia" in k or "numero" in k for k in col_norm):
            return "ocorrencias"
        # Fallback: Id + Data + Departamento -> OS
        if any("departamento" in k for k in col_norm):
            return "os"
        # Fallback: Bairro -> ocorrências
        if any("bairro" in k for k in col_norm):
            return "ocorrencias"
    except Exception as e:
        logger.debug("identificar_tipo_planilha %s: %s", path, e)
    return "desconhecido"


SHEETS_OCORRENCIAS = ["Ocorrências", "Ocorrência", "Ocorrencias", "Ocorrencia", 0]


def _detectar_linha_cabecalho_ocorr(path: Path, sheet: str | int) -> int:
    """Encontra linha (0-indexed) com colunas típicas de ocorrências Mapzer."""
    df_raw = pd.read_excel(path, sheet_name=sheet, header=None, engine="openpyxl")
    for row_idx in range(min(20, len(df_raw))):
        row = df_raw.iloc[row_idx]
        vals = [str(c).strip().lower() for c in row.values if pd.notna(c)]
        # Tipo(s) Ocorrência(s) ou Bairro + Número/Ocorrência
        tem_tipo = any("tipo" in v and "ocorr" in v and "sub" not in v for v in vals)
        tem_bairro = any("bairro" in v for v in vals)
        tem_num = any("numero" in v or "ocorrencia" in v or "n " in v for v in vals)
        if tem_tipo or (tem_bairro and tem_num):
            return row_idx
    return 5  # fallback Mapzer


def ler_planilha_ocorrencias(caminho: str | Path) -> pd.DataFrame:
    """
    Lê planilha Excel de Ocorrências.
    Tenta aba 'Ocorrências' ou primeira aba. Detecta linha de cabeçalho.
    """
    path = Path(caminho)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    xl = pd.ExcelFile(path, engine="openpyxl")
    for sheet in SHEETS_OCORRENCIAS:
        if isinstance(sheet, str) and sheet not in xl.sheet_names:
            continue
        try:
            header_row = _detectar_linha_cabecalho_ocorr(path, sheet)
            df = ler_excel(path, header_row=header_row, sheet_name=sheet)
            cols_lower = [str(c).strip().lower() for c in df.columns]
            # Precisa de Bairro ou Tipo Ocorrência para ser planilha de ocorrências
            tem_bairro = any("bairro" in c for c in cols_lower)
            tem_tipo = any("tipo" in c and "ocorr" in c and "sub" not in c for c in cols_lower)
            if tem_bairro or tem_tipo:
                logger.info(
                    "Planilha Ocorrências: aba=%s, header_row=%s, cols=%s, linhas=%s",
                    sheet, header_row, list(df.columns)[:10], len(df),
                )
                return df
        except Exception as e:
            logger.debug("Tentativa ocorrências sheet=%s falhou: %s", sheet, e)
            continue
    raise ValueError(
        "Planilha não parece ser de Ocorrências (faltam colunas Bairro/Tipo Ocorrência ou Número)"
    )


def ler_excel(
    caminho: str | Path,
    header_row: int | None = None,
    sheet_name: str | int = 0,
) -> pd.DataFrame:
    """
    Lê planilha Excel (.xlsx).
    header_row: linha dos cabeçalhos (0-indexed). None = auto (linha 0).
    Planilhas Mapzer: usar header_row=5 (metadata nas linhas 1-5).
    Raises FileNotFoundError ou PermissionError em falha de leitura.
    """
    path = Path(caminho)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    if path.suffix.lower() not in (".xlsx", ".xls"):
        raise ValueError(f"Formato não suportado: {path.suffix}. Use .xlsx")
    try:
        kwargs = {"engine": "openpyxl", "sheet_name": sheet_name}
        if header_row is not None:
            kwargs["header"] = header_row
        df = pd.read_excel(path, **kwargs)
        if isinstance(df, dict):
            df = df[list(df.keys())[0]]
    except Exception as e:
        raise PermissionError(f"Erro ao ler arquivo Excel: {e}") from e
    return df


def _extrair_tipo_primario(val: str) -> str:
    """
    Extrai o primeiro tipo de campo composto Mapzer.
    Ex: 'Mato=1' -> 'Mato', 'Mato=2, Lixo Irregular=1' -> 'Mato'
    """
    if pd.isna(val) or not str(val).strip():
        return ""
    s = str(val).strip()
    parte = s.split(",")[0].split("=")[0].strip()
    return parte


def _extrair_todos_tipos(val: str) -> list[str]:
    """
    Extrai todos os tipos únicos do campo composto Mapzer.
    Ex: 'Mato=1' -> ['Mato']
        'Mato=2, Lixo Irregular=1' -> ['Mato', 'Lixo Irregular']
    Retorna lista vazia se vazio/inválido.
    """
    if pd.isna(val) or not str(val).strip():
        return []
    s = str(val).strip()
    tipos = []
    for parte in s.split(","):
        nome = parte.split("=")[0].strip()
        if nome and nome not in tipos:
            tipos.append(nome)
    return tipos


def _tipos_por_setor(tipos_lista: list[str], mapa_tipo_setor: dict) -> list[str]:
    """
    Agrupa tipos por setor e retorna um tipo por setor (o primeiro de cada grupo).
    Se todos os tipos são do mesmo setor → 1 elemento.
    Se há tipos de setores diferentes → 1 elemento por setor.
    Ex: ['Mato','Lixo Irregular'] (mesmo setor) -> ['Mato']
        ['Mato','Placa Trânsito Irregular'] (setores diferentes) -> ['Mato', 'Placa Trânsito Irregular']
    """
    if not tipos_lista:
        return []
    vistos = {}
    for tipo in tipos_lista:
        setor = mapa_tipo_setor.get(normalizar_tipo(tipo), {}).get("setor", "Não mapeado")
        if setor not in vistos:
            vistos[setor] = tipo
    return list(vistos.values())


def _limpar_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Remove espaços em branco e trata valores nulos em colunas objeto."""
    for col in df.select_dtypes(include=["object"]).columns:
        def _tratar(x):
            if pd.isna(x):
                return None
            if isinstance(x, str):
                s = x.strip()
                return None if s.lower() in ("nan", "none", "null") else s
            return x
        df[col] = df[col].apply(_tratar)
    return df


def preparar_dataframe(df: pd.DataFrame, usuario: str = "sistema") -> pd.DataFrame:
    """
    Limpa nomes de colunas (formato oco_*), trata strings/nulos e adiciona auditoria.
    Para planilhas Mapzer: extrai oco_tipo do campo 'Tipo(s) Ocorrência(s)'.
    """
    # Renomear colunas
    df = df.copy()
    df.columns = [normalizar_nome_coluna(str(c)) for c in df.columns]
    # Ordem de Serviço -> oco_ordemservico (conforme requisito)
    for c in list(df.columns):
        if "ordem" in c.lower() and "servico" in c.lower() and c != "oco_ordemservico":
            df = df.rename(columns={c: "oco_ordemservico"})
    df = _limpar_strings(df)
    # Tipo(s) Ocorrência(s): 1 registro por tipo (explodir linhas com múltiplos tipos)
    col_tipo = next(
        (c for c in df.columns if "tipo" in c.lower() and "ocorrencia" in c.lower() and "subtipo" not in c.lower()),
        None,
    )
    col_subtipo = next(
        (c for c in df.columns if "subtipo" in c.lower() and "ocorrencia" in c.lower()),
        None,
    )
    if col_tipo:
        # 1 registro por SETOR (só divide quando tipos são de setores diferentes)
        mapa_tipo_setor = carregar_mapeamento()
        df["_tipos_lista"] = df[col_tipo].astype(str).apply(_extrair_todos_tipos)
        df["_tipos_por_setor"] = df["_tipos_lista"].apply(
            lambda lst: _tipos_por_setor(lst, mapa_tipo_setor) if lst else []
        )
        df["_tipos_por_setor"] = df["_tipos_por_setor"].apply(
            lambda lst: lst if lst else [None]
        )
        df = df.explode("_tipos_por_setor", ignore_index=True)
        df["oco_tipo"] = df["_tipos_por_setor"].where(df["_tipos_por_setor"].notna(), "")
        df["oco_tipo_mapeamento"] = df["oco_tipo"]
        df = df.drop(columns=["_tipos_lista", "_tipos_por_setor"])
    else:
        df["oco_tipo"] = ""
        df["oco_tipo_mapeamento"] = ""
    if col_subtipo:
        df["oco_subtipo"] = df[col_subtipo].astype(str).apply(_extrair_tipo_primario)
    df = tratar_tipos(df)
    # Mapear colunas Mapzer -> schema init_db.sql (oco_id é SERIAL, não inserir)
    mapeo_exato = {
        "oco_data_hora": "oco_datahora",
        "oco_data": "oco_datahora",
        "oco_ocorrencia": "oco_numero",
        "oco_numero": "oco_numero",
        "oco_numero_ocorrencia": "oco_numero",
        "oco_numero_da_ocorrencia": "oco_numero",
        "oco_n": "oco_numero",
        "oco_status": "oco_status",
        "oco_status_da_ocorrencia": "oco_status",
        "oco_bairro": "oco_bairro",
        "oco_endereco": "oco_endereco",
        "oco_endereco_aproximado": "oco_endereco",
        "oco_endereco_completo": "oco_endereco",
        "oco_latitude": "oco_latitude",
        "oco_lat": "oco_latitude",
        "oco_longitude": "oco_longitude",
        "oco_lng": "oco_longitude",
        "oco_long": "oco_longitude",
        "oco_ordemservico": "oco_ordemservico",
        "oco_ordem_de_servico": "oco_ordemservico",
        "oco_imagem": "oco_imagem",
        "oco_foto": "oco_imagem",
    }
    for antiga, nova in mapeo_exato.items():
        if antiga in df.columns and antiga != nova:
            df = df.rename(columns={antiga: nova})
    # Mapeamento por substring (colunas que contêm a palavra-chave)
    mapeo_contem = [
        ("numero", "oco_numero"),
        ("status", "oco_status"),
        ("bairro", "oco_bairro"),
        ("endereco", "oco_endereco"),
        ("latitude", "oco_latitude"),
        ("longitude", "oco_longitude"),
        ("ordem", "oco_ordemservico"),
        ("servico", "oco_ordemservico"),
        ("imagem", "oco_imagem"),
        ("foto", "oco_imagem"),
    ]
    for col in list(df.columns):
        if col.startswith("oco_") and col not in mapeo_exato:
            col_low = col.lower()
            for keyword, target in mapeo_contem:
                if keyword in col_low and target not in df.columns and col != target:
                    df = df.rename(columns={col: target})
                    break
    # Campos de auditoria (definir agora para uso no fallback)
    now = datetime.utcnow()
    # oco_datahora NOT NULL: fallback para create_at se ausente
    ts_now = pd.Timestamp(now)
    if "oco_datahora" not in df.columns:
        df["oco_datahora"] = ts_now
    elif df["oco_datahora"].isna().all():
        df["oco_datahora"] = ts_now
    else:
        df["oco_datahora"] = pd.to_datetime(df["oco_datahora"], errors="coerce").fillna(ts_now)
    df["create_by"] = usuario
    df["updated_by"] = usuario
    df["create_at"] = now
    df["update_at"] = now
    # oco_numero: BIGINT - converter de float se vindo do Excel
    if "oco_numero" in df.columns:
        df["oco_numero"] = pd.to_numeric(df["oco_numero"], errors="coerce")
    # tip_id: FK para tipos - resolver a partir de oco_tipo_mapeamento
    if "oco_tipo_mapeamento" in df.columns or "oco_tipo" in df.columns:
        df["tip_id"] = _resolver_tip_ids(df, config=None)
    return df


def _resolver_tip_ids(df: pd.DataFrame, config: DatabaseConfig | None = None) -> pd.Series:
    """
    Resolve tip_id a partir de oco_tipo_mapeamento (ou oco_tipo).
    Retorna Series com tip_id (ou pd.NA se não encontrar).
    """
    col = "oco_tipo_mapeamento" if "oco_tipo_mapeamento" in df.columns else "oco_tipo"
    if col not in df.columns:
        return pd.Series([pd.NA] * len(df))
    engine = obter_engine(config)
    try:
        with engine.connect() as conn:
            r = conn.execute(text("SELECT tip_id, tip_nome FROM tipos"))
            rows = r.fetchall()
        mapa = {normalizar_tipo(nome): tip_id for tip_id, nome in rows}
    except SQLAlchemyError:
        return pd.Series([pd.NA] * len(df))
    def _lookup(val):
        if pd.isna(val) or not str(val).strip():
            return pd.NA
        key = normalizar_tipo(str(val).strip())
        return mapa.get(key, pd.NA)
    return df[col].astype(str).apply(lambda x: _lookup(x))


def obter_engine(config: DatabaseConfig | None = None):
    """Cria engine SQLAlchemy para PostgreSQL."""
    if config is None:
        config = DatabaseConfig()
    return create_engine(config.connection_url)


def _tabela_existe(engine, nome: str = "ocorrencias") -> bool:
    """Verifica se a tabela existe no banco."""
    with engine.connect() as conn:
        r = conn.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_name = :nome)"
            ),
            {"nome": nome},
        )
        return r.scalar()


def persistir_ocorrencias(
    df: pd.DataFrame,
    config: DatabaseConfig | None = None,
    truncar_antes: bool = False,
) -> int:
    """
    Persiste DataFrame na tabela ocorrencias (schema init_db.sql).
    - truncar_antes=True: substitui toda a tabela (evita duplicar em primeiro processamento).
    - truncar_antes=False (reprocessar): remove apenas registros com oco_numero do DF e reinsere (evita duplicar).
    """
    config = config or DatabaseConfig()
    engine = obter_engine(config)
    df = df.copy()
    colunas_esperadas = [
        "oco_datahora", "oco_numero", "oco_status", "oco_tipo", "tip_id", "oco_subtipo",
        "oco_bairro", "oco_endereco", "oco_latitude", "oco_longitude",
        "oco_ordemservico", "oco_imagem", "create_by", "updated_by", "create_at", "update_at",
    ]
    cols = [c for c in colunas_esperadas if c in df.columns]
    df = df[cols]
    for col in df.select_dtypes(include=["datetime64"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    try:
        if _tabela_existe(engine):
            with engine.connect() as conn:
                if truncar_antes:
                    conn.execute(text("TRUNCATE TABLE ocorrencias RESTART IDENTITY CASCADE"))
                    conn.commit()
                elif "oco_numero" in df.columns and len(df) > 0:
                    numeros = [int(x) for x in df["oco_numero"].dropna().unique() if pd.notna(x)]
                    if numeros:
                        stmt = text("DELETE FROM ocorrencias WHERE oco_numero IN :nums").bindparams(
                            bindparam("nums", expanding=True)
                        )
                        conn.execute(stmt, {"nums": numeros})
                        conn.commit()
        df.to_sql(
            "ocorrencias",
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=500,
        )
        return len(df)
    except SQLAlchemyError as e:
        logger.exception("Persistir ocorrências: %s", e)
        raise ConnectionError(f"Erro ao persistir no banco: {e}") from e


def executar_etl(
    caminho_excel: str | Path,
    usuario: str = "sistema",
    truncar_antes: bool = False,
    header_row: int | None = None,
) -> int:
    """
    Fluxo completo: ler Excel -> tratar -> persistir em ocorrencias.
    Detecta aba e linha de cabeçalho automaticamente.
    Colunas mapeadas para tipos (oco_tipo extraído de 'Tipo(s) Ocorrência(s)').
    Retorna quantidade de registros inseridos.
    """
    df = ler_planilha_ocorrencias(caminho_excel)
    df = preparar_dataframe(df, usuario=usuario)
    if len(df) == 0:
        logger.warning("Planilha de ocorrências vazia após preparação - nada a persistir")
        if truncar_antes and _tabela_existe(obter_engine()):
            with obter_engine().connect() as conn:
                conn.execute(text("TRUNCATE TABLE ocorrencias RESTART IDENTITY CASCADE"))
                conn.commit()
        return 0
    return persistir_ocorrencias(df, truncar_antes=truncar_antes)
