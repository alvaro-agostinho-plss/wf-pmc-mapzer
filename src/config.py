"""Configuração do sistema via Pydantic e variáveis de ambiente."""

import json
import unicodedata
from pathlib import Path

from pydantic import Field


def normalizar_tipo(tipo: str) -> str:
    """
    Normaliza tipo para busca: lowercase, sem acentos.
    Permite match entre planilha (Sinalizacao) e JSON (Sinalização).
    """
    if not tipo or not str(tipo).strip():
        return ""
    s = unicodedata.normalize("NFD", str(tipo).strip())
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s.lower()
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseConfig(BaseSettings):
    """Configuração do PostgreSQL."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str | None = Field(alias="DATABASE_URL", default=None)
    db_host: str = Field(alias="DB_HOST", default="localhost")
    db_port: int = Field(alias="DB_PORT", default=5432)
    db_name: str = Field(alias="DB_NAME", default="prefeitura_db")
    db_user: str = Field(alias="DB_USER", default="postgres")
    db_password: str = Field(alias="DB_PASSWORD", default="")

    @property
    def connection_url(self) -> str:
        if self.database_url:
            url = self.database_url.strip()
            if url.startswith("postgres://"):
                url = "postgresql://" + url[11:]
            return url
        return (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


class AppConfig(BaseSettings):
    """Configuração geral da aplicação."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    dir_uploads: str | None = Field(alias="DIR_UPLOADS", default=None)
    email_template: str | None = Field(alias="EMAIL_TEMPLATE", default=None)
    municipio: str = Field(alias="MUNICIPIO", default="Castro - PR")

    omitir_sem_localizacao: bool = Field(alias="OMITIR_SEM_LOCALIZACAO", default=False)
    tipo_sem_localizacao: str | None = Field(alias="TIPO_SEM_LOCALIZACAO", default=None)

    @property
    def valores_sem_localizacao(self) -> tuple[str, ...]:
        """Valores que indicam bairro sem localização (lowercase para comparação)."""
        base = ("", "nan", "none", "null")
        if not self.tipo_sem_localizacao or not str(self.tipo_sem_localizacao).strip():
            return base + ("não identificado", "nao identificado", "bairro não identificado", "bairro nao identificado")
        seen = set(base)
        result = list(base)
        for v in str(self.tipo_sem_localizacao).split(","):
            if not v or not v.strip():
                continue
            v_lower = v.strip().lower()
            v_norm = normalizar_tipo(v)
            if v_lower not in seen:
                seen.add(v_lower)
                result.append(v_lower)
            if v_norm and v_norm not in seen:
                seen.add(v_norm)
                result.append(v_norm)
        return tuple(result)

    # Limites de coordenadas para validação no ETL (ocorrências)
    lat_min: float = Field(alias="LAT_MIN", default=-25.0700)
    lat_max: float = Field(alias="LAT_MAX", default=-24.4400)
    lng_min: float = Field(alias="LNG_MIN", default=-50.2500)
    lng_max: float = Field(alias="LNG_MAX", default=-49.6100)
    coord_precision: int = Field(alias="COORD_PRECISION", default=6)


class SMTPConfig(BaseSettings):
    """Configuração SMTP para envio de e-mails."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    smtp_host: str = Field(alias="SMTP_HOST", default="smtp.gmail.com")
    smtp_port: int = Field(alias="SMTP_PORT", default=587)
    smtp_user: str = Field(alias="SMTP_USER", default="")
    smtp_password: str = Field(alias="SMTP_PASSWORD", default="")
    smtp_from: str = Field(alias="SMTP_FROM", default="relatorios@prefeitura.gov.br")
    email_prefeito: str | None = Field(alias="EMAIL_PREFEITO", default=None)
    email_relatorio_total: str | None = Field(alias="EMAIL_RELTORIO_TOTAL", default=None)
    email_copia: str | None = Field(alias="EMAIL_COPIA", default=None)


def carregar_mapeamento_db(config: DatabaseConfig | None = None) -> dict:
    """
    Carrega mapeamento tipo->setor/email das tabelas setores, tipos, setores_tipos.
    Retorna {tipo_normalizado: {"setor": str, "email": str}}
    """
    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import SQLAlchemyError
    try:
        cfg = config or DatabaseConfig()
        engine = create_engine(cfg.connection_url)
        with engine.connect() as conn:
            r = conn.execute(text("""
                SELECT t.tip_nome, s.set_nome, s.set_email
                FROM setores_tipos st
                JOIN setores s ON st.stp_setid = s.set_id
                JOIN tipos t ON st.stp_tipid = t.tip_id
                ORDER BY s.set_id
            """))
            rows = r.fetchall()
        return {normalizar_tipo(r[0]): {"setor": r[1], "email": r[2]} for r in rows}
    except (SQLAlchemyError, ImportError):
        return {}


def carregar_mapeamento(caminho: Path | str | None = None, usar_banco: bool = True) -> dict:
    """
    Carrega mapeamento tipo->setor/email.
    Prioridade: 1) Banco (setores/tipos/setores_tipos) 2) JSON.
    Retorna sempre: {tipo_normalizado: {"setor": str, "email": str}}
    """
    if usar_banco:
        mapeamento = carregar_mapeamento_db()
        if mapeamento:
            return mapeamento

    if caminho is None:
        caminho = Path(__file__).parent.parent / "config" / "mapeamento_setores.json"
    path = Path(caminho)
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        mapeamento = {}
        for item in data:
            setor = item.get("setor", "")
            email = item.get("email", "")
            tipos = item.get("tipos", [])
            for t in tipos:
                tipo = t.get("tipo", str(t)).strip() if isinstance(t, dict) else str(t).strip()
                if tipo:
                    mapeamento[normalizar_tipo(tipo)] = {"setor": setor, "email": email}
        return mapeamento

    if isinstance(data, dict):
        return {normalizar_tipo(k): v for k, v in data.items()}
    return {}
