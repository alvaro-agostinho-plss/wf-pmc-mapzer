"""
Configuração SQLAlchemy e modelos de dados.
Regras: prefixo oco_ em campos de ocorrência, auditoria: create_by, updated_by, create_at, update_at.
"""

from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import Column, DateTime, MetaData, String, Text, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

load_dotenv()

# Importa config após dotenv
from src.config import DatabaseConfig

DB_CONFIG = DatabaseConfig()
DATABASE_URL = DB_CONFIG.connection_url

metadata = MetaData()
Base = declarative_base(metadata=metadata)
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_engine():
    """Retorna engine SQLAlchemy."""
    return engine


def get_session():
    """Retorna sessão para operações de banco."""
    return SessionLocal()


# Campos de auditoria padrão para todas as tabelas
def audit_columns():
    return [
        Column("create_by", String(255), nullable=False, default="sistema"),
        Column("updated_by", String(255), nullable=False, default="sistema"),
        Column("create_at", DateTime, nullable=False, default=datetime.utcnow),
        Column("update_at", DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow),
    ]
