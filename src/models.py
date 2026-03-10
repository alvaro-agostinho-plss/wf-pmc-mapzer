"""Modelos SQLAlchemy e tabelas do banco."""

from datetime import datetime

from sqlalchemy import Column, DateTime, MetaData, String, Text
from sqlalchemy.orm import declarative_base

# Campos de auditoria obrigatórios em todas as tabelas
AUDIT_COLUMNS = [
    ("create_by", String(255), "sistema"),
    ("updated_by", String(255), "sistema"),
    ("create_at", DateTime, datetime.utcnow),
    ("update_at", DateTime, datetime.utcnow),
]

metadata = MetaData()
Base = declarative_base(metadata=metadata)
