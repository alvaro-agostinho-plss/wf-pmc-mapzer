-- Migration: tabela envios_email para log de envios de e-mail
-- Execute: psql -U postgres -d prefeitura_db -f add_envios_email.sql
-- Ou via SQLAlchemy/text no Python.

CREATE TABLE IF NOT EXISTS envios_email (
    env_id SERIAL PRIMARY KEY,
    lot_id UUID REFERENCES lotes(lot_id) ON DELETE SET NULL,
    env_dt_inicio DATE,
    env_dt_fim DATE,
    env_token VARCHAR(64),
    env_resultado TEXT,
    env_meta JSONB NOT NULL DEFAULT '{}',
    env_destinatario VARCHAR(512),
    env_expires_at TIMESTAMPTZ,
    env_usuario VARCHAR(255),
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_envios_lot_id ON envios_email(lot_id);
CREATE INDEX IF NOT EXISTS idx_envios_create_at ON envios_email(create_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_envios_email_env_token ON envios_email(env_token) WHERE env_token IS NOT NULL;
COMMENT ON TABLE envios_email IS 'Log de envios: HTML público (env_resultado), token (env_token), metadados (env_meta)';
