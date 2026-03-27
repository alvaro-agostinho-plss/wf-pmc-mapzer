-- env_token + env_resultado (HTML) + env_meta (JSON status). Idempotente.
-- psql -f scripts/migrate_envios_email_token_html.sql

ALTER TABLE envios_email ADD COLUMN IF NOT EXISTS env_token VARCHAR(64);
CREATE UNIQUE INDEX IF NOT EXISTS idx_envios_email_env_token ON envios_email(env_token) WHERE env_token IS NOT NULL;

ALTER TABLE envios_email ADD COLUMN IF NOT EXISTS env_meta JSONB NOT NULL DEFAULT '{}';

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'envios_email'
      AND column_name = 'env_resultado' AND udt_name = 'jsonb'
  ) THEN
    UPDATE envios_email SET env_meta = COALESCE(env_resultado, '{}'::jsonb)
      WHERE env_meta = '{}'::jsonb OR env_meta IS NULL;
    ALTER TABLE envios_email DROP COLUMN env_resultado;
    ALTER TABLE envios_email ADD COLUMN env_resultado TEXT;
  END IF;
END $$;

ALTER TABLE envios_email ADD COLUMN IF NOT EXISTS env_resultado TEXT;

COMMENT ON COLUMN envios_email.env_token IS 'Token único para URL pública do relatório (?token=)';
COMMENT ON COLUMN envios_email.env_resultado IS 'HTML completo do relatório (mesmo conteúdo enviado por e-mail)';
COMMENT ON COLUMN envios_email.env_meta IS 'JSON: status por setor, WhatsApp, etc.';

ALTER TABLE envios_email ADD COLUMN IF NOT EXISTS env_destinatario VARCHAR(512);
ALTER TABLE envios_email ADD COLUMN IF NOT EXISTS env_expires_at TIMESTAMPTZ;
