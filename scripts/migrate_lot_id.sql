-- Migração: adiciona lot_id em ocorrencias e ordens_servico (FK para lotes com ON DELETE CASCADE)
-- Executar apenas se lotes já existir (migrate_lotes.sql aplicado)

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'lotes') THEN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='ocorrencias' AND column_name='lot_id') THEN
      ALTER TABLE ocorrencias ADD COLUMN lot_id UUID REFERENCES lotes(lot_id) ON DELETE CASCADE;
      CREATE INDEX IF NOT EXISTS idx_oco_lot_id ON ocorrencias(lot_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='ordens_servico' AND column_name='lot_id') THEN
      ALTER TABLE ordens_servico ADD COLUMN lot_id UUID REFERENCES lotes(lot_id) ON DELETE CASCADE;
      CREATE INDEX IF NOT EXISTS idx_ose_lot_id ON ordens_servico(lot_id);
    END IF;
  END IF;
END $$;
