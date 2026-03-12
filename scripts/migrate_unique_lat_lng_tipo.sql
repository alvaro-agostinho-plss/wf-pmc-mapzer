-- Índice único: evita duplicar ocorrência com mesmo (lat, lng, tip_id).
-- Permite mesmas coordenadas com tipo diferente.
-- Uso: psql -f scripts/migrate_unique_lat_lng_tipo.sql

CREATE UNIQUE INDEX IF NOT EXISTS idx_ocorrencias_uk_lat_lng_tipo
ON ocorrencias (oco_latitude, oco_longitude, tip_id)
WHERE oco_latitude IS NOT NULL AND oco_longitude IS NOT NULL AND tip_id IS NOT NULL;

COMMENT ON INDEX idx_ocorrencias_uk_lat_lng_tipo IS 'Evita duplicar ocorrência: mesmas coordenadas + mesmo tipo';
