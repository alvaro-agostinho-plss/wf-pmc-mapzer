-- Adiciona coluna set_whatsapp na tabela setores e atualiza a view.
-- Números WhatsApp separados por ; (ex: 5542999999999;5542988888888)
-- Uso: psql -f scripts/migrate_set_whatsapp.sql

ALTER TABLE setores ADD COLUMN IF NOT EXISTS set_whatsapp VARCHAR(100);
COMMENT ON COLUMN setores.set_whatsapp IS 'Números WhatsApp para mensagens (separador: ponto e vírgula)';

-- Recria a view para incluir set_whatsapp (necessário para bancos existentes)
CREATE OR REPLACE VIEW vw_ocorrencias_status AS
WITH ocorrencias_com_status AS (
    SELECT
        o.oco_id,
        o.oco_datahora,
        o.oco_numero,
        o.oco_bairro,
        o.oco_latitude,
        o.oco_longitude,
        o.tip_id,
        o.oco_ordemservico,
        CASE
            WHEN o.oco_ordemservico IS NULL OR TRIM(COALESCE(o.oco_ordemservico::TEXT, '')) = ''
            THEN 'EM_ABERTO'
            WHEN EXISTS (
                SELECT 1 FROM ordens_servico os
                WHERE os.ose_numos IS NOT NULL
                  AND o.oco_ordemservico::TEXT LIKE '%' || os.ose_numos::TEXT || '%'
                  AND UPPER(TRIM(COALESCE(os.ose_status, ''))) = 'RESOLVIDO'
            )
            THEN 'SOLUCIONADO'
            WHEN EXISTS (
                SELECT 1 FROM ordens_servico os
                WHERE os.ose_numos IS NOT NULL
                  AND o.oco_ordemservico::TEXT LIKE '%' || os.ose_numos::TEXT || '%'
                  AND UPPER(TRIM(COALESCE(os.ose_status, ''))) = 'PENDENTE'
            )
            THEN 'EM_TRATAMENTO'
            ELSE 'EM_ABERTO'
        END AS status
    FROM ocorrencias o
)
SELECT
    ocs.oco_id,
    ocs.oco_datahora,
    ocs.oco_numero,
    ocs.oco_bairro,
    ocs.oco_latitude,
    ocs.oco_longitude,
    ocs.status,
    ocs.tip_id,
    t.tip_nome,
    s.set_id,
    s.set_nome,
    s.set_email,
    s.set_whatsapp
FROM ocorrencias_com_status ocs
JOIN tipos t ON t.tip_id = ocs.tip_id
JOIN setores_tipos st ON st.stp_tipid = ocs.tip_id
JOIN setores s ON s.set_id = st.stp_setid
WHERE ocs.tip_id IS NOT NULL
ORDER BY s.set_id, ocs.oco_bairro, t.tip_nome;
