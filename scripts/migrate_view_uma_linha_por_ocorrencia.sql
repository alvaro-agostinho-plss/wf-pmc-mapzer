-- Migração: view retorna exatamente a mesma quantidade de linhas que ocorrencias.
-- Uma ocorrência = uma linha. tip_id NULL ou sem setor: set_nome/tip_nome NULL.

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
            WHEN o.oco_ordemservico IS NULL
                 OR TRIM(COALESCE(o.oco_ordemservico::TEXT, '')) = ''
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
),
ocorr_com_setor AS (
    SELECT
        ocs.oco_id,
        ocs.oco_datahora,
        ocs.oco_numero,
        ocs.oco_bairro,
        ocs.oco_latitude,
        ocs.oco_longitude,
        ocs.status,
        ocs.tip_id,
        sub.tip_nome,
        sub.set_id,
        sub.set_nome,
        sub.set_email,
        sub.set_whatsapp
    FROM ocorrencias_com_status ocs
    LEFT JOIN LATERAL (
        SELECT t.tip_nome, s.set_id, s.set_nome, s.set_email, s.set_whatsapp
        FROM tipos t
        JOIN setores_tipos st ON st.stp_tipid = t.tip_id
        JOIN setores s ON s.set_id = st.stp_setid
        WHERE t.tip_id = ocs.tip_id
          AND COALESCE(t.tip_status, 'ATIVO') = 'ATIVO'
          AND COALESCE(s.set_status, 'ATIVO') = 'ATIVO'
        ORDER BY s.set_id
        LIMIT 1
    ) sub ON true
)
SELECT oco_id, oco_datahora, oco_numero, oco_bairro, oco_latitude, oco_longitude,
       status, tip_id, tip_nome, set_id, set_nome, set_email, set_whatsapp
FROM ocorr_com_setor
ORDER BY COALESCE(set_id, 0), oco_bairro, COALESCE(tip_nome, '');
