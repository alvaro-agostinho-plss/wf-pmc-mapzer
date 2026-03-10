-- =============================================================================
-- RECRIAÇÃO DA ESTRUTURA NORMALIZADA - PROJETO MAPZER
-- =============================================================================



-- 1. TABELA DE SETORES
CREATE TABLE setores (
    set_id SERIAL PRIMARY KEY,
    set_nome VARCHAR(100) NOT NULL,
    set_email VARCHAR(500) NOT NULL,  -- Múltiplos e-mails separados por ; ou ,
    -- Campos de Auditoria (Obrigatoriamente sem prefixo e com underscores)
    create_by VARCHAR(100),
    updated_by VARCHAR(100),
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. TABELA DE TIPOS DE OCORRÊNCIA (Mestra)
CREATE TABLE tipos (
    tip_id SERIAL PRIMARY KEY,
    tip_nome VARCHAR(100) UNIQUE NOT NULL,
    create_by VARCHAR(100),
    updated_by VARCHAR(100),
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. TABELA DE VÍNCULO (Setor x Tipo) - Prefixo stp_
CREATE TABLE setores_tipos (
    stp_id SERIAL PRIMARY KEY,
    stp_setid INTEGER NOT NULL REFERENCES setores(set_id) ON DELETE CASCADE,
    stp_tipid INTEGER NOT NULL REFERENCES tipos(tip_id) ON DELETE CASCADE,
    create_by VARCHAR(100),
    updated_by VARCHAR(100),
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. TABELA DE OCORRÊNCIAS
CREATE TABLE ocorrencias (
    oco_id SERIAL PRIMARY KEY,
    oco_datahora TIMESTAMP NOT NULL, -- Sem underscore adicional após o prefixo
    oco_numero BIGINT, -- permite duplicata quando mesma ocorrência tem múltiplos tipos
    oco_status VARCHAR(50),
    oco_tipo TEXT,
    tip_id INTEGER REFERENCES tipos(tip_id) ON DELETE SET NULL, -- FK para tipos
    oco_subtipo TEXT,
    oco_bairro VARCHAR(100),
    oco_endereco VARCHAR(255),
    oco_latitude NUMERIC(15, 10),
    oco_longitude NUMERIC(15, 10),
    oco_ordemservico VARCHAR(50), -- Sem underscore adicional
    oco_imagem TEXT,
    create_by VARCHAR(100),
    updated_by VARCHAR(100),
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para garantir performance semanal
CREATE INDEX idx_oco_datahora ON ocorrencias(oco_datahora);
CREATE INDEX idx_oco_bairro ON ocorrencias(oco_bairro);
CREATE INDEX idx_oco_tipid ON ocorrencias(tip_id);
CREATE INDEX idx_tip_nome ON tipos(tip_nome);


CREATE TABLE ordens_servico (
    -- ID do Banco (Auto-incremento)
    ose_id SERIAL PRIMARY KEY,
    
    -- Campos com prefixo 'ose_' (3 letras) e sem underscores adicionais
    ose_numos INTEGER, -- Nova coluna para o ID da planilha
    tip_id INTEGER REFERENCES tipos(tip_id) ON DELETE SET NULL, -- FK para tipos
    ose_data TIMESTAMP,
    ose_status VARCHAR(100),
    ose_statushistorico VARCHAR(100), -- Removido underscore conforme sua regra
    ose_ocorrencias TEXT,
    ose_departamento VARCHAR(255),
    ose_endereco TEXT,

    -- Campos de Auditoria (Obrigatórios, sem prefixo)
    create_by VARCHAR(255),
    updated_by VARCHAR(255),
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE ordens_servico IS 'Tabela de ordens de serviço importadas do Mapzer';

-- Índice único para UPSERT (reprocessar sem duplicar)
CREATE UNIQUE INDEX IF NOT EXISTS idx_ordens_servico_ose_numos 
ON ordens_servico (ose_numos) WHERE ose_numos IS NOT NULL;

-- =============================================================
-- REESCRITA DA TABELA DE UPLOADS - PADRÃO DE NOMENCLATURA
-- =============================================================

CREATE TABLE IF NOT EXISTS uploads_planilha (
    -- ID com prefixo e sem underscore adicional
    upl_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Campos de negócio: prefixo_nomecampo (sem underscores extras)
    upl_nomearquivo VARCHAR(255) NOT NULL,
    upl_caminhoarmazenado VARCHAR(512) NOT NULL,
    upl_tamanhobytes BIGINT,
    upl_totalregistros INT,
    upl_status VARCHAR(50) DEFAULT 'uploaded',
    upl_mensagemerro TEXT,
    upl_tipo VARCHAR(20),
    
    -- Campos de auditoria obrigatórios: sem prefixo e com underscores
    create_by VARCHAR(255) DEFAULT 'sistema',
    updated_by VARCHAR(255) DEFAULT 'sistema',
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Ajuste dos Índices seguindo o novo padrão
CREATE INDEX IF NOT EXISTS idx_upl_status ON uploads_planilha(upl_status);
CREATE INDEX IF NOT EXISTS idx_upl_createat ON uploads_planilha(create_at DESC);

-- Tabela de lotes: vincula ocorrências + OS processados juntos
CREATE TABLE IF NOT EXISTS lotes (
    lot_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    upl_id_ocorrencias UUID NOT NULL REFERENCES uploads_planilha(upl_id),
    upl_id_os UUID NOT NULL REFERENCES uploads_planilha(upl_id),
    lot_data_processamento TIMESTAMP,
    lot_data_envio_email TIMESTAMP,
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_lotes_processamento ON lotes(lot_data_processamento DESC);

-- =============================================================================
-- VIEW: OCORRÊNCIAS COM STATUS (Em Aberto / Em Tratamento / Solucionado)
-- Usar: SELECT * FROM vw_ocorrencias_status WHERE oco_datahora BETWEEN :dt_inicio AND :dt_fim
-- Python faz GROUP BY set_id, set_nome, oco_bairro, tip_nome para gerar relatórios de email.
-- =============================================================================

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
        -- Regra: SEM OS -> Em Aberto; COM OS PENDENTE -> Em Tratamento; COM OS RESOLVIDO -> Solucionado
        -- Prioridade: se qualquer OS vinculada for RESOLVIDO -> Solucionado; senão se PENDENTE -> Em Tratamento
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
    -- Setor obtido pelo tipo: tip_id -> setores_tipos -> setores
    s.set_id,
    s.set_nome,
    s.set_email
FROM ocorrencias_com_status ocs
JOIN tipos t ON t.tip_id = ocs.tip_id
JOIN setores_tipos st ON st.stp_tipid = ocs.tip_id  -- Pelo tipo, busca o setor
JOIN setores s ON s.set_id = st.stp_setid
WHERE ocs.tip_id IS NOT NULL
ORDER BY s.set_id, ocs.oco_bairro, t.tip_nome;

COMMENT ON VIEW vw_ocorrencias_status IS 'Ocorrências com status calculado (EM_ABERTO/EM_TRATAMENTO/SOLUCIONADO) por setor. Uma ocorrência com tipo em múltiplos setores aparece em múltiplas linhas. Filtrar por oco_datahora no SELECT.';

-- =============================================================================
-- SCRIPT DE POPULAÇÃO FINAL - SISTEMA MAPZER
-- =============================================================================

-- 1. LIMPEZA SEGURA (Truncate reiniciando IDs e tratando dependências)
TRUNCATE TABLE setores_tipos, setores, tipos RESTART IDENTITY CASCADE;

-- 2. INSERÇÃO NA TABELA: tipos
-- Populando todos os tipos únicos extraídos do mapeamento
INSERT INTO tipos (tip_nome, create_by, updated_by) VALUES 
('Animais', 'admin', 'admin'),
('Asfalto Irregular', 'admin', 'admin'),
('Bueiro Irregular', 'admin', 'admin'),
('Buraco', 'admin', 'admin'),
('Calçada Irregular', 'admin', 'admin'),
('Coletor Recicláveis', 'admin', 'admin'),
('Comercio', 'admin', 'admin'),
('Cone', 'admin', 'admin'),
('Empoçamento', 'admin', 'admin'),
('Iluminação Acesa', 'admin', 'admin'),
('Lixo Irregular', 'admin', 'admin'),
('Material Comercio Irregular', 'admin', 'admin'),
('Material de Construção', 'admin', 'admin'),
('Mato', 'admin', 'admin'),
('Pichação', 'admin', 'admin'),
('Placa Comercial', 'admin', 'admin'),
('Placa Comercial Irregular', 'admin', 'admin'),
('Placa Trânsito Irregular', 'admin', 'admin'),
('Poste Irregular', 'admin', 'admin'),
('Rachadura', 'admin', 'admin'),
('Reparo', 'admin', 'admin'),
('Rua Irregular', 'admin', 'admin'),
('Sinalizacao Irregular', 'admin', 'admin'), -- Versão sem acento presente no JSON
('Sinalização Inexistente', 'admin', 'admin'),
('Sinalização Irregular', 'admin', 'admin'),
('Tampa Bueiro Irregular', 'admin', 'admin'),
('Terreno Irregular', 'admin', 'admin'),
('Veículo Irregular', 'admin', 'admin'),
('Zona de Atenção', 'admin', 'admin');

-- 3. INSERÇÃO NA TABELA: setores E VÍNCULO EM: setores_tipos
-- Usando blocos anônimos para garantir que o vínculo use o ID correto gerado
DO $$
DECLARE 
    v_setid INTEGER;
BEGIN
    -- 3.1 Agricultura
    INSERT INTO setores (set_nome, set_email, create_by) 
    VALUES ('Agricultura', 'alvaro.agostinho@plss.com.br', 'admin') RETURNING set_id INTO v_setid;
    INSERT INTO setores_tipos (stp_setid, stp_tipid, create_by)
    SELECT v_setid, tip_id, 'admin' FROM tipos WHERE tip_nome IN ('Animais');

    -- 3.2 Departamento de serviços públicos
    INSERT INTO setores (set_nome, set_email, create_by) 
    VALUES ('Departamento de serviços públicos', 'alvaro.agostinho@plss.com.br', 'admin') RETURNING set_id INTO v_setid;
    INSERT INTO setores_tipos (stp_setid, stp_tipid, create_by)
    SELECT v_setid, tip_id, 'admin' FROM tipos WHERE tip_nome IN ('Asfalto Irregular', 'Bueiro Irregular', 'Buraco', 'Calçada Irregular', 'Coletor Recicláveis', 'Comercio', 'Cone', 'Empoçamento', 'Iluminação Acesa', 'Lixo Irregular', 'Material Comercio Irregular', 'Material de Construção', 'Mato', 'Pichação', 'Placa Comercial Irregular', 'Placa Trânsito Irregular', 'Poste Irregular', 'Rachadura', 'Reparo', 'Rua Irregular', 'Sinalização Inexistente', 'Sinalização Irregular', 'Tampa Bueiro Irregular', 'Terreno Irregular', 'Veículo Irregular', 'Zona de Atenção');

    -- 3.3 Fiscalização
    INSERT INTO setores (set_nome, set_email, create_by) 
    VALUES ('Fiscalização', 'alvaro.agostinho@plss.com.br', 'admin') RETURNING set_id INTO v_setid;
    INSERT INTO setores_tipos (stp_setid, stp_tipid, create_by)
    SELECT v_setid, tip_id, 'admin' FROM tipos WHERE tip_nome IN ('Calçada Irregular', 'Comercio', 'Lixo Irregular', 'Material Comercio Irregular', 'Material de Construção', 'Mato', 'Placa Comercial', 'Placa Comercial Irregular', 'Terreno Irregular');

    -- 3.4 Industria, Comercio e turismo
    INSERT INTO setores (set_nome, set_email, create_by) 
    VALUES ('Industria, Comercio e turismo', 'alvaro.agostinho@plss.com.br', 'admin') RETURNING set_id INTO v_setid;
    INSERT INTO setores_tipos (stp_setid, stp_tipid, create_by)
    SELECT v_setid, tip_id, 'admin' FROM tipos WHERE tip_nome IN ('Asfalto Irregular', 'Bueiro Irregular', 'Buraco', 'Calçada Irregular', 'Coletor Recicláveis', 'Comercio', 'Cone', 'Empoçamento', 'Iluminação Acesa', 'Lixo Irregular', 'Material Comercio Irregular', 'Material de Construção', 'Mato', 'Pichação', 'Placa Comercial Irregular', 'Placa Trânsito Irregular', 'Poste Irregular', 'Rachadura', 'Reparo', 'Rua Irregular', 'Sinalização Inexistente', 'Sinalização Irregular', 'Tampa Bueiro Irregular', 'Terreno Irregular', 'Veículo Irregular', 'Zona de Atenção');

    -- 3.5 Meio Ambiente
    INSERT INTO setores (set_nome, set_email, create_by) 
    VALUES ('Meio Ambiente', 'alvaro.agostinho@plss.com.br', 'admin') RETURNING set_id INTO v_setid;
    INSERT INTO setores_tipos (stp_setid, stp_tipid, create_by)
    SELECT v_setid, tip_id, 'admin' FROM tipos WHERE tip_nome IN ('Bueiro Irregular', 'Calçada Irregular', 'Coletor Recicláveis', 'Empoçamento', 'Lixo Irregular', 'Material de Construção', 'Mato', 'Pichação', 'Tampa Bueiro Irregular', 'Terreno Irregular', 'Veículo Irregular', 'Zona de Atenção');

    -- 3.6 Secretaria de Desenvolvimento Urbano
    INSERT INTO setores (set_nome, set_email, create_by) 
    VALUES ('Secretaria de Desenvolvimento Urbano', 'alvaro.agostinho@plss.com.br', 'admin') RETURNING set_id INTO v_setid;
    INSERT INTO setores_tipos (stp_setid, stp_tipid, create_by)
    SELECT v_setid, tip_id, 'admin' FROM tipos WHERE tip_nome IN ('Asfalto Irregular', 'Buraco', 'Calçada Irregular', 'Lixo Irregular', 'Material Comercio Irregular', 'Material de Construção', 'Mato', 'Placa Comercial', 'Placa Comercial Irregular', 'Tampa Bueiro Irregular', 'Terreno Irregular');

    -- 3.7 Secretaria de Obras
    INSERT INTO setores (set_nome, set_email, create_by) 
    VALUES ('Secretaria de Obras', 'alvaro.agostinho@plss.com.br', 'admin') RETURNING set_id INTO v_setid;
    INSERT INTO setores_tipos (stp_setid, stp_tipid, create_by)
    SELECT v_setid, tip_id, 'admin' FROM tipos WHERE tip_nome IN ('Asfalto Irregular', 'Bueiro Irregular', 'Buraco', 'Coletor Recicláveis', 'Empoçamento', 'Iluminação Acesa', 'Lixo Irregular', 'Rachadura', 'Rua Irregular', 'Sinalização Inexistente', 'Sinalização Irregular', 'Tampa Bueiro Irregular');

    -- 3.8 Segurança Publica
    INSERT INTO setores (set_nome, set_email, create_by) 
    VALUES ('Segurança Publica', 'alvaro.agostinho@plss.com.br', 'admin') RETURNING set_id INTO v_setid;
    INSERT INTO setores_tipos (stp_setid, stp_tipid, create_by)
    SELECT v_setid, tip_id, 'admin' FROM tipos WHERE tip_nome IN ('Placa Trânsito Irregular', 'Sinalização Inexistente', 'Sinalização Irregular', 'Sinalizacao Irregular', 'Veículo Irregular');

    -- 3.9 Tributação
    INSERT INTO setores (set_nome, set_email, create_by) 
    VALUES ('Tributação', 'alvaro.agostinho@plss.com.br', 'admin') RETURNING set_id INTO v_setid;
    INSERT INTO setores_tipos (stp_setid, stp_tipid, create_by)
    SELECT v_setid, tip_id, 'admin' FROM tipos WHERE tip_nome IN ('Material de Construção');

END $$;