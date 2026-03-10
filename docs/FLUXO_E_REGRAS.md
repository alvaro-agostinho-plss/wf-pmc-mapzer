# Fluxo e Regras - Sistema ETL Ocorrências Mapzer

## 1. Visão Geral

O sistema importa planilhas Excel do Mapzer (Smart City), processa os dados e persiste na tabela `ocorrencias` do PostgreSQL, com envio de relatórios por e-mail agrupados por Setor → Bairro → Tipo.

---

## 2. Fluxo Principal

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Upload        │     │   Processar     │     │  Enviar E-mail   │
│   .xlsx         │ ──► │   (ETL)         │ ──► │   (Relatórios)   │
│   via Web       │     │   ocorrencias   │     │   por setor      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         │                         │                        │
         ▼                         ▼                        ▼
   uploads_planilha          ocorrencias              SMTP (setores)
   (registro)                (dados)                 + Prefeito/cópia
```

### 2.1 Ações Separadas

| Ação | Descrição |
|------|-----------|
| **Processar** | Executa apenas o ETL (Excel → banco). Pode ser reexecutado para reprocessar. |
| **Enviar e-mail** | Envia relatórios com os dados já carregados. Pode ser clicado novamente para reenvio. |

---

## 3. Estrutura da Planilha Mapzer

### 3.1 Layout

| Linha | Conteúdo |
|-------|----------|
| 1–5 | Metadados |
| 6 | **Cabeçalhos** (títulos das colunas) |
| 7+ | **Dados** |

**Parâmetro:** `header_row=5` (0-indexed)

### 3.2 Colunas Esperadas

| Coluna Excel | Campo BD (ocorrencias) | Observação |
|--------------|------------------------|------------|
| Data/Hora | oco_datahora | NOT NULL; fallback para create_at se vazio |
| Ocorrência | oco_numero | BIGINT |
| Status | oco_status | |
| Tipo(s) Ocorrência(s) | oco_tipo, tip_id | Ver regra de divisão |
| SubTipo(s) Ocorrência(s) | oco_subtipo | |
| Bairro | oco_bairro | |
| Endereço Aproximado | oco_endereco | |
| Latitude / Longitude | oco_latitude, oco_longitude | |
| Ordem de Serviço | oco_ordemservico | |
| Imagem | oco_imagem | |

---

## 4. Regra de Divisão por Setor

### 4.1 Formato do Campo Tipo(s) Ocorrência(s)

O campo é composto: `Tipo1=N, Tipo2=M, ...`

**Exemplos:**
- `Mato=1` → um tipo
- `Mato=2, Lixo Irregular=1` → dois tipos
- `Lixo Irregular=1, Lixo Irregular=1` → um tipo (deduplicado)

### 4.2 Quando Dividir em Múltiplas Linhas

**Só se divide quando os tipos são de SETORES DIFERENTES.**

| Cenário | Registros gerados |
|---------|-------------------|
| Tipos do **mesmo setor** (ex: Mato, Lixo Irregular → Dep. Serviços Públicos) | **1 registro** (usa o primeiro tipo do grupo) |
| Tipos de **setores diferentes** (ex: Mato, Placa Trânsito → Dep. + Segurança) | **1 registro por setor** |
| Sem tipo | 1 registro com tip_id NULL |

### 4.3 Função `_tipos_por_setor`

Agrupa os tipos pelo setor (via mapeamento `setores` ↔ `tipos`) e retorna um tipo representante por setor. O `tip_id` é resolvido pela tabela `tipos`.

---

## 5. Modelo de Dados

### 5.1 Tabelas Principais

| Tabela | Descrição |
|--------|-----------|
| **tipos** | Catálogo de tipos de ocorrência (Mato, Buraco, etc.) |
| **setores** | Setores responsáveis (Dep. Serviços Públicos, Fiscalização, etc.) |
| **setores_tipos** | Vínculo N:N entre setor e tipo |
| **ocorrencias** | Registros importados; `tip_id` FK para `tipos` |
| **ordens_servico** | Ordens de serviço; `tip_id` FK para `tipos` |
| **uploads_planilha** | Histórico de uploads (nome, caminho, status, total de registros) |

### 5.2 Regras de Nomenclatura

- **ocorrencias:** prefixo `oco_` (oco_datahora, oco_bairro, etc.)
- **ordens_servico:** prefixo `ose_`
- **uploads_planilha:** prefixo `upl_`
- **Auditoria:** `create_by`, `updated_by`, `create_at`, `update_at` (sem prefixo)

### 5.3 Chave Estrangeira tip_id

O campo `tip_id` (sem prefixo) é usado em `ocorrencias` e `ordens_servico` para vincular ao tipo. Facilita joins e integridade referencial.

### 5.4 oco_numero

Permite duplicatas quando a mesma ocorrência tem múltiplos tipos/setores (sem constraint UNIQUE).

---

## 6. Mapeamento Tipo → Setor

- **Fonte 1:** banco (`setores_tipos` + `tipos` + `setores`)
- **Fonte 2 (fallback):** `config/mapeamento_setores.json`

Usado para:
1. Decidir se divide ou não a linha (regra por setor)
2. Envio de e-mails (setor → destinatário)
3. Agrupamento dos relatórios

---

## 7. Log e Tratamento de Erros

| Arquivo | Conteúdo |
|---------|----------|
| `logs/erros.log` | Erros com traceback |
| `logs/app.log` | Log geral (INFO+) |

Mensagens de erro longas são truncadas na UI e no banco (upl_mensagemerro) para facilitar leitura.

---

## 8. Configuração (.env)

| Variável | Uso |
|----------|-----|
| DATABASE_URL | PostgreSQL (senha com @ → %40) |
| DIR_UPLOADS | Diretório de uploads |
| SMTP_* | Envio de e-mails |
| EMAIL_RELTORIO_TOTAL | Destinatários do relatório Prefeito |
| EMAIL_COPIA | Cópias |

---

## 9. Scripts de Manutenção

| Script | Função |
|--------|--------|
| `init_db.sql` | Criação das tabelas e carga inicial de tipos/setores |
| `migrate_add_tipid.sql` | Adiciona tip_id e remove UNIQUE de oco_numero em bancos antigos |
| `inspector_colunas.py` | Mostra mapeamento das colunas da planilha |

---

## 10. Resumo das Regras

1. **Planilha Mapzer:** linha 6 = cabeçalhos, linha 7+ = dados.
2. **Divisão por setor:** 1 registro por setor; tipos do mesmo setor mantêm 1 linha só.
3. **tip_id:** FK em ocorrencias e ordens_servico para integridade e leitura.
4. **Processar e Enviar e-mail:** ações independentes, permitindo reenvio sem reprocessar.
5. **Auditoria:** create_by, updated_by, create_at, update_at em todas as tabelas.
