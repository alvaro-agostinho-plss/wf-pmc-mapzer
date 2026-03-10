# Sistema de Inteligência de Ocorrências Mapzer

ETL e automação de relatórios por e-mail. Agrupamento Setor→Bairro→Tipo, tabelas com subtotais, e-mail Prefeito.

## Estrutura

```
.
├── config/
│   ├── mapeamento_setores.json   # Setor → [tipos]
│   └── template_email.html       # Template Jinja2 do e-mail
├── database.py                   # SQLAlchemy
├── data/
│   └── ocorrencias_exemplo.xlsx  # Planilha de exemplo
├── src/
│   ├── config.py     # Pydantic + .env
│   ├── etl.py        # Leitura, tratamento, persistência
│   ├── models.py     # Modelos SQLAlchemy
│   └── reports.py    # Agrupamento e envio de e-mails
├── main.py
├── .env.example
└── requirements.txt
```

## Setup

```bash
python -m venv venv
source venv/bin/activate  # ou venv\Scripts\activate no Windows
pip install -r requirements.txt
cp .env.example .env
# Editar .env com credenciais de PostgreSQL e SMTP
```

## Configuração

### `.env`

- **PostgreSQL**: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- **SMTP**: `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASSWORD`, `SMTP_FROM`

### Mapeamento Setores

`config/mapeamento_setores.json`:

```json
{
  "Buraco na Via": {"setor": "Obras", "email": "obras@prefeitura.gov.br"},
  "Iluminação Pública": {"setor": "Iluminação", "email": "iluminacao@prefeitura.gov.br"}
}
```

## Uso

```bash
# Planilha Mapzer (relatório geral)
python main.py etl "docs/mapzer _geral_relatorio.xlsx" --header-row 5 --truncar
python main.py all "docs/mapzer _geral_relatorio.xlsx" --header-row 5 --truncar

# Planilha simples (cabeçalho na primeira linha)
python main.py etl data/ocorrencias_exemplo.xlsx --truncar

# Enviar relatórios por e-mail
python main.py relatorios

# Interface web (upload + processar)
python run_server.py
# Acesse http://localhost:8000
```

## Regras de Negócio

- **Colunas**: Todas normalizadas para `oco_nomecampo` (ex: oco_bairro, oco_tipo, oco_status)
- **Auditoria**: `create_by`, `updated_by`, `create_at`, `update_at` em todas as tabelas
- **Mapeamento**: Tipos de ocorrência vinculados a setor/e-mail via JSON
