# Deploy em Produção (Docker)

## Pastas necessárias para cópia

| Pasta      | Conteúdo                               | Uso                    |
|------------|----------------------------------------|------------------------|
| `src/`     | API FastAPI, ETL, SSO, reports, config | Código Python          |
| `static/`  | index.html, login.html, css/, js/, img/| Frontend               |
| `config/`  | email_geral.html, email_secretaria.html, mapeamento_setores.json | Templates e-mail |
| `scripts/` | init_uploads.sql, migrate_lotes.sql   | Migrações runtime      |

## Não copiadas (dev/local)

- `main.py`, `run_server.py`, `database.py` — CLI e dev
- `modelos/` — duplicata de config
- `docs/`, `logs/` — documentação e logs (logs criados em runtime)
- `.env` — secrets via `env_file` no compose

## Build e run

```bash
docker compose build
docker compose up -d
```

## Variáveis de ambiente

Obtidas do `.env` do host via `env_file`. Principais: `DATABASE_URL`, `AUTH_MOCK`, `DIR_UPLOADS`, SSO, SMTP.
