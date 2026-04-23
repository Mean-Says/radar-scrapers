# radar-scrapers

Scrapers externos do [Radar de Empregos](https://radardeempregos.com) — rodam em GitHub Actions (Python) e Cloudflare Workers (TypeScript) e enviam vagas via `POST /api/jobs/ingest`.

## Arquitetura

```
GitHub Actions (a cada 30min)
  ├── linkedin.yml     → scrapers/linkedin.py     (python-jobspy)
  └── remote-jobs.yml  → scrapers/remote_apis.py  (Remotive, RemoteOK, Himalayas, Jobicy)

Cloudflare Workers (a cada 15min)
  └── cloudflare/src/index.ts  (Remotive, RemoteOK, Himalayas, Jobicy — via JSON API)
        ↓
POST /api/jobs/ingest  (Radar de Empregos backend)
```

## Setup: GitHub Actions

### 1. Fazer fork / criar repo público

Repo precisa ser **público** para usar GitHub Actions sem limite de minutos.

### 2. Configurar os Secrets

Vá em **Settings → Secrets and variables → Actions → New repository secret** e adicione:

| Secret | Valor | Descrição |
|--------|-------|-----------|
| `INGEST_URL` | `https://radardeempregos.com/api/jobs/ingest` | URL do endpoint de ingestão |
| `INGEST_API_KEY` | *(gere uma chave segura)* | Chave de API do endpoint (deve bater com `INGEST_API_KEY` no `.env` do backend) |

> **Gerar uma chave segura:**
> ```bash
> python -c "import secrets; print(secrets.token_hex(32))"
> ```

### 3. Ativar os workflows

Os workflows rodam automaticamente pelo cron. Para testar manualmente:
**Actions → LinkedIn Scraper → Run workflow**

---

## Setup: Cloudflare Workers

### 1. Instalar Wrangler

```bash
cd cloudflare
npm install
```

### 2. Autenticar no Cloudflare

```bash
npx wrangler login
```

### 3. Configurar os secrets

```bash
npx wrangler secret put INGEST_URL
# → cole: https://radardeempregos.com/api/jobs/ingest

npx wrangler secret put INGEST_API_KEY
# → cole a mesma chave usada no GitHub Actions
```

### 4. Deploy

```bash
npm run deploy
```

O worker passa a rodar automaticamente a cada **15 minutos** via Cron Trigger.

Para monitorar logs em tempo real:
```bash
npm run tail
```

---

## Desenvolvimento local

```bash
# Instalar deps Python
pip install -r requirements.txt

# Testar um grupo de scrapers (precisa de INGEST_URL e INGEST_API_KEY no ambiente)
export INGEST_URL=https://radardeempregos.com/api/jobs/ingest
export INGEST_API_KEY=sua_chave_aqui
python runner.py linkedin
python runner.py remote
python runner.py all

# Testar o worker localmente
cd cloudflare
npm run dev
```

---

## Adicionando novos scrapers

1. Crie `scrapers/minha_fonte.py` com uma função `scrape() -> List[IngestJob]`
2. Use o schema de `schema.py` — todos os campos opcionais exceto `title`, `url`, `source`
3. Adicione o grupo em `runner.py`
4. Crie o workflow em `.github/workflows/minha-fonte.yml`

Padrão obrigatório no campo `source`: sempre minúsculo e sem espaços (ex: `"linkedin"`, `"remoteok"`, `"gupy"`).
