# Tech Challenge — Fases 1, 2 e 3 (Python + AWS)

Repositório base para três fases integradas. Estrutura focada em **S3 (raw/trusted/refined)**, **Lambda**, **Glue**, **Athena** e **FastAPI**.  
Na **Fase 3**, usamos o **dataset IMDb** (arquivos `.tsv.gz` do site oficial).

## Pré-requisitos
- Python 3.11+
- AWS CLI configurado (perfil com permissões mínimas)
- Buckets no S3 (dev): `tc-dev-raw`, `tc-dev-trusted`, `tc-dev-refined` (ou defina outros via env)

## Variáveis de ambiente (copiar de `.env.example` para `.env`)
- `TC_STAGE` (default: `dev`)
- `TC_REGION` (ex.: `sa-east-1`)
- `TC_S3_RAW`, `TC_S3_TRUSTED`, `TC_S3_REFINED`
- `TC_IMDB_LOCAL_DIR` (pasta local com `.tsv.gz` da IMDb; usado na Fase 3)

## Make (atalhos)
- `make env` — cria venv local
- `make install` — instala dependências
- `make run-fase1` — sobe FastAPI (localhost:8000)
- `make imdb-ingest` — ingere IMDb (Fase 3) da pasta local para S3 (raw/trusted/refined)
- `make imdb-train` — treina baseline e salva modelo em S3
- `make lint` / `make fmt` / `make test` — qualidade/corretude

---

## Fase 1 — API Embrapa (FastAPI)
**Objetivo**: expor rotas simples para dados (Produção, Processamento, Comercialização, Importação, Exportação), com **cache no S3**, `/docs` e `/healthz`.

### Rodar local
```bash
make run-fase1
# abra http://127.0.0.1:8000/docs
```

### Deploy (ideia)
- Lambda + API Gateway (HTTP API), com `fase1_api_embrapa/handler.py` (adapter `Mangum`).
- CloudWatch Logs para observabilidade.

---

## Fase 2 — Big Data Architecture (B3)
**Objetivo**: scraper salva **Parquet** no RAW (partição diária) → **S3 event** aciona **Lambda** → **Glue Job** transforma (agregação, renomeio, cálculo de data) → escreve em **REFINED** (partição por **dia** e **ticker**) → **Glue Catalog** → **Athena**.

### Passos rápidos
1) `python fase2_b3_pipeline/scraper_b3.py --date YYYY-MM-DD` (gera/atualiza RAW)  
2) Configure evento no S3 → Lambda (`fase2_b3_pipeline/lambda_trigger_glue.py`)  
3) Rode o Job Glue com o script `fase2_b3_pipeline/glue_job_script.py`  
4) Consulte no Athena e gere 1–2 gráficos no Athena Notebook.

---

## Fase 3 — ML com IMDb
**Objetivo**: ingerir IMDb em RAW/TRUSTED/REFINED, treinar **baseline** (regressão de `averageRating`) e publicar **endpoint** de predição (Lambda).

### Ingestão
```bash
make imdb-ingest
```
- Lê `.tsv.gz` (ex.: `title.basics.tsv.gz`, `title.ratings.tsv.gz`) da pasta `TC_IMDB_LOCAL_DIR`.
- Escreve Parquet no S3.

### Treino
```bash
make imdb-train
```
- Cria features simples e treina **LinearRegression** (baseline).  
- Salva modelo `.pkl` em `s3://.../fase3/models/`.

### Serving
- Lambda (`fase3_ml_imdb/lambda_predict.py`) carrega o `.pkl` do S3 e responde `POST /v1/imdb/predict_rating`.

---

## Status Atual - Fase 3 - INFRAESTRUTURA COMPLETA

**Pipeline End-to-End Funcionando:**
- **7 Glue Jobs executados** (ratings, basics, crew, episode, akas, principals, name_basics)
- **2.6GB dados processados** em S3 Medallion Architecture (RAW → TRUSTED → REFINED)
- **16 tabelas catalogadas** no AWS Glue Catalog
- **AWS Athena funcionando** com 1.6M+ registros de ratings + 726K filmes
- **Jupyter Notebook** pronto para ML conectando diretamente no Athena
- **GitHub Actions CI/CD** completo (lint, test, deploy CloudFormation)
- **CloudFormation IaC** para Lambda, Glue, Athena, API Gateway
- **Query complexa testada**: JOIN entre ratings e basics funcionando

**Próximos passos:**
1. Executar notebook ML completo (`notebooks/01_imdb_ml_athena.ipynb`)
2. Deploy do modelo treinado em Lambda
3. API de predição funcionando

---

## Dicas rápidas
- **Custos baixos**: use pandas local para baseline; Glue só para consolidar refinado.
- **Governança**: padronize partições: `year=YYYY/month=MM/day=DD/` (e `ticker=...` quando aplicável).
- **Reprodutibilidade**: fixe seeds, salve métricas e artefatos com `ingestion_date=...`.
