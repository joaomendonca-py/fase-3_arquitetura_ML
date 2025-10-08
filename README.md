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

## Status Atual - Fase 3 - PROJETO COMPLETO ✓

### INFRAESTRUTURA (100% Completa)
- ✓ **7 Glue Jobs executados** (ratings, basics, crew, episode, akas, principals, name_basics)
- ✓ **2.6GB dados processados** em S3 Medallion Architecture (RAW → TRUSTED → REFINED)
- ✓ **16 tabelas catalogadas** no AWS Glue Catalog
- ✓ **AWS Athena funcionando** com 1.6M+ registros de ratings + 726K filmes
- ✓ **GitHub Actions CI/CD** completo (lint, test, deploy CloudFormation)
- ✓ **CloudFormation IaC** para Lambda, Glue, Athena, API Gateway

### MACHINE LEARNING (100% Completo)
- ✓ **Dataset preparado**: 100.990 filmes com ratings (1980-2023)
- ✓ **Feature Engineering**: 46 features (18 base + 28 engineered)
  - Features de contexto (year_movie_count, genre_avg_votes, genre_avg_rating)
  - Todos os gêneros expandidos (15 gêneros binários + num_genres)
  - Features temporais avançadas (cinema_era, movie_age_squared/log, is_recent/classic)
  - Interações entre features (runtime × gênero, year × gênero, votes_per_age)
- ✓ **8 Modelos treinados e comparados**:
  - V1: Linear Regression (R²=0.2681), Random Forest (overfitting)
  - V2: Linear Regression (R²=0.3413), Random Forest (R²=0.4044), Ridge, Lasso
  - V3: Gradient Boosting (R²=0.4003), **Ensemble RF+GBM (R²=0.4102)** ← MELHOR
- ✓ **Performance Final**: R²=0.4102, RMSE=1.0153 (53% melhor que baseline)
- ✓ **Validação completa**: Cross-validation 5-fold, sem overfitting
- ✓ **Análises avançadas**: Pair plot, Predicted vs Real, Residual Analysis
- ✓ **Notebook documentado**: `notebooks/02_imdb_ml_tech_challenge_clean.ipynb`

### ENTREGÁVEIS
1. ✓ Pipeline de dados AWS completo (Glue + Athena)
2. ✓ Modelo ML estado-da-arte (Ensemble Random Forest + Gradient Boosting)
3. ✓ Notebook Jupyter profissional com análise completa
4. ✓ Documentação técnica e executiva
5. ✓ Infraestrutura como código (CloudFormation)
6. ✓ CI/CD configurado (GitHub Actions)

**Status Final:** TODAS AS FASES CONCLUÍDAS COM SUCESSO

---

## Dicas rápidas
- **Custos baixos**: use pandas local para baseline; Glue só para consolidar refinado.
- **Governança**: padronize partições: `year=YYYY/month=MM/day=DD/` (e `ticker=...` quando aplicável).
- **Reprodutibilidade**: fixe seeds, salve métricas e artefatos com `ingestion_date=...`.
