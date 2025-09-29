# Tech Challenge - Fase 3 - IMDb ML Pipeline

.PHONY: help env install clean lint fmt test

# Environment Variables
PYTHON_VERSION=3.11
VENV_NAME=venv
API_HOST=0.0.0.0
API_PORT=8000

help: ## Mostra comandos disponíveis
	@echo "Tech Challenge - Fase 3 - Comandos Disponíveis:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

env: ## Cria ambiente virtual Python
	python$(PYTHON_VERSION) -m venv $(VENV_NAME)
	@echo "Ambiente virtual criado. Ative com: source $(VENV_NAME)/bin/activate"

install: ## Instala dependências
	pip install --upgrade pip
	pip install -r requirements.txt
	@echo "OK Dependências instaladas"

# === COLETA DE DADOS ===
run-api: ## Sobe API de coleta de dados (local)
	uvicorn fase3_ml_imdb.api_collector.main:app --host $(API_HOST) --port $(API_PORT) --reload

imdb-ingest: ## Ingere dados IMDb para S3 (RAW → TRUSTED → REFINED)
	python -m fase3_ml_imdb.data_pipeline.ingest_imdb

# === MACHINE LEARNING ===
jupyter: ## Inicia Jupyter Lab para experimentação
	jupyter lab --ip=0.0.0.0 --port=8888 --allow-root --no-browser

imdb-train: ## Treina modelo ML (notebooks → produção)
	python -m fase3_ml_imdb.ml_training.train_model

# === QUALIDADE DE CÓDIGO ===
lint: ## Executa linting (flake8)
	flake8 fase3_ml_imdb/ tests/

fmt: ## Formata código (black + isort)
	black fase3_ml_imdb/ tests/
	isort fase3_ml_imdb/ tests/

test: ## Executa testes
	pytest tests/ -v

clean: ## Remove arquivos temporários
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -name "*.egg-info" -type d -exec rm -rf {} +

# === DEPLOYMENT ===
deploy-api: ## Deploy API para AWS Lambda
	@echo " Deploy da API para AWS Lambda..."
	# TODO: Implementar script de deploy

deploy-pipeline: ## Deploy pipeline de dados
	@echo " Deploy do pipeline de dados..."
	# TODO: Implementar com Serverless Framework

# === STATUS ===
status: ## Mostra status do projeto
	@echo " Status do Projeto:"
	@echo "- Python: $(shell python --version)"
	@echo "- Ambiente: $(shell if [ -d $(VENV_NAME) ]; then echo "OK Criado"; else echo "NOK Não criado"; fi)"
	@echo "- AWS CLI: $(shell if command -v aws > /dev/null; then echo "OK Configurado"; else echo "NOK Não configurado"; fi)"
	@echo "- Buckets S3: $(shell aws s3 ls | grep imdb | wc -l) criados"
