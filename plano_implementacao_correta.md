# 🔧 IMPLEMENTAÇÃO CORRETA - SEGUINDO FASES 1 e 2

## ETAPA 1: Lambda Functions
□ Criar função Lambda para trigger S3 events  
□ Lambda para processamento de ingestão
□ Lambda para API endpoints (Mangum)

## ETAPA 2: Glue Jobs & Catalog
□ Script Glue para RAW → TRUSTED
□ Script Glue para TRUSTED → REFINED  
□ Configurar Glue Crawler para catalogar Parquet
□ Registrar tabelas no Glue Catalog

## ETAPA 3: Athena Integration
□ Configurar Athena Workgroup
□ Criar views/queries para dados IMDb
□ Testar acesso via SQL

## ETAPA 4: Jupyter + Athena
□ Adaptar exercicio_cross_validation.ipynb
□ Conectar notebooks no Athena (PyAthena)
□ Implementar modelo ML com dados do Athena

## ETAPA 5: CI/CD
□ GitHub Actions workflows
□ Deploy automático Render
□ Pipeline de ML integrado

## ETAPA 6: API Lambda Deploy
□ Deploy FastAPI via Lambda (Mangum)
□ API Gateway integration
□ CloudWatch monitoring

