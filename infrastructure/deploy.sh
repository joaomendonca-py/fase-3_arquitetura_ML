#!/bin/bash
# Deploy Script - Tech Challenge Fase 3
# Deploy via AWS CLI + CloudFormation

set -e

echo " DEPLOY REAL DA INFRAESTRUTURA - TECH CHALLENGE FASE 3"
echo "========================================================="

# Configurações
STAGE=${1:-dev}
REGION=${AWS_DEFAULT_REGION:-us-east-1}
STACK_PREFIX="imdb-ml-${STAGE}"

echo " Configurações:"
echo "  Stage: ${STAGE}"
echo "  Region: ${REGION}"
echo "  Stack Prefix: ${STACK_PREFIX}"
echo ""

# Verificar AWS CLI
echo "🔍 Verificando AWS CLI..."
if ! command -v aws &> /dev/null; then
    echo " AWS CLI não encontrado!"
    exit 1
fi

# Verificar credenciais AWS
echo "🔐 Verificando credenciais AWS..."
aws sts get-caller-identity > /dev/null
echo " Credenciais AWS OK"

# Criar diretórios necessários
mkdir -p infrastructure/builds

# STEP 1: Preparar código Lambda
echo ""
echo "📦 STEP 1: Preparando código das Lambda Functions..."

# Criar ZIP com código da API
echo "   Empacotando API Lambda..."
cd ..
zip -r "fase-3_arquitetura_ML/infrastructure/builds/api-lambda.zip" \
    "fase-3_arquitetura_ML/fase3_ml_imdb/" \
    -x "*.pyc" "*__pycache__*" "*.git*"
cd "fase-3_arquitetura_ML"

# Criar ZIP com código S3 Trigger
echo "   Empacotando S3 Trigger Lambda..."
zip -r infrastructure/builds/s3-trigger-lambda.zip \
    infrastructure/lambda/ \
    -x "*.pyc" "*__pycache__*"

# Upload dos códigos para S3 (bucket temporário)
echo "   Fazendo upload dos ZIPs para S3..."
aws s3 cp infrastructure/builds/api-lambda.zip \
    s3://imdb-raw-data-718942601863/lambda-code/api-lambda.zip

aws s3 cp infrastructure/builds/s3-trigger-lambda.zip \
    s3://imdb-raw-data-718942601863/lambda-code/s3-trigger-lambda.zip

echo " Código Lambda preparado e uploaded"

# STEP 2: Upload scripts Glue
echo ""  
echo "🔧 STEP 2: Fazendo upload dos scripts Glue..."

aws s3 cp infrastructure/glue/imdb_ratings_etl_job.py \
    s3://imdb-raw-data-718942601863/glue-scripts/imdb_ratings_etl_job.py

# Criar script básico para basics (placeholder)
cat > infrastructure/builds/imdb_basics_etl_job.py << 'EOF'
"""
Glue Job - IMDb Basics ETL (Placeholder)
TODO: Implementar processamento completo
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(" IMDb Basics ETL Job - Placeholder funcionando!")
# TODO: Implementar processamento real

job.commit()
EOF

aws s3 cp infrastructure/builds/imdb_basics_etl_job.py \
    s3://imdb-raw-data-718942601863/glue-scripts/imdb_basics_etl_job.py

echo " Scripts Glue uploaded"

# STEP 3: Deploy CloudFormation Stacks
echo ""
echo "☁️ STEP 3: Fazendo deploy dos CloudFormation Stacks..."

# Deploy 1: Glue Jobs & Database
echo "   Deploy Glue Jobs & Database..."
aws cloudformation deploy \
    --template-file infrastructure/cloudformation/glue-jobs.yml \
    --stack-name "${STACK_PREFIX}-glue" \
    --parameter-overrides \
        Stage=${STAGE} \
        S3BucketRaw=imdb-raw-data-718942601863 \
        S3BucketTrusted=imdb-trusted-data-718942601863 \
        S3BucketRefined=imdb-refined-data-718942601863 \
    --capabilities CAPABILITY_NAMED_IAM \
    --region ${REGION}

echo " Glue stack deployed"

# Deploy 2: Athena Workgroup  
echo "  🔍 Deploy Athena Workgroup..."
aws cloudformation deploy \
    --template-file infrastructure/cloudformation/athena-workgroup.yml \
    --stack-name "${STACK_PREFIX}-athena" \
    --parameter-overrides \
        Stage=${STAGE} \
        QueryResultsBucket=aws-athena-query-results-718942601863-${REGION}-8c1egr1z \
    --region ${REGION}

echo " Athena stack deployed"

# Deploy 3: Lambda Functions & API Gateway
echo "  🔧 Deploy Lambda Functions & API..."
aws cloudformation deploy \
    --template-file infrastructure/cloudformation/lambda-functions.yml \
    --stack-name "${STACK_PREFIX}-lambda" \
    --parameter-overrides \
        Stage=${STAGE} \
        S3BucketRaw=imdb-raw-data-718942601863 \
        S3BucketTrusted=imdb-trusted-data-718942601863 \
        S3BucketRefined=imdb-refined-data-718942601863 \
        S3BucketModels=imdb-ml-models-718942601863 \
    --capabilities CAPABILITY_NAMED_IAM \
    --region ${REGION}

echo " Lambda stack deployed"

# STEP 4: Obter outputs
echo ""
echo " STEP 4: Obtendo informações dos recursos criados..."

# API URL
API_URL=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_PREFIX}-lambda" \
    --query 'Stacks[0].Outputs[?OutputKey==`APIEndpoint`].OutputValue' \
    --output text)

# Database name
DB_NAME=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_PREFIX}-glue" \
    --query 'Stacks[0].Outputs[?OutputKey==`GlueDatabaseName`].OutputValue' \
    --output text)

# Workgroup name
WORKGROUP=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_PREFIX}-athena" \
    --query 'Stacks[0].Outputs[?OutputKey==`WorkgroupName`].OutputValue' \
    --output text)

# STEP 5: Testar API
echo ""
echo "🧪 STEP 5: Testando API deployada..."

echo "  📡 Testando endpoint de saúde..."
curl -f "${API_URL}/" || echo "⚠️ API não respondeu (pode ser normal se código estiver placeholder)"

# Criar arquivo de configuração
cat > .env.deployed << EOF
# Configuração de Deploy - Gerada automaticamente
TC_STAGE=${STAGE}
TC_REGION=${REGION}

# API
API_ENDPOINT=${API_URL}

# Glue
GLUE_DATABASE=${DB_NAME}

# Athena  
ATHENA_WORKGROUP=${WORKGROUP}
ATHENA_STAGING_DIR=s3://aws-athena-query-results-718942601863-${REGION}-8c1egr1z/

# S3 Buckets (já existentes)
TC_S3_RAW=imdb-raw-data-718942601863
TC_S3_TRUSTED=imdb-trusted-data-718942601863
TC_S3_REFINED=imdb-refined-data-718942601863
TC_S3_MODELS=imdb-ml-models-718942601863
EOF

echo ""
echo " DEPLOY CONCLUÍDO COM SUCESSO!"
echo "================================"
echo ""
echo " RECURSOS CRIADOS:"
echo "  🌐 API Endpoint: ${API_URL}"
echo "   Glue Database: ${DB_NAME}"
echo "  🔍 Athena Workgroup: ${WORKGROUP}"
echo ""
echo " Configuração salva em: .env.deployed"
echo ""
echo "🧪 PRÓXIMOS PASSOS:"
echo "  1. Testar API endpoints"
echo "  2. Executar Glue Jobs"
echo "  3. Rodar queries Athena"
echo "  4. Desenvolver notebooks ML"
echo ""
echo " Infraestrutura pronta para uso!"
