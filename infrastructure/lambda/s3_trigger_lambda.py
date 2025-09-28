"""
Lambda Function - S3 Event Trigger
Tech Challenge Fase 3 - Seguindo padrão das Fases 1 e 2

Função Lambda acionada quando novos arquivos chegam no S3-RAW
Dispara Glue Jobs para processamento RAW → TRUSTED → REFINED
"""

import json
import boto3
import logging
import os
from datetime import datetime

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Clients
glue_client = boto3.client('glue')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Handler principal da função Lambda
    Acionado por eventos S3 PUT/POST
    """
    
    try:
        logger.info(f"🚀 Lambda triggered com evento: {json.dumps(event)}")
        
        # Parse S3 event
        for record in event['Records']:
            bucket_name = record['s3']['bucket']['name']
            object_key = record['s3']['object']['key']
            
            logger.info(f"📁 Processando: s3://{bucket_name}/{object_key}")
            
            # Determinar tipo de arquivo IMDb
            file_type = _extract_file_type(object_key)
            
            if file_type:
                # Disparar Glue Job correspondente
                job_run_id = _trigger_glue_job(file_type, bucket_name, object_key)
                logger.info(f"✅ Glue Job iniciado: {job_run_id}")
            else:
                logger.warning(f"⚠️ Tipo de arquivo não reconhecido: {object_key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Lambda processado com sucesso',
                'timestamp': datetime.now().isoformat(),
                'processed_files': len(event['Records'])
            })
        }
        
    except Exception as e:
        logger.error(f"❌ Erro no Lambda: {str(e)}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
        }

def _extract_file_type(object_key: str) -> str:
    """Extrai tipo de arquivo IMDb da key S3"""
    
    type_mapping = {
        'title.basics.tsv.gz': 'basics',
        'title.ratings.tsv.gz': 'ratings', 
        'title.crew.tsv.gz': 'crew',
        'title.principals.tsv.gz': 'principals',
        'title.akas.tsv.gz': 'akas',
        'title.episode.tsv.gz': 'episode',
        'name.basics.tsv.gz': 'names'
    }
    
    for filename, file_type in type_mapping.items():
        if filename in object_key:
            return file_type
    
    return None

def _trigger_glue_job(file_type: str, bucket: str, key: str) -> str:
    """Dispara Glue Job específico para o tipo de arquivo"""
    
    job_name = f"imdb-{file_type}-etl-job"
    
    try:
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments={
                '--source-bucket': bucket,
                '--source-key': key,
                '--file-type': file_type,
                '--target-bucket-trusted': os.getenv('TC_S3_TRUSTED', 'imdb-trusted-data-718942601863'),
                '--target-bucket-refined': os.getenv('TC_S3_REFINED', 'imdb-refined-data-718942601863'),
                '--enable-metrics': '',
                '--enable-continuous-cloudwatch-log': '',
                '--job-language': 'python',
                '--job-bookmark-option': 'job-bookmark-enable'
            }
        )
        
        return response['JobRunId']
        
    except Exception as e:
        logger.error(f"❌ Erro ao iniciar Glue Job {job_name}: {str(e)}")
        raise

# Para testes locais
if __name__ == "__main__":
    # Mock S3 event para teste
    test_event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "imdb-raw-data-718942601863"},
                    "object": {"key": "imdb/ratings/year=2025/month=09/day=28/title.ratings.tsv.gz"}
                }
            }
        ]
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))
