#!/usr/bin/env python3
"""
Orquestrador de Glue Jobs - Tech Challenge Fase 3
Executa todos os 6 Glue Jobs para processar dados IMDb
RAW → TRUSTED → REFINED
"""

import boto3
import time
import sys
from datetime import datetime
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configurações AWS
glue_client = boto3.client('glue', region_name='us-east-1')
s3_client = boto3.client('s3', region_name='us-east-1')

# Configurações dos buckets
S3_RAW = os.getenv('TC_S3_RAW', 'imdb-raw-data-718942601863')
S3_TRUSTED = os.getenv('TC_S3_TRUSTED', 'imdb-trusted-data-718942601863')
S3_REFINED = os.getenv('TC_S3_REFINED', 'imdb-refined-data-718942601863')

# Mapeamento dos jobs e seus arquivos correspondentes
GLUE_JOBS_CONFIG = [
    {
        'job_name': 'imdb-ratings-etl-job-dev',
        'file_key': 'imdb/raw/title.ratings.tsv.gz',
        'file_type': 'ratings',
        'description': 'IMDb Ratings (Avaliações)',
        'timeout_minutes': 60
    },
    {
        'job_name': 'imdb-title-akas-etl-job-dev',
        'file_key': 'imdb/raw/title.akas.tsv.gz',
        'file_type': 'title_akas',
        'description': 'IMDb Title Akas (Títulos Alternativos)',
        'timeout_minutes': 90
    },
    {
        'job_name': 'imdb-title-crew-etl-job-dev',
        'file_key': 'imdb/raw/title.crew.tsv.gz',
        'file_type': 'title_crew',
        'description': 'IMDb Title Crew (Equipe Técnica)',
        'timeout_minutes': 60
    },
    {
        'job_name': 'imdb-title-episode-etl-job-dev',
        'file_key': 'imdb/raw/title.episode.tsv.gz',
        'file_type': 'title_episode',
        'description': 'IMDb Title Episode (Episódios)',
        'timeout_minutes': 60
    },
    {
        'job_name': 'imdb-title-principals-etl-job-dev',
        'file_key': 'imdb/raw/title.principals.tsv.gz',
        'file_type': 'title_principals',
        'description': 'IMDb Title Principals (Elenco Principal)',
        'timeout_minutes': 120
    },
    {
        'job_name': 'imdb-name-basics-etl-job-dev',
        'file_key': 'imdb/raw/name.basics.tsv.gz',
        'file_type': 'name_basics',
        'description': 'IMDb Name Basics (Pessoas/Profissionais)',
        'timeout_minutes': 150
    }
]

def check_file_exists_in_s3(bucket: str, key: str) -> bool:
    """Verifica se um arquivo existe no S3"""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except s3_client.exceptions.NoSuchKey:
        return False
    except Exception as e:
        print(f" Erro ao verificar arquivo {key}: {e}")
        return False

def start_glue_job(job_config: dict) -> str:
    """Inicia um Glue Job e retorna o job_run_id"""
    
    job_name = job_config['job_name']
    file_key = job_config['file_key']
    file_type = job_config['file_type']
    
    print(f"\n Iniciando: {job_config['description']}")
    print(f"   Job: {job_name}")
    print(f"   Arquivo: {file_key}")
    
    # Verificar se arquivo existe no S3-RAW
    if not check_file_exists_in_s3(S3_RAW, file_key):
        print(f" Arquivo {file_key} não encontrado em {S3_RAW}")
        return None
    
    # Parâmetros do job
    job_arguments = {
        '--source-bucket': S3_RAW,
        '--source-key': file_key,
        '--file-type': file_type,
        '--target-bucket-trusted': S3_TRUSTED,
        '--target-bucket-refined': S3_REFINED
    }
    
    try:
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments=job_arguments
        )
        
        job_run_id = response['JobRunId']
        print(f" Job iniciado! Run ID: {job_run_id}")
        
        return job_run_id
        
    except Exception as e:
        print(f" Erro ao iniciar job {job_name}: {e}")
        return None

def monitor_job_run(job_name: str, job_run_id: str, timeout_minutes: int) -> bool:
    """Monitora a execução de um Glue Job até completar ou falhar"""
    
    print(f" Monitorando job {job_name} (timeout: {timeout_minutes}min)")
    
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    
    while True:
        try:
            response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
            job_run = response['JobRun']
            
            state = job_run['JobRunState']
            
            if state == 'SUCCEEDED':
                elapsed = int((time.time() - start_time) / 60)
                print(f" Job {job_name} CONCLUÍDO! (tempo: {elapsed}min)")
                return True
                
            elif state in ['FAILED', 'ERROR', 'STOPPED']:
                error_message = job_run.get('ErrorMessage', 'Sem detalhes do erro')
                print(f" Job {job_name} FALHOU!")
                print(f"   Estado: {state}")
                print(f"   Erro: {error_message}")
                return False
                
            elif state in ['RUNNING', 'STARTING']:
                elapsed = int((time.time() - start_time) / 60)
                print(f"⏳ Job {job_name} em execução... ({elapsed}min)")
                
                # Verificar timeout
                if time.time() - start_time > timeout_seconds:
                    print(f"⏰ TIMEOUT! Job {job_name} excedeu {timeout_minutes} minutos")
                    return False
                    
                time.sleep(30)  # Aguardar 30 segundos
                
            else:
                print(f" Job {job_name} no estado: {state}")
                time.sleep(10)
                
        except Exception as e:
            print(f" Erro ao monitorar job {job_name}: {e}")
            return False

def verify_output_data(file_type: str) -> dict:
    """Verifica se os dados foram processados corretamente"""
    
    result = {
        'trusted_exists': False,
        'refined_exists': False,
        'trusted_count': 0,
        'refined_count': 0
    }
    
    try:
        # Verificar TRUSTED
        trusted_prefix = f"imdb/{file_type}/"
        trusted_objects = s3_client.list_objects_v2(
            Bucket=S3_TRUSTED,
            Prefix=trusted_prefix
        )
        
        if trusted_objects.get('Contents'):
            result['trusted_exists'] = True
            result['trusted_count'] = len(trusted_objects['Contents'])
        
        # Verificar REFINED
        refined_prefix = f"imdb/{file_type}/"
        refined_objects = s3_client.list_objects_v2(
            Bucket=S3_REFINED,
            Prefix=refined_prefix
        )
        
        if refined_objects.get('Contents'):
            result['refined_exists'] = True
            result['refined_count'] = len(refined_objects['Contents'])
            
    except Exception as e:
        print(f"⚠️ Erro ao verificar dados para {file_type}: {e}")
    
    return result

def main():
    """Função principal - executa todos os Glue Jobs"""
    
    print("=" * 60)
    print(" ORQUESTRADOR DE GLUE JOBS - TECH CHALLENGE FASE 3")
    print("=" * 60)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" S3 RAW: {S3_RAW}")
    print(f" S3 TRUSTED: {S3_TRUSTED}")
    print(f" S3 REFINED: {S3_REFINED}")
    print(f"🔧 Total de jobs: {len(GLUE_JOBS_CONFIG)}")
    
    # Estatísticas
    total_jobs = len(GLUE_JOBS_CONFIG)
    successful_jobs = 0
    failed_jobs = 0
    
    # Executar cada job sequencialmente
    for i, job_config in enumerate(GLUE_JOBS_CONFIG, 1):
        
        print(f"\n{'='*20} JOB {i}/{total_jobs} {'='*20}")
        
        # Iniciar job
        job_run_id = start_glue_job(job_config)
        
        if not job_run_id:
            failed_jobs += 1
            continue
        
        # Monitorar job
        success = monitor_job_run(
            job_config['job_name'],
            job_run_id,
            job_config['timeout_minutes']
        )
        
        if success:
            successful_jobs += 1
            
            # Verificar dados de saída
            print(f" Verificando dados processados...")
            output_data = verify_output_data(job_config['file_type'])
            
            if output_data['trusted_exists'] and output_data['refined_exists']:
                print(f" Dados verificados: TRUSTED ({output_data['trusted_count']} arquivos), REFINED ({output_data['refined_count']} arquivos)")
            else:
                print(f"⚠️ Dados incompletos: TRUSTED: {output_data['trusted_exists']}, REFINED: {output_data['refined_exists']}")
                
        else:
            failed_jobs += 1
    
    # Relatório final
    print(f"\n{'='*60}")
    print(" RELATÓRIO FINAL")
    print(f"{'='*60}")
    print(f" Jobs bem-sucedidos: {successful_jobs}/{total_jobs}")
    print(f" Jobs falharam: {failed_jobs}/{total_jobs}")
    print(f"📅 Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if successful_jobs == total_jobs:
        print("\n TODOS OS JOBS CONCLUÍDOS COM SUCESSO!")
        print(" Próximos passos:")
        print("   1. Verificar dados no Athena")
        print("   2. Executar MSCK REPAIR TABLE para cada tabela")
        print("   3. Começar desenvolvimento do notebook ML")
        return 0
    else:
        print(f"\n⚠️ {failed_jobs} job(s) falharam. Verificar logs na AWS.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
