#!/usr/bin/env python3
"""
Teste individual de um Glue Job
"""

import boto3
import time

glue_client = boto3.client('glue', region_name='us-east-1')

def start_ratings_job():
    """Testa apenas o job de ratings"""
    
    job_arguments = {
        '--source-key': 'imdb/raw/title.ratings.tsv.gz',
        '--file-type': 'ratings'
    }
    
    print(" Iniciando job de ratings...")
    print(f" Argumentos: {job_arguments}")
    
    try:
        response = glue_client.start_job_run(
            JobName='imdb-ratings-etl-job-dev',
            Arguments=job_arguments
        )
        
        job_run_id = response['JobRunId']
        print(f" Job iniciado! Run ID: {job_run_id}")
        
        # Monitorar por 2 minutos
        for i in range(8):  # 8 * 15 = 120 segundos
            time.sleep(15)
            
            response = glue_client.get_job_run(
                JobName='imdb-ratings-etl-job-dev',
                RunId=job_run_id
            )
            
            state = response['JobRun']['JobRunState']
            
            if state == 'SUCCEEDED':
                print(f" Job concluído com sucesso!")
                return True
            elif state in ['FAILED', 'ERROR', 'STOPPED']:
                error = response['JobRun'].get('ErrorMessage', 'Sem erro')
                print(f" Job falhou: {error}")
                return False
            else:
                print(f"⏳ Estado: {state} (tentativa {i+1}/8)")
        
        print("⏰ Timeout - job ainda executando")
        return False
        
    except Exception as e:
        print(f" Erro: {e}")
        return False

if __name__ == "__main__":
    start_ratings_job()
