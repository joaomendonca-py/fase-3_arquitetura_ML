"""
IMDb Data Ingester
Tech Challenge Fase 3

Pipeline de ingestão: Local → S3-RAW → S3-TRUSTED → S3-REFINED
Seguindo padrão arquitetural das fases anteriores
"""

import os
import gzip
import pandas as pd
import boto3
from typing import List, Dict, Optional
import logging
from datetime import datetime
import json
from io import StringIO

logger = logging.getLogger(__name__)

class IMDbIngester:
    """
    Classe para ingestão de dados IMDb seguindo pipeline S3
    RAW → TRUSTED → REFINED
    """
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket_raw = os.getenv('TC_S3_RAW', 'imdb-raw-data-718942601863')
        self.bucket_refined = os.getenv('TC_S3_REFINED', 'imdb-refined-data-718942601863')
        self.bucket_models = os.getenv('TC_S3_MODELS', 'imdb-ml-models-718942601863')
        self.imdb_local_dir = os.getenv('TC_IMDB_LOCAL_DIR', './imdb')
        self.region = os.getenv('TC_REGION', 'us-east-1')
        
        # Mapeamento de arquivos IMDb
        self.imdb_files = {
            'basics': 'title.basics.tsv.gz',
            'ratings': 'title.ratings.tsv.gz', 
            'crew': 'title.crew.tsv.gz',
            'principals': 'title.principals.tsv.gz',
            'akas': 'title.akas.tsv.gz',
            'episode': 'title.episode.tsv.gz',
            'names': 'name.basics.tsv.gz'
        }
    
    def upload_raw_data(self, file_types: List[str]) -> Dict[str, str]:
        """
        Upload dos arquivos .tsv.gz originais para S3-RAW
        
        Args:
            file_types: Lista de tipos de arquivo para processar
            
        Returns:
            Dict com paths S3 dos arquivos carregados
        """
        uploaded_files = {}
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for file_type in file_types:
            if file_type not in self.imdb_files:
                logger.warning(f"Tipo de arquivo inválido: {file_type}")
                continue
                
            local_file = os.path.join(self.imdb_local_dir, self.imdb_files[file_type])
            
            if not os.path.exists(local_file):
                logger.warning(f"Arquivo não encontrado: {local_file}")
                continue
            
            # Path no S3-RAW com particionamento por data
            s3_key = f"imdb/{file_type}/year={datetime.now().year}/month={datetime.now().month:02d}/day={datetime.now().day:02d}/{self.imdb_files[file_type]}"
            
            try:
                logger.info(f"Uploading {local_file} → s3://{self.bucket_raw}/{s3_key}")
                
                self.s3_client.upload_file(
                    local_file,
                    self.bucket_raw,
                    s3_key,
                    ExtraArgs={'ServerSideEncryption': 'AES256'}
                )
                
                uploaded_files[file_type] = f"s3://{self.bucket_raw}/{s3_key}"
                logger.info(f"✅ Upload concluído: {file_type}")
                
            except Exception as e:
                logger.error(f"❌ Erro no upload {file_type}: {str(e)}")
                
        return uploaded_files
    
    def process_to_trusted(self, file_types: List[str]) -> Dict[str, int]:
        """
        Processa dados RAW → TRUSTED (limpeza, tipagem, validação)
        
        Args:
            file_types: Tipos de arquivo para processar
            
        Returns:
            Dict com contagem de registros processados
        """
        processed_counts = {}
        
        for file_type in file_types:
            if file_type not in self.imdb_files:
                continue
                
            try:
                logger.info(f"Processando {file_type} para TRUSTED...")
                
                # Lê arquivo local (para esta implementação inicial)
                local_file = os.path.join(self.imdb_local_dir, self.imdb_files[file_type])
                
                if not os.path.exists(local_file):
                    continue
                
                # Lê e processa dados
                df = self._read_tsv_gz(local_file)
                df_clean = self._clean_data(df, file_type)
                
                # Salva como Parquet no S3-TRUSTED
                s3_key = f"imdb/{file_type}/year={datetime.now().year}/month={datetime.now().month:02d}/day={datetime.now().day:02d}/data.parquet"
                
                self._save_parquet_to_s3(df_clean, self.bucket_refined, s3_key)
                
                processed_counts[file_type] = len(df_clean)
                logger.info(f"✅ {file_type}: {len(df_clean)} registros processados")
                
            except Exception as e:
                logger.error(f"❌ Erro processando {file_type}: {str(e)}")
                processed_counts[file_type] = 0
                
        return processed_counts
    
    def _read_tsv_gz(self, file_path: str) -> pd.DataFrame:
        """Lê arquivo .tsv.gz e retorna DataFrame"""
        logger.info(f"Lendo arquivo: {file_path}")
        
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            df = pd.read_csv(f, sep='\t', na_values=['\\N'])
            
        logger.info(f"Arquivo carregado: {len(df)} registros, {len(df.columns)} colunas")
        return df
    
    def _clean_data(self, df: pd.DataFrame, file_type: str) -> pd.DataFrame:
        """
        Aplica limpeza de dados específica por tipo de arquivo
        
        Args:
            df: DataFrame original
            file_type: Tipo do arquivo para aplicar limpezas específicas
            
        Returns:
            DataFrame limpo
        """
        df_clean = df.copy()
        
        # Limpeza geral
        # Remove linhas completamente vazias
        df_clean = df_clean.dropna(how='all')
        
        # Limpeza específica por tipo
        if file_type == 'basics':
            # title.basics.tsv.gz
            df_clean = self._clean_title_basics(df_clean)
        elif file_type == 'ratings':
            # title.ratings.tsv.gz
            df_clean = self._clean_title_ratings(df_clean)
        elif file_type == 'crew':
            # title.crew.tsv.gz  
            df_clean = self._clean_title_crew(df_clean)
        elif file_type == 'principals':
            # title.principals.tsv.gz
            df_clean = self._clean_title_principals(df_clean)
            
        # Add metadata
        df_clean['ingestion_date'] = datetime.now().date()
        df_clean['ingestion_timestamp'] = datetime.now()
        
        return df_clean
    
    def _clean_title_basics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpeza específica para title.basics.tsv.gz"""
        # Converte tipos
        df['startYear'] = pd.to_numeric(df['startYear'], errors='coerce')
        df['endYear'] = pd.to_numeric(df['endYear'], errors='coerce') 
        df['runtimeMinutes'] = pd.to_numeric(df['runtimeMinutes'], errors='coerce')
        df['isAdult'] = df['isAdult'].astype('int8', errors='ignore')
        
        # Filtra apenas filmes e séries (remove adulto)
        df = df[df['isAdult'] == 0]
        
        # Filtra filmes com anos válidos (após 1900)
        df = df[(df['startYear'] >= 1900) | (df['startYear'].isna())]
        
        return df
    
    def _clean_title_ratings(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpeza específica para title.ratings.tsv.gz"""
        # Converte tipos
        df['averageRating'] = pd.to_numeric(df['averageRating'], errors='coerce')
        df['numVotes'] = pd.to_numeric(df['numVotes'], errors='coerce')
        
        # Remove ratings inválidos
        df = df[(df['averageRating'] >= 1) & (df['averageRating'] <= 10)]
        df = df[df['numVotes'] >= 5]  # Pelo menos 5 votos
        
        return df
    
    def _clean_title_crew(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpeza específica para title.crew.tsv.gz"""
        return df
    
    def _clean_title_principals(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpeza específica para title.principals.tsv.gz"""
        # Converte tipos
        df['ordering'] = pd.to_numeric(df['ordering'], errors='coerce')
        return df
    
    def _save_parquet_to_s3(self, df: pd.DataFrame, bucket: str, key: str):
        """Salva DataFrame como Parquet no S3"""
        # Converte DataFrame para Parquet em memória
        parquet_buffer = StringIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_data = parquet_buffer.getvalue().encode('utf-8')
        
        # Upload para S3
        self.s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=parquet_data,
            ServerSideEncryption='AES256',
            ContentType='application/octet-stream'
        )
        
        logger.info(f"Parquet salvo: s3://{bucket}/{key}")
    
    def run_full_pipeline(self, file_types: List[str], force_refresh: bool = False):
        """
        Executa pipeline completo: Local → RAW → TRUSTED → REFINED
        
        Args:
            file_types: Lista de tipos de arquivo para processar
            force_refresh: Se deve reprocessar mesmo se já existir
        """
        logger.info("🚀 Iniciando pipeline completo IMDb")
        start_time = datetime.now()
        
        try:
            # Step 1: Upload para RAW
            logger.info("📤 Step 1: Upload para S3-RAW")
            uploaded = self.upload_raw_data(file_types)
            
            # Step 2: Processo para TRUSTED/REFINED
            logger.info("🔄 Step 2: Processamento para TRUSTED/REFINED")
            processed = self.process_to_trusted(file_types)
            
            # Log final
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"✅ Pipeline concluído em {execution_time:.2f}s")
            logger.info(f"Arquivos processados: {list(processed.keys())}")
            logger.info(f"Total de registros: {sum(processed.values())}")
            
        except Exception as e:
            logger.error(f"❌ Erro no pipeline: {str(e)}")
            raise

def main():
    """Função principal para execução standalone"""
    logging.basicConfig(level=logging.INFO)
    
    ingester = IMDbIngester()
    file_types = ['basics', 'ratings']  # Começar com os principais
    
    ingester.run_full_pipeline(file_types)

if __name__ == "__main__":
    main()
