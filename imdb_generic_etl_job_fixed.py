"""
Glue Job - IMDb Generic ETL - VERSÃO CORRIGIDA
Script genérico que pode processar qualquer arquivo IMDb
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from datetime import datetime

# Parse argumentos essenciais + arquivo específico
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_file', 'file_type'])

# Setup Spark/Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f" Iniciando Glue Job: {args['JOB_NAME']}")
print(f" Arquivo: {args['source_file']}")
print(f" Tipo: {args['file_type']}")

def process_imdb_generic():
    """Processa qualquer arquivo IMDb com limpeza básica"""
    
    # STEP 1: Ler dados RAW do S3
    print(" STEP 1: Lendo dados RAW...")
    
    source_path = f"s3://imdb-raw-data-718942601863/imdb/raw/{args['source_file']}"
    
    # Ler TSV comprimido
    df_raw = spark.read \
        .option("header", "true") \
        .option("sep", "\t") \
        .csv(source_path)
    
    print(f" Dados RAW carregados: {df_raw.count()} registros")
    
    # STEP 2: Limpeza básica → TRUSTED
    print(" STEP 2: Processando RAW → TRUSTED...")
    
    # Converter campos nulos IMDb (\\N) para NULL real
    df_clean = df_raw
    for col_name in df_raw.columns:
        df_clean = df_clean.withColumn(col_name, 
            when(col(col_name) == "\\N", None).otherwise(col(col_name)))
    
    # Filtrar apenas registros com primeira coluna não nula (chave primária)
    first_col = df_raw.columns[0]
    df_clean = df_clean.filter(
        (col(first_col).isNotNull()) & 
        (trim(col(first_col)) != "")
    )
    
    # Adicionar timestamp
    current_time = datetime.now()
    df_clean = df_clean.withColumn("ingestion_timestamp", lit(current_time)) \
                      .withColumn("ingestion_year", lit(current_time.year)) \
                      .withColumn("ingestion_month", lit(current_time.month)) \
                      .withColumn("ingestion_day", lit(current_time.day))
    
    print(f" Dados limpos: {df_clean.count()} registros")
    
    # STEP 3: Salvar TRUSTED
    trusted_path = f"s3://imdb-trusted-data-718942601863/imdb/{args['file_type']}"
    
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("ingestion_year", "ingestion_month", "ingestion_day") \
        .parquet(trusted_path)
    
    print(f" TRUSTED salvo: {trusted_path}")
    
    # STEP 4: Features básicas → REFINED
    print("🔧 STEP 4: Processando TRUSTED → REFINED...")
    
    # Features ML básicas (genéricas)
    df_features = df_clean
    
    # Contar campos não nulos por linha - CORRIGIDO
    non_null_cols = [when(col(c).isNotNull(), 1).otherwise(0) for c in df_clean.columns if c not in ['ingestion_timestamp', 'ingestion_year', 'ingestion_month', 'ingestion_day']]
    
    # Somar usando reduce ao invés de sum() direto
    from functools import reduce
    from operator import add
    completeness_expr = reduce(add, non_null_cols) if non_null_cols else lit(0)
    df_features = df_features.withColumn("completeness_score", completeness_expr)
    
    # Adicionar tamanho do registro principal (primeira coluna)
    if first_col:
        df_features = df_features.withColumn("primary_key_length", length(col(first_col)))
    
    # STEP 5: Salvar REFINED
    refined_path = f"s3://imdb-refined-data-718942601863/imdb/{args['file_type']}"
    
    df_features.write \
        .mode("overwrite") \
        .partitionBy("ingestion_year", "ingestion_month", "ingestion_day") \
        .parquet(refined_path)
    
    print(f" REFINED salvo: {refined_path}")
    print(" Glue Job concluído com sucesso!")

# Executar processamento
if __name__ == "__main__":
    process_imdb_generic()
    job.commit()
