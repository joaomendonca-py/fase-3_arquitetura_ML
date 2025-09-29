"""
Glue Job - IMDb Ratings ETL - VERSÃO CORRIGIDA
Tech Challenge Fase 3 - Script simplificado que funciona
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from datetime import datetime

# Parse APENAS argumentos essenciais - sem conflitos
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Setup Spark/Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f" Iniciando Glue Job: {args['JOB_NAME']}")

def process_imdb_ratings():
    """Processa dados IMDb ratings - versão simplificada"""
    
    # STEP 1: Ler dados RAW do S3 (hardcoded para teste)
    print(" STEP 1: Lendo dados RAW...")
    
    source_path = "s3://imdb-raw-data-718942601863/imdb/raw/title.ratings.tsv.gz"
    
    # Ler TSV comprimido
    df_raw = spark.read \
        .option("header", "true") \
        .option("sep", "\t") \
        .csv(source_path)
    
    print(f" Dados RAW carregados: {df_raw.count()} registros")
    
    # STEP 2: Limpeza básica → TRUSTED
    print(" STEP 2: Processando RAW → TRUSTED...")
    
    # Converter tipos básicos
    df_clean = df_raw.withColumn("averageRating", col("averageRating").cast("double")) \
                    .withColumn("numVotes", col("numVotes").cast("integer"))
    
    # Filtrar dados válidos
    df_clean = df_clean.filter(
        (col("averageRating") >= 1.0) & 
        (col("averageRating") <= 10.0) &
        (col("numVotes") >= 5)
    )
    
    # Adicionar timestamp
    current_time = datetime.now()
    df_clean = df_clean.withColumn("ingestion_timestamp", lit(current_time)) \
                      .withColumn("ingestion_year", lit(current_time.year)) \
                      .withColumn("ingestion_month", lit(current_time.month)) \
                      .withColumn("ingestion_day", lit(current_time.day))
    
    print(f" Dados limpos: {df_clean.count()} registros")
    
    # STEP 3: Salvar TRUSTED
    trusted_path = "s3://imdb-trusted-data-718942601863/imdb/ratings"
    
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("ingestion_year", "ingestion_month", "ingestion_day") \
        .parquet(trusted_path)
    
    print(f" TRUSTED salvo: {trusted_path}")
    
    # STEP 4: Feature Engineering → REFINED
    print("🔧 STEP 4: Processando TRUSTED → REFINED...")
    
    # Features ML básicas
    df_features = df_clean \
        .withColumn("log_votes", log1p(col("numVotes"))) \
        .withColumn("rating_normalized", (col("averageRating") - 1) / 9) \
        .withColumn("rating_category",
            when(col("averageRating") <= 4, "poor")
            .when(col("averageRating") <= 6, "fair") 
            .when(col("averageRating") <= 7, "good")
            .when(col("averageRating") <= 8, "very_good")
            .otherwise("excellent")
        )
    
    # STEP 5: Salvar REFINED
    refined_path = "s3://imdb-refined-data-718942601863/imdb/ratings"
    
    df_features.write \
        .mode("overwrite") \
        .partitionBy("ingestion_year", "ingestion_month", "ingestion_day") \
        .parquet(refined_path)
    
    print(f" REFINED salvo: {refined_path}")
    print(" Glue Job concluído com sucesso!")

# Executar processamento
if __name__ == "__main__":
    process_imdb_ratings()
    job.commit()
