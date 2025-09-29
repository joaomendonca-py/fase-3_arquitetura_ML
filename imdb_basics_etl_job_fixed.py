"""
Glue Job - IMDb Title Basics ETL - VERSÃO CORRIGIDA
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

print(f"Iniciando Glue Job: {args['JOB_NAME']}")

def process_imdb_basics():
    """Processa dados IMDb title basics - versão simplificada"""
    
    # STEP 1: Ler dados RAW do S3 (hardcoded para teste)
    print("STEP 1: Lendo dados RAW...")
    
    source_path = "s3://imdb-raw-data-718942601863/imdb/raw/title.basics.tsv.gz"
    
    # Ler TSV comprimido
    df_raw = spark.read \
        .option("header", "true") \
        .option("sep", "\t") \
        .csv(source_path)
    
    print(f"Dados RAW carregados: {df_raw.count()} registros")
    
    # STEP 2: Limpeza básica → TRUSTED
    print("STEP 2: Processando RAW → TRUSTED...")
    
    # Converter campos nulos IMDb (\\N) para NULL real
    df_clean = df_raw
    for col_name in df_raw.columns:
        df_clean = df_clean.withColumn(col_name, 
            when(col(col_name) == "\\N", None).otherwise(col(col_name)))
    
    # Converter campos numéricos
    df_clean = df_clean.withColumn("startYear", col("startYear").cast("integer")) \
                      .withColumn("endYear", col("endYear").cast("integer")) \
                      .withColumn("runtimeMinutes", col("runtimeMinutes").cast("integer"))
    
    # Filtrar registros válidos
    df_clean = df_clean.filter(
        (col("tconst").isNotNull()) & 
        (col("titleType").isNotNull()) &
        (col("primaryTitle").isNotNull())
    )
    
    # Adicionar timestamp
    current_time = datetime.now()
    df_clean = df_clean.withColumn("ingestion_timestamp", lit(current_time)) \
                      .withColumn("ingestion_year", lit(current_time.year)) \
                      .withColumn("ingestion_month", lit(current_time.month)) \
                      .withColumn("ingestion_day", lit(current_time.day))
    
    print(f" Dados limpos: {df_clean.count()} registros")
    
    # STEP 3: Salvar TRUSTED
    trusted_path = "s3://imdb-trusted-data-718942601863/imdb/basics"
    
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("ingestion_year", "ingestion_month", "ingestion_day") \
        .parquet(trusted_path)
    
    print(f" TRUSTED salvo: {trusted_path}")
    
    # STEP 4: Feature Engineering → REFINED
    print("🔧 STEP 4: Processando TRUSTED → REFINED...")
    
    # Features ML básicas
    df_features = df_clean \
        .withColumn("title_length", length(col("primaryTitle"))) \
        .withColumn("has_runtime", col("runtimeMinutes").isNotNull().cast("integer")) \
        .withColumn("has_end_year", col("endYear").isNotNull().cast("integer")) \
        .withColumn("is_adult", col("isAdult").cast("integer")) \
        .withColumn("decade", 
            when(col("startYear").isNull(), "unknown")
            .when(col("startYear") < 1950, "classic")
            .when(col("startYear") < 1980, "golden_age")
            .when(col("startYear") < 2000, "modern")
            .otherwise("contemporary")
        ) \
        .withColumn("runtime_category",
            when(col("runtimeMinutes").isNull(), "unknown")
            .when(col("runtimeMinutes") <= 30, "short")
            .when(col("runtimeMinutes") <= 90, "standard")
            .when(col("runtimeMinutes") <= 150, "long")
            .otherwise("epic")
        )
    
    # STEP 5: Salvar REFINED
    refined_path = "s3://imdb-refined-data-718942601863/imdb/basics"
    
    df_features.write \
        .mode("overwrite") \
        .partitionBy("ingestion_year", "ingestion_month", "ingestion_day") \
        .parquet(refined_path)
    
    print(f" REFINED salvo: {refined_path}")
    print(" Glue Job concluído com sucesso!")

# Executar processamento
if __name__ == "__main__":
    process_imdb_basics()
    job.commit()
