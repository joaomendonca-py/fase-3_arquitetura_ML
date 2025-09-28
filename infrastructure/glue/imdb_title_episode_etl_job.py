"""
Glue Job - IMDb Title Episode ETL
Tech Challenge Fase 3 - Seguindo padrão das Fases 1 e 2

Job Glue para processar dados IMDb title episode (episódios de séries):
RAW (.tsv.gz) → TRUSTED (Parquet limpo) → REFINED (Features ML)
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
from datetime import datetime

# Parse argumentos do Glue
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source-bucket',
    'source-key', 
    'file-type',
    'target-bucket-trusted',
    'target-bucket-refined'
])

# Setup Spark/Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f"🚀 Iniciando Glue Job: {args['JOB_NAME']}")
print(f"📁 Source: s3://{args['source-bucket']}/{args['source-key']}")
print(f"🎯 Target Trusted: {args['target-bucket-trusted']}")
print(f"🎯 Target Refined: {args['target-bucket-refined']}")

def process_imdb_title_episode():
    """Processa dados IMDb title episode seguindo arquitetura medalhão"""
    
    # STEP 1: Ler dados RAW do S3
    print("📤 STEP 1: Lendo dados RAW...")
    
    source_path = f"s3://{args['source-bucket']}/{args['source-key']}"
    
    # Ler TSV comprimido
    df_raw = spark.read \
        .option("header", "true") \
        .option("sep", "\t") \
        .csv(source_path)
    
    print(f"✅ Dados RAW carregados: {df_raw.count()} registros")
    
    # STEP 2: Limpar e validar → TRUSTED
    print("🧹 STEP 2: Processando RAW → TRUSTED...")
    
    df_trusted = clean_title_episode_data(df_raw)
    
    # Salvar TRUSTED
    trusted_path = f"s3://{args['target-bucket-trusted']}/imdb/{args['file-type']}"
    
    df_trusted.write \
        .mode("overwrite") \
        .partitionBy("ingestion_year", "ingestion_month", "ingestion_day") \
        .parquet(trusted_path)
    
    print(f"✅ TRUSTED salvo: {trusted_path}")
    
    # STEP 3: Feature Engineering → REFINED  
    print("🔧 STEP 3: Processando TRUSTED → REFINED...")
    
    df_refined = create_ml_features(df_trusted)
    
    # Salvar REFINED
    refined_path = f"s3://{args['target-bucket-refined']}/imdb/{args['file-type']}"
    
    df_refined.write \
        .mode("overwrite") \
        .partitionBy("ingestion_year", "ingestion_month", "ingestion_day") \
        .parquet(refined_path)
    
    print(f"✅ REFINED salvo: {refined_path}")
    
    # STEP 4: Catalogar no Glue Catalog
    print("📊 STEP 4: Catalogando tabelas...")
    
    catalog_tables(args['file-type'], trusted_path, refined_path)
    
    print("🎉 Glue Job concluído com sucesso!")

def clean_title_episode_data(df_raw: DataFrame) -> DataFrame:
    """Limpa e valida dados de title episode"""
    
    # Converter campos nulos IMDb (\\N) para NULL real
    df_clean = df_raw
    for col_name in df_raw.columns:
        df_clean = df_clean.withColumn(col_name, 
            when(col(col_name) == "\\N", None).otherwise(col(col_name)))
    
    # Converter campos numéricos
    df_clean = df_clean.withColumn("seasonNumber", col("seasonNumber").cast("integer")) \
                      .withColumn("episodeNumber", col("episodeNumber").cast("integer"))
    
    # Filtrar registros válidos
    df_clean = df_clean.filter(
        (col("tconst").isNotNull()) & 
        (col("parentTconst").isNotNull()) &
        (trim(col("tconst")) != "") &
        (trim(col("parentTconst")) != "")
    )
    
    # Adicionar metadados
    current_time = datetime.now()
    df_clean = df_clean.withColumn("ingestion_timestamp", lit(current_time)) \
                      .withColumn("ingestion_year", lit(current_time.year)) \
                      .withColumn("ingestion_month", lit(current_time.month)) \
                      .withColumn("ingestion_day", lit(current_time.day))
    
    return df_clean

def create_ml_features(df_trusted: DataFrame) -> DataFrame:
    """Cria features para machine learning"""
    
    df_features = df_trusted
    
    # Feature: tem número da temporada
    df_features = df_features.withColumn("has_season_number", 
                                       col("seasonNumber").isNotNull().cast("integer"))
    
    # Feature: tem número do episódio
    df_features = df_features.withColumn("has_episode_number", 
                                       col("episodeNumber").isNotNull().cast("integer"))
    
    # Feature: categoria da temporada
    df_features = df_features.withColumn("season_category",
        when(col("seasonNumber").isNull(), "unknown")
        .when(col("seasonNumber") == 1, "first_season")
        .when(col("seasonNumber") <= 3, "early_seasons")
        .when(col("seasonNumber") <= 7, "mid_seasons")
        .otherwise("later_seasons")
    )
    
    # Feature: categoria do episódio
    df_features = df_features.withColumn("episode_category",
        when(col("episodeNumber").isNull(), "unknown")
        .when(col("episodeNumber") == 1, "pilot")
        .when(col("episodeNumber") <= 5, "early_episode")
        .when(col("episodeNumber") <= 10, "mid_episode")
        .otherwise("later_episode")
    )
    
    # Feature: posição relativa na temporada (para episódios com números)
    df_features = df_features.withColumn("episode_position",
        when((col("episodeNumber").isNotNull()) & (col("episodeNumber") <= 3), "beginning")
        .when((col("episodeNumber").isNotNull()) & (col("episodeNumber") >= 20), "ending")
        .when(col("episodeNumber").isNotNull(), "middle")
        .otherwise("unknown")
    )
    
    return df_features

def catalog_tables(file_type: str, trusted_path: str, refined_path: str):
    """Cataloga tabelas no Glue Catalog para acesso via Athena"""
    
    glue_client = boto3.client('glue')
    database_name = "imdb_database"
    
    # Criar database se não existir
    try:
        glue_client.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': 'IMDb database para Tech Challenge Fase 3'
            }
        )
    except glue_client.exceptions.AlreadyExistsException:
        pass
    
    # Catalogar tabela TRUSTED
    table_trusted = f"imdb_{file_type}_trusted"
    create_table_definition(glue_client, database_name, table_trusted, trusted_path, "trusted")
    
    # Catalogar tabela REFINED  
    table_refined = f"imdb_{file_type}_refined"
    create_table_definition(glue_client, database_name, table_refined, refined_path, "refined")
    
    print(f"📊 Tabelas catalogadas: {table_trusted}, {table_refined}")

def create_table_definition(glue_client, database: str, table: str, location: str, layer: str):
    """Cria definição de tabela no Glue Catalog"""
    
    # Schema para title episode
    if layer == "trusted":
        columns = [
            {'Name': 'tconst', 'Type': 'string'},
            {'Name': 'parentTconst', 'Type': 'string'},
            {'Name': 'seasonNumber', 'Type': 'int'},
            {'Name': 'episodeNumber', 'Type': 'int'},
            {'Name': 'ingestion_timestamp', 'Type': 'timestamp'}
        ]
    else:  # refined
        columns = [
            {'Name': 'tconst', 'Type': 'string'},
            {'Name': 'parentTconst', 'Type': 'string'},
            {'Name': 'seasonNumber', 'Type': 'int'},
            {'Name': 'episodeNumber', 'Type': 'int'},
            {'Name': 'has_season_number', 'Type': 'int'},
            {'Name': 'has_episode_number', 'Type': 'int'},
            {'Name': 'season_category', 'Type': 'string'},
            {'Name': 'episode_category', 'Type': 'string'},
            {'Name': 'episode_position', 'Type': 'string'},
            {'Name': 'ingestion_timestamp', 'Type': 'timestamp'}
        ]
    
    try:
        glue_client.create_table(
            DatabaseName=database,
            TableInput={
                'Name': table,
                'StorageDescriptor': {
                    'Columns': columns,
                    'Location': location,
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    }
                },
                'PartitionKeys': [
                    {'Name': 'ingestion_year', 'Type': 'int'},
                    {'Name': 'ingestion_month', 'Type': 'int'},
                    {'Name': 'ingestion_day', 'Type': 'int'}
                ]
            }
        )
    except glue_client.exceptions.AlreadyExistsException:
        pass

# Executar processamento
if __name__ == "__main__":
    process_imdb_title_episode()
    job.commit()
