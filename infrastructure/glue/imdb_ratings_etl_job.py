"""
Glue Job - IMDb Ratings ETL
Tech Challenge Fase 3 - Seguindo padrão das Fases 1 e 2

Job Glue para processar dados IMDb ratings:
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

# Parse argumentos do Glue - NOMES ÚNICOS PARA EVITAR CONFLITOS
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'tc-source-bucket',
    'tc-source-key',
    'tc-file-type', 
    'tc-target-bucket-trusted',
    'tc-target-bucket-refined',
    'tc-database-name'
])

# Usar valores dos argumentos
source_bucket = args['tc-source-bucket']
source_key = args['tc-source-key']
file_type = args['tc-file-type']

# Setup Spark/Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f" Iniciando Glue Job: {args['JOB_NAME']}")
print(f" Source: s3://{source_bucket}/{source_key}")
print(f" Target Trusted: {args['tc-target-bucket-trusted']}")
print(f" Target Refined: {args['tc-target-bucket-refined']}")
print(f" File Type: {file_type}")

def process_imdb_ratings():
    """Processa dados IMDb ratings seguindo arquitetura medalhão"""
    
    # STEP 1: Ler dados RAW do S3
    print(" STEP 1: Lendo dados RAW...")
    
    source_path = f"s3://{source_bucket}/{source_key}"
    
    # Ler TSV comprimido
    df_raw = spark.read \
        .option("header", "true") \
        .option("sep", "\t") \
        .csv(source_path)
    
    print(f" Dados RAW carregados: {df_raw.count()} registros")
    
    # STEP 2: Limpar e validar → TRUSTED
    print(" STEP 2: Processando RAW → TRUSTED...")
    
    df_trusted = clean_ratings_data(df_raw)
    
    # Salvar TRUSTED
    trusted_path = f"s3://{args['tc-target-bucket-trusted']}/imdb/{file_type}"
    
    df_trusted.write \
        .mode("overwrite") \
        .partitionBy("ingestion_year", "ingestion_month", "ingestion_day") \
        .parquet(trusted_path)
    
    print(f" TRUSTED salvo: {trusted_path}")
    
    # STEP 3: Feature Engineering → REFINED  
    print("🔧 STEP 3: Processando TRUSTED → REFINED...")
    
    df_refined = create_ml_features(df_trusted)
    
    # Salvar REFINED
    refined_path = f"s3://{args['tc-target-bucket-refined']}/imdb/{file_type}"
    
    df_refined.write \
        .mode("overwrite") \
        .partitionBy("ingestion_year", "ingestion_month", "ingestion_day") \
        .parquet(refined_path)
    
    print(f" REFINED salvo: {refined_path}")
    
    # STEP 4: Catalogar no Glue Catalog
    print(" STEP 4: Catalogando tabelas...")
    
    catalog_tables(file_type, trusted_path, refined_path)
    
    print(" Glue Job concluído com sucesso!")

def clean_ratings_data(df_raw: DataFrame) -> DataFrame:
    """Limpa e valida dados de ratings"""
    
    # Converter tipos
    df_clean = df_raw.withColumn("averageRating", col("averageRating").cast("double")) \
                    .withColumn("numVotes", col("numVotes").cast("integer"))
    
    # Filtrar dados válidos
    df_clean = df_clean.filter(
        (col("averageRating") >= 1.0) & 
        (col("averageRating") <= 10.0) &
        (col("numVotes") >= 5)
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
    
    # Feature: popularidade (log scale)
    df_features = df_features.withColumn("log_votes", log1p(col("numVotes")))
    
    # Feature: rating normalizado (0-1)
    df_features = df_features.withColumn("rating_normalized", 
                                       (col("averageRating") - 1) / 9)
    
    # Feature: categorias de rating
    df_features = df_features.withColumn("rating_category",
        when(col("averageRating") <= 4, "poor")
        .when(col("averageRating") <= 6, "fair") 
        .when(col("averageRating") <= 7, "good")
        .when(col("averageRating") <= 8, "very_good")
        .otherwise("excellent")
    )
    
    # Feature: filme popular (top 20% por votos)
    vote_threshold = df_features.approxQuantile("numVotes", [0.8], 0.01)[0]
    df_features = df_features.withColumn("is_popular", 
                                       (col("numVotes") >= vote_threshold).cast("integer"))
    
    return df_features

def catalog_tables(file_type: str, trusted_path: str, refined_path: str):
    """Cataloga tabelas no Glue Catalog para acesso via Athena"""
    
    glue_client = boto3.client('glue')
    database_name = args['tc-database-name']
    
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
    
    print(f" Tabelas catalogadas: {table_trusted}, {table_refined}")

def create_table_definition(glue_client, database: str, table: str, location: str, layer: str):
    """Cria definição de tabela no Glue Catalog"""
    
    # Schema básico para ratings (ajustar conforme necessário)
    if layer == "trusted":
        columns = [
            {'Name': 'tconst', 'Type': 'string'},
            {'Name': 'averageRating', 'Type': 'double'},
            {'Name': 'numVotes', 'Type': 'int'},
            {'Name': 'ingestion_timestamp', 'Type': 'timestamp'}
        ]
    else:  # refined
        columns = [
            {'Name': 'tconst', 'Type': 'string'},
            {'Name': 'averageRating', 'Type': 'double'},
            {'Name': 'numVotes', 'Type': 'int'},
            {'Name': 'log_votes', 'Type': 'double'},
            {'Name': 'rating_normalized', 'Type': 'double'},
            {'Name': 'rating_category', 'Type': 'string'},
            {'Name': 'is_popular', 'Type': 'int'},
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
    process_imdb_ratings()
    job.commit()
