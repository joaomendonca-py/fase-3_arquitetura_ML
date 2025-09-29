"""
Glue Job - IMDb Name Basics ETL
Tech Challenge Fase 3 - Seguindo padrão das Fases 1 e 2

Job Glue para processar dados IMDb name basics (pessoas/profissionais):
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

print(f" Iniciando Glue Job: {args['JOB_NAME']}")
print(f" Source: s3://{args['source-bucket']}/{args['source-key']}")
print(f" Target Trusted: {args['target-bucket-trusted']}")
print(f" Target Refined: {args['target-bucket-refined']}")

def process_imdb_name_basics():
    """Processa dados IMDb name basics seguindo arquitetura medalhão"""
    
    # STEP 1: Ler dados RAW do S3
    print(" STEP 1: Lendo dados RAW...")
    
    source_path = f"s3://{args['source-bucket']}/{args['source-key']}"
    
    # Ler TSV comprimido
    df_raw = spark.read \
        .option("header", "true") \
        .option("sep", "\t") \
        .csv(source_path)
    
    print(f" Dados RAW carregados: {df_raw.count()} registros")
    
    # STEP 2: Limpar e validar → TRUSTED
    print(" STEP 2: Processando RAW → TRUSTED...")
    
    df_trusted = clean_name_basics_data(df_raw)
    
    # Salvar TRUSTED
    trusted_path = f"s3://{args['target-bucket-trusted']}/imdb/{args['file-type']}"
    
    df_trusted.write \
        .mode("overwrite") \
        .partitionBy("ingestion_year", "ingestion_month", "ingestion_day") \
        .parquet(trusted_path)
    
    print(f" TRUSTED salvo: {trusted_path}")
    
    # STEP 3: Feature Engineering → REFINED  
    print("🔧 STEP 3: Processando TRUSTED → REFINED...")
    
    df_refined = create_ml_features(df_trusted)
    
    # Salvar REFINED
    refined_path = f"s3://{args['target-bucket-refined']}/imdb/{args['file-type']}"
    
    df_refined.write \
        .mode("overwrite") \
        .partitionBy("ingestion_year", "ingestion_month", "ingestion_day") \
        .parquet(refined_path)
    
    print(f" REFINED salvo: {refined_path}")
    
    # STEP 4: Catalogar no Glue Catalog
    print(" STEP 4: Catalogando tabelas...")
    
    catalog_tables(args['file-type'], trusted_path, refined_path)
    
    print(" Glue Job concluído com sucesso!")

def clean_name_basics_data(df_raw: DataFrame) -> DataFrame:
    """Limpa e valida dados de name basics"""
    
    # Converter campos nulos IMDb (\\N) para NULL real
    df_clean = df_raw
    for col_name in df_raw.columns:
        df_clean = df_clean.withColumn(col_name, 
            when(col(col_name) == "\\N", None).otherwise(col(col_name)))
    
    # Converter anos para integer
    df_clean = df_clean.withColumn("birthYear", col("birthYear").cast("integer")) \
                      .withColumn("deathYear", col("deathYear").cast("integer"))
    
    # Filtrar registros válidos
    df_clean = df_clean.filter(
        (col("nconst").isNotNull()) & 
        (col("primaryName").isNotNull()) &
        (trim(col("nconst")) != "") &
        (trim(col("primaryName")) != "")
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
    
    # Feature: está vivo (não tem data de morte)
    df_features = df_features.withColumn("is_alive", 
                                       col("deathYear").isNull().cast("integer"))
    
    # Feature: tem data de nascimento
    df_features = df_features.withColumn("has_birth_year", 
                                       col("birthYear").isNotNull().cast("integer"))
    
    # Feature: idade aproximada (se vivo)
    current_year = datetime.now().year
    df_features = df_features.withColumn("approximate_age",
        when((col("is_alive") == 1) & col("birthYear").isNotNull(), 
             current_year - col("birthYear")).otherwise(None))
    
    # Feature: anos de atividade (se morreu)
    df_features = df_features.withColumn("career_span",
        when((col("birthYear").isNotNull()) & (col("deathYear").isNotNull()), 
             col("deathYear") - col("birthYear")).otherwise(None))
    
    # Feature: comprimento do nome
    df_features = df_features.withColumn("name_length", length(col("primaryName")))
    
    # Feature: conta profissões (split por vírgula)
    df_features = df_features.withColumn("num_professions", 
        when(col("primaryProfession").isNotNull(), 
             size(split(col("primaryProfession"), ","))).otherwise(0))
    
    # Feature: conta títulos conhecidos (split por vírgula)
    df_features = df_features.withColumn("num_known_titles", 
        when(col("knownForTitles").isNotNull(), 
             size(split(col("knownForTitles"), ","))).otherwise(0))
    
    # Feature: é ator/atriz
    df_features = df_features.withColumn("is_actor", 
        when(col("primaryProfession").isNotNull(),
             col("primaryProfession").contains("actor") | 
             col("primaryProfession").contains("actress")).otherwise(False).cast("integer"))
    
    # Feature: é diretor
    df_features = df_features.withColumn("is_director", 
        when(col("primaryProfession").isNotNull(),
             col("primaryProfession").contains("director")).otherwise(False).cast("integer"))
    
    # Feature: é escritor
    df_features = df_features.withColumn("is_writer", 
        when(col("primaryProfession").isNotNull(),
             col("primaryProfession").contains("writer")).otherwise(False).cast("integer"))
    
    # Feature: categoria de fama (baseado em títulos conhecidos)
    df_features = df_features.withColumn("fame_category",
        when(col("num_known_titles") == 0, "unknown")
        .when(col("num_known_titles") == 1, "emerging")
        .when(col("num_known_titles") <= 3, "established")
        .otherwise("celebrity")
    )
    
    # Feature: geração (baseado no ano de nascimento)
    df_features = df_features.withColumn("generation",
        when(col("birthYear").isNull(), "unknown")
        .when(col("birthYear") < 1930, "classic_era")
        .when(col("birthYear") < 1950, "golden_age")
        .when(col("birthYear") < 1970, "new_hollywood")
        .when(col("birthYear") < 1990, "modern_era")
        .otherwise("contemporary")
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
    
    print(f" Tabelas catalogadas: {table_trusted}, {table_refined}")

def create_table_definition(glue_client, database: str, table: str, location: str, layer: str):
    """Cria definição de tabela no Glue Catalog"""
    
    # Schema para name basics
    if layer == "trusted":
        columns = [
            {'Name': 'nconst', 'Type': 'string'},
            {'Name': 'primaryName', 'Type': 'string'},
            {'Name': 'birthYear', 'Type': 'int'},
            {'Name': 'deathYear', 'Type': 'int'},
            {'Name': 'primaryProfession', 'Type': 'string'},
            {'Name': 'knownForTitles', 'Type': 'string'},
            {'Name': 'ingestion_timestamp', 'Type': 'timestamp'}
        ]
    else:  # refined
        columns = [
            {'Name': 'nconst', 'Type': 'string'},
            {'Name': 'primaryName', 'Type': 'string'},
            {'Name': 'birthYear', 'Type': 'int'},
            {'Name': 'deathYear', 'Type': 'int'},
            {'Name': 'primaryProfession', 'Type': 'string'},
            {'Name': 'knownForTitles', 'Type': 'string'},
            {'Name': 'is_alive', 'Type': 'int'},
            {'Name': 'has_birth_year', 'Type': 'int'},
            {'Name': 'approximate_age', 'Type': 'int'},
            {'Name': 'career_span', 'Type': 'int'},
            {'Name': 'name_length', 'Type': 'int'},
            {'Name': 'num_professions', 'Type': 'int'},
            {'Name': 'num_known_titles', 'Type': 'int'},
            {'Name': 'is_actor', 'Type': 'int'},
            {'Name': 'is_director', 'Type': 'int'},
            {'Name': 'is_writer', 'Type': 'int'},
            {'Name': 'fame_category', 'Type': 'string'},
            {'Name': 'generation', 'Type': 'string'},
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
    process_imdb_name_basics()
    job.commit()
