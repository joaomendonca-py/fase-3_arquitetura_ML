#!/usr/bin/env python3
"""
Script Glue MÍNIMO para teste
Apenas imprime argumentos e termina
"""

import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# Parse APENAS argumentos obrigatórios
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Setup básico
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(" TESTE MÍNIMO - SUCESSO!")
print(f"Job Name: {args['JOB_NAME']}")
print("Script executado com sucesso!")

# Finalizar
job.commit()
