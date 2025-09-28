"""
Lambda Handler para FastAPI
Tech Challenge Fase 3 - Seguindo padrão das Fases 1 e 2

Deploy da API FastAPI via Lambda usando Mangum
Igual ao padrão usado nas fases anteriores
"""

from mangum import Mangum
from .main import app

# Handler para AWS Lambda (padrão das fases anteriores)
handler = Mangum(app, lifespan="off")

# Para compatibilidade com diferentes versões
lambda_handler = handler
