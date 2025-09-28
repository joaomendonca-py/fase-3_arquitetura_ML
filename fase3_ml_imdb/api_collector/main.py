"""
API de Coleta de Dados - IMDb
Tech Challenge Fase 3

API pública para ingestão e processamento de dados IMDb
Seguindo padrão das Fases 1 e 2
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Optional
import os
import logging
from datetime import datetime

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicialização da API
app = FastAPI(
    title="IMDb Data Collector API",
    description="API para coleta e processamento de dados IMDb - Tech Challenge Fase 3",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# === MODELOS PYDANTIC ===

class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    version: str
    environment: str

class DataIngestionRequest(BaseModel):
    source: str = "local"  # local, s3, api
    file_types: List[str] = ["basics", "ratings", "crew", "principals"]
    force_refresh: bool = False

class DataIngestionResponse(BaseModel):
    status: str
    message: str
    files_processed: List[str]
    records_count: Dict[str, int]
    execution_time: float

class MovieData(BaseModel):
    tconst: str
    title_type: Optional[str] = None
    primary_title: Optional[str] = None
    start_year: Optional[int] = None
    runtime_minutes: Optional[int] = None
    genres: Optional[str] = None
    average_rating: Optional[float] = None
    num_votes: Optional[int] = None

# === ENDPOINTS ===

@app.get("/", response_model=Dict[str, str])
async def root():
    """Endpoint raiz da API"""
    return {
        "message": "IMDb Data Collector API - Tech Challenge Fase 3",
        "docs": "/docs",
        "health": "/healthz",
        "version": "1.0.0"
    }

@app.get("/healthz", response_model=HealthResponse)
async def health_check():
    """Health check da API"""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now(),
        version="1.0.0",
        environment=os.getenv("TC_STAGE", "dev")
    )

@app.post("/v1/imdb/ingest", response_model=DataIngestionResponse)
async def ingest_imdb_data(
    request: DataIngestionRequest,
    background_tasks: BackgroundTasks
):
    """
    Ingere dados IMDb para S3 (RAW → TRUSTED → REFINED)
    
    Processa arquivos .tsv.gz locais e armazena no pipeline S3
    """
    try:
        # Import aqui para evitar circular imports
        from ..data_pipeline.ingest_imdb import IMDbIngester
        
        logger.info(f"Iniciando ingestão IMDb: {request.dict()}")
        
        ingester = IMDbIngester()
        
        # Executa ingestão em background
        background_tasks.add_task(
            ingester.run_full_pipeline,
            file_types=request.file_types,
            force_refresh=request.force_refresh
        )
        
        return DataIngestionResponse(
            status="started",
            message="Ingestão iniciada em background",
            files_processed=request.file_types,
            records_count={},
            execution_time=0.0
        )
        
    except Exception as e:
        logger.error(f"Erro na ingestão: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro na ingestão: {str(e)}")

@app.get("/v1/imdb/status")
async def get_ingestion_status():
    """Status da última ingestão de dados"""
    try:
        # TODO: Implementar lógica de status
        return {
            "status": "ready",
            "last_update": datetime.now(),
            "records_available": {
                "raw": 0,
                "trusted": 0,
                "refined": 0
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/imdb/movies/{movie_id}")
async def get_movie_data(movie_id: str):
    """
    Busca dados de um filme específico
    
    Args:
        movie_id: ID do filme (formato: ttXXXXXXX)
    """
    try:
        # TODO: Implementar busca de dados do filme
        # Por enquanto retorna mock data
        return {
            "tconst": movie_id,
            "primary_title": "Movie Title",
            "average_rating": 7.5,
            "num_votes": 10000,
            "message": "Dados mockados - implementar busca real"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/imdb/movies", response_model=List[MovieData])
async def bulk_movie_lookup(movie_ids: List[str]):
    """
    Busca dados de múltiplos filmes
    
    Args:
        movie_ids: Lista de IDs de filmes
    """
    try:
        # TODO: Implementar busca em lote
        results = []
        for movie_id in movie_ids[:10]:  # Limit para evitar sobrecarga
            results.append(MovieData(
                tconst=movie_id,
                primary_title=f"Movie {movie_id}",
                average_rating=7.0,
                num_votes=5000
            ))
        
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# === STARTUP EVENTS ===

@app.on_event("startup")
async def startup_event():
    """Inicialização da API"""
    logger.info("🚀 Iniciando IMDb Data Collector API")
    logger.info(f"Environment: {os.getenv('TC_STAGE', 'dev')}")
    logger.info(f"AWS Region: {os.getenv('TC_REGION', 'us-east-1')}")

@app.on_event("shutdown")
async def shutdown_event():
    """Finalização da API"""
    logger.info("🔄 Finalizando IMDb Data Collector API")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=os.getenv("TC_API_HOST", "0.0.0.0"),
        port=int(os.getenv("TC_API_PORT", "8000")),
        reload=True
    )
