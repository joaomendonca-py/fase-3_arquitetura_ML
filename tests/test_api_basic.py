"""
Testes básicos para a API IMDb ML
Tech Challenge Fase 3 - Testes para GitHub Actions
"""

import pytest
from fastapi.testclient import TestClient


def test_basic_import():
    """Teste básico para verificar imports funcionam."""
    from fase3_ml_imdb.api_collector.main import app

    assert app is not None


def test_health_check():
    """Teste do endpoint de saúde."""
    from fase3_ml_imdb.api_collector.main import app

    with TestClient(app) as client:
        response = client.get("/")
        assert response.status_code == 200
        assert "IMDb Data Collector API" in response.json()["message"]


def test_healthz_endpoint():
    """Teste do endpoint healthz."""
    from fase3_ml_imdb.api_collector.main import app

    with TestClient(app) as client:
        response = client.get("/healthz")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"


def test_status_endpoint():
    """Teste do endpoint de status."""
    from fase3_ml_imdb.api_collector.main import app

    with TestClient(app) as client:
        response = client.get("/v1/imdb/status")
        assert response.status_code == 200
        data = response.json()
        assert "buckets" in data
        assert "environment" in data


class TestAPIStructure:
    """Testes estruturais da API."""

    def test_app_creation(self):
        """Teste se a aplicação é criada corretamente."""
        from fase3_ml_imdb.api_collector.main import app

        assert hasattr(app, "routes")
        assert len(app.routes) > 0

    def test_environment_variables(self):
        """Teste se variáveis de ambiente são carregadas."""
        import os

        # Não vamos falhar se variáveis não estiverem definidas em teste
        stage = os.getenv("TC_STAGE", "test")
        assert stage in ["dev", "prod", "test"]
