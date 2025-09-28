"""
Testes para pipeline de dados
Tech Challenge Fase 3 - Data Pipeline Tests
"""

from unittest.mock import Mock, patch

import pandas as pd
import pytest


def test_pandas_import():
    """Teste básico se pandas está disponível."""
    assert pd.__version__ is not None


def test_imdb_ingester_import():
    """Teste se conseguimos importar o IMDbIngester."""
    from fase3_ml_imdb.data_pipeline.ingest_imdb import IMDbIngester

    assert IMDbIngester is not None


class TestIMDbIngester:
    """Testes para o IMDbIngester."""

    def test_ingester_initialization(self):
        """Teste se o ingester inicializa corretamente."""
        from fase3_ml_imdb.data_pipeline.ingest_imdb import IMDbIngester

        ingester = IMDbIngester()
        assert ingester.bucket_raw is not None
        assert ingester.bucket_trusted is not None
        assert ingester.bucket_refined is not None
        assert ingester.bucket_models is not None

    def test_imdb_files_mapping(self):
        """Teste se mapeamento de arquivos IMDb está correto."""
        from fase3_ml_imdb.data_pipeline.ingest_imdb import IMDbIngester

        ingester = IMDbIngester()
        assert "basics" in ingester.imdb_files
        assert "ratings" in ingester.imdb_files
        assert ingester.imdb_files["ratings"] == "title.ratings.tsv.gz"

    @patch("boto3.client")
    def test_s3_client_creation(self, mock_boto3):
        """Teste se cliente S3 é criado (mockado)."""
        from fase3_ml_imdb.data_pipeline.ingest_imdb import IMDbIngester

        mock_s3 = Mock()
        mock_boto3.return_value = mock_s3

        ingester = IMDbIngester()
        # Como estamos mockando, verificamos se boto3 foi chamado
        mock_boto3.assert_called_with("s3")


def test_feature_engineering_functions():
    """Teste se funções de feature engineering existem."""
    from fase3_ml_imdb.data_pipeline.ingest_imdb import IMDbIngester

    ingester = IMDbIngester()
    # Verificar se métodos existem
    assert hasattr(ingester, "_clean_data")
    assert hasattr(ingester, "_feature_engineering")
    assert hasattr(ingester, "_create_ratings_features")
