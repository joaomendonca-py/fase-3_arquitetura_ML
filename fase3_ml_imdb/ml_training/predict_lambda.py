"""
Lambda Function - ML Prediction Endpoint
Tech Challenge Fase 3 - Seguindo padrão das Fases 1 e 2

Endpoint serverless para predições ML do modelo IMDb
Carrega modelo do S3 e serve predições
"""

import json
import logging
import os
from datetime import datetime
from io import BytesIO

import boto3
import joblib
import numpy as np
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Global variables para cache
model_cache = None
model_timestamp = None


def lambda_handler(event, context):
    """
    Handler principal para predições ML

    POST /v1/imdb/predict
    Body: {
        "movies": [
            {
                "numVotes": 1000,
                "log_votes": 6.9,
                "rating_normalized": 0.7,
                "rating_category": "good",
                "is_popular": 0
            }
        ]
    }
    """

    try:
        logger.info("🤖 Lambda ML Prediction iniciado")

        # Parse request
        if "body" in event:
            body = (
                json.loads(event["body"])
                if isinstance(event["body"], str)
                else event["body"]
            )
        else:
            body = event

        movies_data = body.get("movies", [])

        if not movies_data:
            return {
                "statusCode": 400,
                "body": json.dumps(
                    {
                        "error": 'Campo "movies" é obrigatório',
                        "example": {
                            "movies": [
                                {
                                    "numVotes": 1000,
                                    "log_votes": 6.9,
                                    "rating_normalized": 0.7,
                                    "rating_category": "good",
                                    "is_popular": 0,
                                }
                            ]
                        },
                    }
                ),
            }

        # Carregar modelo
        model_info = load_model_from_s3()
        model = model_info["model"]
        features = model_info["features"]

        # Processar predições
        predictions = []

        for movie_data in movies_data:
            try:
                # Preparar features
                df_input = prepare_features(movie_data, features)

                # Fazer predição
                rating_pred = model.predict(df_input)[0]

                # Calcular confiança (baseado em métricas do modelo)
                confidence = calculate_confidence(rating_pred, model_info)

                predictions.append(
                    {
                        "predicted_rating": round(float(rating_pred), 2),
                        "confidence": round(confidence, 3),
                        "rating_category": categorize_rating(rating_pred),
                        "business_opportunity": calculate_opportunity(
                            movie_data, rating_pred
                        ),
                    }
                )

            except Exception as e:
                predictions.append(
                    {
                        "error": f"Erro no processamento: {str(e)}",
                        "movie_data": movie_data,
                    }
                )

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps(
                {
                    "predictions": predictions,
                    "model_info": {
                        "timestamp": model_info["timestamp"],
                        "r2_cv": model_info["metrics"]["r2_cv"],
                        "n_features": len(features),
                    },
                    "processed_at": datetime.now().isoformat(),
                }
            ),
        }

    except Exception as e:
        logger.error(f"❌ Erro na predição: {str(e)}")

        return {
            "statusCode": 500,
            "body": json.dumps(
                {"error": str(e), "timestamp": datetime.now().isoformat()}
            ),
        }


def load_model_from_s3():
    """Carrega modelo do S3 com cache"""
    global model_cache, model_timestamp

    try:
        s3_client = boto3.client("s3")
        bucket = os.getenv("MODEL_BUCKET", "imdb-ml-models-718942601863")
        key = os.getenv("MODEL_KEY", "models/latest/imdb_rating_predictor.pkl")

        # Verificar se modelo já está em cache
        obj_info = s3_client.head_object(Bucket=bucket, Key=key)
        last_modified = obj_info["LastModified"]

        if model_cache is None or model_timestamp != last_modified:
            logger.info(f"📥 Carregando modelo: s3://{bucket}/{key}")

            # Download do modelo
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            model_data = obj["Body"].read()

            # Carregar com joblib
            model_info = joblib.load(BytesIO(model_data))

            # Atualizar cache
            model_cache = model_info
            model_timestamp = last_modified

            logger.info("✅ Modelo carregado e cacheado")
        else:
            logger.info("📋 Usando modelo do cache")

        return model_cache

    except Exception as e:
        logger.error(f"❌ Erro ao carregar modelo: {str(e)}")
        raise


def prepare_features(movie_data, expected_features):
    """Prepara features no formato esperado pelo modelo"""

    # Criar DataFrame com features
    df = pd.DataFrame([movie_data])

    # Aplicar one-hot encoding se necessário (igual ao notebook)
    if "rating_category" in movie_data:
        category_dummies = pd.get_dummies(df[["rating_category"]], prefix="cat")
        df = pd.concat([df, category_dummies], axis=1)

    if "is_popular" in movie_data:
        popular_dummies = pd.get_dummies(df[["is_popular"]], prefix="pop")
        df = pd.concat([df, popular_dummies], axis=1)

    # Garantir que todas as features esperadas estejam presentes
    for feature in expected_features:
        if feature not in df.columns:
            df[feature] = 0

    # Selecionar apenas as features do modelo
    df_final = df[expected_features].fillna(0)

    return df_final


def calculate_confidence(predicted_rating, model_info):
    """Calcula confiança da predição baseado nas métricas do modelo"""

    r2_cv = model_info["metrics"]["r2_cv"]

    # Confiança baseada em R² e distância da média
    base_confidence = r2_cv  # R² como base da confiança

    # Ajustar confiança baseado na faixa de rating
    if 4.0 <= predicted_rating <= 8.0:
        # Faixa central tem maior confiança
        confidence = base_confidence * 1.1
    else:
        # Extremos têm menor confiança
        confidence = base_confidence * 0.9

    return min(confidence, 0.95)  # Cap em 95%


def categorize_rating(rating):
    """Categoriza rating em faixas (igual ao notebook)"""
    if rating <= 4:
        return "poor"
    elif rating <= 6:
        return "fair"
    elif rating <= 7:
        return "good"
    elif rating <= 8:
        return "very_good"
    else:
        return "excellent"


def calculate_opportunity(movie_data, predicted_rating):
    """Calcula oportunidade de negócio (igual ao notebook)"""

    log_votes = movie_data.get("log_votes", 5.0)
    is_popular = movie_data.get("is_popular", 0)

    # Oportunidade = Rating Alto / Popularidade Baixa
    if predicted_rating >= 7.0 and is_popular == 0:
        opportunity_score = (predicted_rating / log_votes) * 100
        return {
            "score": round(opportunity_score, 2),
            "level": "HIGH" if opportunity_score > 1.5 else "MEDIUM",
            "reason": "Alto rating predito com baixa popularidade",
        }
    else:
        return {
            "score": 0.0,
            "level": "LOW",
            "reason": "Não atende critérios de oportunidade",
        }


# Para testes locais
if __name__ == "__main__":
    test_event = {
        "movies": [
            {
                "numVotes": 1000,
                "log_votes": 6.9,
                "rating_normalized": 0.7,
                "rating_category": "good",
                "is_popular": 0,
            }
        ]
    }

    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))
