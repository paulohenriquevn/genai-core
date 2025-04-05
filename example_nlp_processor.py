#!/usr/bin/env python3
"""
Exemplo de uso do processador NLP (NLPProcessor) com diferentess modelos.

Este script demonstra como usar a classe NLPProcessor para processar
consultas em linguagem natural em diferentes contextos e com diferentes
tipos de modelos.
"""

import os
import sys
import logging
from typing import Dict, Any
import json

# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("example_nlp_processor")

# Importa o NLPProcessor
from genai_core.nlp.nlp_processor import NLPProcessor
from genai_core.config.settings import Settings


def process_with_mock_model():
    """Demonstra o uso do processador NLP com o modelo mock (baseado em regras)."""
    logger.info("=== Exemplo 1: Usando o modelo mock (baseado em regras) ===")
    
    # Inicializa o processador com o modelo mock
    processor = NLPProcessor(model_name="mock")
    
    # Define um schema simplificado
    schema = {
        "data_sources": {
            "vendas": {
                "tables": {
                    "vendas": {
                        "columns": {
                            "data": "TIMESTAMP",
                            "cliente": "TEXT",
                            "produto": "TEXT",
                            "categoria": "TEXT",
                            "valor": "FLOAT",
                            "quantidade": "INTEGER"
                        }
                    }
                }
            }
        }
    }
    
    # Processa algumas consultas
    queries = [
        "Mostre todas as vendas",
        "Qual o total de vendas por cliente?",
        "Quais são os produtos da categoria Eletrônicos?",
        "Qual cliente fez mais compras?",
        "Mostre o total de vendas por categoria"
    ]
    
    for query in queries:
        logger.info(f"\nConsulta: {query}")
        result = processor.parse_question(query, schema)
        print(f"Intent: {result.get('intent')}")
        print(f"SQL Template: {result.get('sql_template')}")
        print(f"Parâmetros: {json.dumps(result.get('parameters'), indent=2)}")
        print(f"Tipo de resposta: {result.get('expected_response_type')}")
        print(f"Fonte de dados: {result.get('data_source')}")
        print("-" * 50)


def process_with_openai_model():
    """Demonstra o uso do processador NLP com o modelo OpenAI (se disponível)."""
    logger.info("=== Exemplo 2: Usando o modelo OpenAI (se disponível) ===")
    
    # Verifica se a chave da API está disponível
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        logger.warning("Chave da API OpenAI não encontrada. Pulando este exemplo.")
        return
    
    # Inicializa as configurações
    settings = Settings()
    settings.set("llm_type", "openai")
    settings.set("llm_api_key", api_key)
    settings.set("llm_model", "gpt-3.5-turbo")
    
    # Inicializa o processador com o modelo OpenAI
    processor = NLPProcessor(model_name="openai", settings=settings)
    
    # Define um schema detalhado
    schema = {
        "data_sources": {
            "vendas": {
                "tables": {
                    "vendas": {
                        "columns": {
                            "data": "TIMESTAMP",
                            "cliente": "TEXT",
                            "produto": "TEXT",
                            "categoria": "TEXT",
                            "valor": "FLOAT",
                            "quantidade": "INTEGER"
                        }
                    }
                }
            },
            "clientes": {
                "tables": {
                    "clientes": {
                        "columns": {
                            "nome": "TEXT",
                            "cidade": "TEXT",
                            "tipo": "TEXT",
                            "limite_credito": "FLOAT"
                        }
                    }
                }
            }
        }
    }
    
    # Processa uma consulta mais complexa
    query = "Quais são os 3 clientes que mais compraram produtos da categoria Eletrônicos no último mês?"
    
    logger.info(f"\nConsulta: {query}")
    try:
        result = processor.parse_question(query, schema)
        print(f"Intent: {result.get('intent')}")
        print(f"SQL Template: {result.get('sql_template')}")
        print(f"Parâmetros: {json.dumps(result.get('parameters'), indent=2)}")
        print(f"Tipo de resposta: {result.get('expected_response_type')}")
        print(f"Fonte de dados: {result.get('data_source')}")
    except Exception as e:
        logger.error(f"Erro ao processar com OpenAI: {str(e)}")
        print(f"Erro: {str(e)}")
    
    print("-" * 50)


def main():
    """Função principal do exemplo."""
    logger.info("Iniciando exemplo do processador NLP")
    
    # Exemplo com modelo mock (baseado em regras)
    process_with_mock_model()
    
    # Exemplo com modelo OpenAI (se disponível)
    process_with_openai_model()
    
    logger.info("Exemplo concluído")


if __name__ == "__main__":
    main()