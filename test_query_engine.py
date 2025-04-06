#!/usr/bin/env python3
"""
Teste da classe QueryEngine.

Este script demonstra como usar o sistema unificado com a estrutura genai_core.
"""

import os
import pandas as pd
import logging

# Configuração básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_query_engine")

# Importar componentes
from genai_core.core import GenAICore, QueryEngine
from genai_core.nlp.nlp_processor import NLPProcessor
from genai_core.sql.sql_generator import SQLGenerator
from genai_core.data.connectors import DuckDBConnector, DataSourceConfig


def create_sample_data():
    """Cria um arquivo CSV de exemplo para teste."""
    os.makedirs('data', exist_ok=True)
    
    # Criar dados de vendas
    vendas_df = pd.DataFrame({
        'data': ['2025-01-01', '2025-01-02', '2025-01-03', '2025-01-04', '2025-01-05'],
        'cliente': ['Cliente A', 'Cliente B', 'Cliente A', 'Cliente C', 'Cliente B'],
        'produto': ['Produto X', 'Produto Y', 'Produto Z', 'Produto X', 'Produto Z'],
        'categoria': ['Eletronicos', 'Moveis', 'Eletronicos', 'Eletronicos', 'Moveis'],
        'valor': [100.0, 150.0, 200.0, 120.0, 180.0],
        'quantidade': [1, 2, 1, 3, 2]
    })
    
    # Salvar CSV
    path = 'data/vendas_teste.csv'
    vendas_df.to_csv(path, index=False)
    logger.info(f"Dados de teste criados em {path}")
    return path


def test_direct_query_engine(data_path):
    """Testa o QueryEngine diretamente."""
    # Definir variável de ambiente para modo de teste
    os.environ['GENAI_TEST_MODE'] = '1'
    
    # 1. Criar componentes
    logger.info("Inicializando componentes...")
    nlp = NLPProcessor(model="mock")
    sql_gen = SQLGenerator(dialect="duckdb")
    
    # 2. Configurar conector
    config = DataSourceConfig.from_dict({
        "id": "vendas",
        "type": "duckdb",
        "path": data_path,
        "file_type": "csv"
    })
    
    # 3. Inicializar conector
    connector = DuckDBConnector(config)
    connector.connect()
    
    # 4. Criar QueryEngine
    logger.info("Criando QueryEngine...")
    engine = QueryEngine(nlp, sql_gen, connector, debug_mode=True)
    
    # 5. Testar consultas
    consultas = [
        "Mostre todas as vendas",
        "Qual o total de vendas por cliente?",
        "Quais produtos são da categoria Eletronicos?",
        "Qual cliente fez mais compras?",
        "Mostre o total de vendas por categoria"
    ]
    
    logger.info("Executando consultas com QueryEngine direto...")
    for i, consulta in enumerate(consultas, 1):
        logger.info(f"\n--- Consulta {i}: '{consulta}' ---")
        
        try:
            # Obter explicação da consulta (para debug)
            explanation = engine.explain_query(consulta)
            logger.info(f"SQL a ser executado: {explanation['sql']}")
            
            # Executar consulta e mostrar resultados
            df = engine.run_query(consulta)
            
            print(f"\nResultado para: {consulta}")
            print("-" * 50)
            print(df.head())
            print("-" * 50)
            
        except Exception as e:
            logger.error(f"Erro ao executar consulta: {str(e)}")
    
    # 6. Testar execução direta de SQL
    logger.info("\n--- Teste de execução direta de SQL com QueryEngine ---")
    try:
        sql = "SELECT cliente, SUM(valor) as total FROM vendas GROUP BY cliente ORDER BY total DESC"
        df = engine.execute_sql(sql)
        
        print("\nResultado da execução direta de SQL:")
        print("-" * 50)
        print(df.head())
        print("-" * 50)
    except Exception as e:
        logger.error(f"Erro ao executar SQL direto: {str(e)}")
    
    # 7. Fechar recursos
    logger.info("Fechando recursos do QueryEngine...")
    engine.close()


def test_genai_core(data_path):
    """Testa o GenAICore."""
    # Definir variável de ambiente para modo de teste
    os.environ['GENAI_TEST_MODE'] = '1'
    
    # 1. Criar GenAICore
    logger.info("Inicializando GenAICore...")
    core = GenAICore({
        "llm_type": "mock",
        "sql_dialect": "duckdb",
        "debug_mode": True
    })
    
    # 2. Carregar fonte de dados
    logger.info("Carregando fonte de dados...")
    config = {
        "id": "vendas",
        "type": "duckdb",
        "path": data_path,
        "file_type": "csv"
    }
    
    core.load_data_source(config)
    
    # 3. Testar consultas
    consultas = [
        "Mostre todas as vendas",
        "Qual o total de vendas por cliente?",
        "Quais produtos são da categoria Eletronicos?"
    ]
    
    logger.info("Executando consultas com GenAICore...")
    for i, consulta in enumerate(consultas, 1):
        logger.info(f"\n--- Consulta {i}: '{consulta}' ---")
        
        try:
            # Executar consulta
            resultado = core.process_query(consulta, "vendas")
            
            print(f"\nResultado para: {consulta}")
            print("-" * 50)
            if resultado["success"]:
                if resultado["type"] == "table":
                    df = pd.DataFrame(resultado["data"]["data"])
                    print(df.head())
                else:
                    print(resultado)
            else:
                print(f"Erro: {resultado['error']}")
            print("-" * 50)
            
        except Exception as e:
            logger.error(f"Erro ao executar consulta: {str(e)}")
    
    # 4. Fechar recursos
    logger.info("Fechando recursos do GenAICore...")
    core.close()


if __name__ == "__main__":
    # Criar dados de teste
    data_path = create_sample_data()
    
    # Testar o QueryEngine diretamente
    print("\n=== TESTE DO QUERYENGINE DIRETAMENTE ===\n")
    test_direct_query_engine(data_path)
    
    # Testar o GenAICore
    print("\n=== TESTE DO GENAICORE ===\n")
    test_genai_core(data_path)
    
    logger.info("Testes concluídos com sucesso!")