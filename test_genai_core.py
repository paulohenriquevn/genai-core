#!/usr/bin/env python3
"""
Script de teste para demonstração do GenAI Core.

Este script demonstra as principais funcionalidades do sistema GenAI Core:
- Inicialização com configurações
- Carregamento de fontes de dados
- Processamento de consultas em linguagem natural
- Exibição de resultados
"""

import os
import sys
import pandas as pd
import logging
from typing import Dict, Any

# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_genai_core")

# Importa o GenAI Core
from genai_core import GenAICore
from genai_core.config import Settings
from genai_core.utils import setup_logging


def create_sample_data() -> Dict[str, str]:
    """
    Cria arquivos de dados de exemplo para teste.
    
    Returns:
        Dicionário com caminhos para os arquivos criados
    """
    # Cria diretório de dados se não existir
    os.makedirs('data', exist_ok=True)
    
    # Cria arquivo CSV de vendas
    vendas_df = pd.DataFrame({
        'data': ['2025-01-01', '2025-01-02', '2025-01-03', '2025-01-04', '2025-01-05'],
        'cliente': ['Cliente A', 'Cliente B', 'Cliente A', 'Cliente C', 'Cliente B'],
        'produto': ['Produto X', 'Produto Y', 'Produto Z', 'Produto X', 'Produto Z'],
        'categoria': ['Eletrônicos', 'Móveis', 'Eletrônicos', 'Eletrônicos', 'Móveis'],
        'valor': [100.0, 150.0, 200.0, 120.0, 180.0],
        'quantidade': [1, 2, 1, 3, 2]
    })
    vendas_path = 'data/vendas.csv'
    vendas_df.to_csv(vendas_path, index=False)
    logger.info(f"Arquivo de vendas criado: {vendas_path} ({len(vendas_df)} registros)")
    
    # Cria arquivo CSV de clientes
    clientes_df = pd.DataFrame({
        'nome': ['Cliente A', 'Cliente B', 'Cliente C', 'Cliente D'],
        'cidade': ['São Paulo', 'Rio de Janeiro', 'Belo Horizonte', 'Curitiba'],
        'tipo': ['Premium', 'Standard', 'Premium', 'Standard'],
        'limite_credito': [10000, 5000, 8000, 3000]
    })
    clientes_path = 'data/clientes.csv'
    clientes_df.to_csv(clientes_path, index=False)
    logger.info(f"Arquivo de clientes criado: {clientes_path} ({len(clientes_df)} registros)")
    
    return {
        'vendas': vendas_path,
        'clientes': clientes_path
    }


def run_test_queries(genai: GenAICore) -> None:
    """
    Executa consultas de teste no sistema GenAI Core.
    
    Args:
        genai: Instância do GenAI Core inicializada
    """
    # Lista de consultas de teste em linguagem natural
    queries = [
        "Mostre todas as vendas",
        "Qual o total de vendas por cliente?",
        "Quais são os produtos da categoria Eletrônicos?",
        "Qual cliente fez mais compras?",
        "Mostre o total de vendas por categoria"
    ]
    
    # Executa cada consulta
    for i, query in enumerate(queries, 1):
        logger.info(f"\n--- Consulta {i}: '{query}' ---")
        
        try:
            # Processa a consulta
            result = genai.process_query(query)
            
            # Exibe o resultado formatado
            print(f"\nConsulta: {query}")
            print("-" * 50)
            
            if result.get("success", False):
                data = result.get("data", {})
                
                if result.get("type") == "table":
                    # Converte para DataFrame para exibição formatada
                    if "data" in data and isinstance(data["data"], list):
                        df = pd.DataFrame(data["data"])
                        print(f"Resultado ({len(df)} registros):")
                        print(df.head(10))  # Limita a 10 registros na exibição
                    else:
                        print("Resultado vazio ou formato inesperado")
                else:
                    print(f"Resultado ({result.get('type')}):")
                    print(data)
            else:
                print(f"Erro: {result.get('error', 'Erro desconhecido')}")
            
            print("-" * 50)
            
        except Exception as e:
            logger.error(f"Erro ao processar consulta: {str(e)}")
            print(f"Erro: {str(e)}")


def main() -> int:
    """
    Função principal do script de teste.
    
    Returns:
        Código de saída (0 = sucesso, 1 = erro)
    """
    try:
        logger.info("Iniciando teste do GenAI Core")
        
        # Configura logging detalhado
        setup_logging(log_level="debug")
        
        # Cria dados de exemplo
        sample_data_paths = create_sample_data()
        
        # Inicializa as configurações
        settings = Settings()
        
        # Configura para usar o modo mock para LLM (sem API externa)
        settings.set("llm_type", "mock")
        
        # Ativa o executor SQL com DuckDB
        settings.set("sql_dialect", "duckdb")
        
        # Inicializa o sistema GenAI Core
        logger.info("Inicializando GenAI Core")
        genai = GenAICore(settings)
        
        # Não é mais necessário substituir o processador NLP,
        # pois o GenAICore já está configurado para usar o modo mock
        
        # Carrega as fontes de dados
        logger.info("Carregando fontes de dados")
        
        # Carrega vendas com CSV simples em vez de DuckDB
        genai.load_data_source({
            "id": "vendas",
            "type": "csv",
            "path": "data/vendas_test.csv"
        })
        
        # Hack - also register as 'dados' for mock processor
        genai.load_data_source({
            "id": "dados",
            "type": "csv",
            "path": "data/vendas_test.csv"
        })
        
        # Carrega clientes com CSV comum
        genai.load_data_source({
            "id": "clientes",
            "type": "csv",
            "path": sample_data_paths["clientes"]
        })
        
        # Executa consultas de teste
        run_test_queries(genai)
        
        # Fecha o sistema e libera recursos
        logger.info("Teste concluído com sucesso")
        
        return 0
        
    except Exception as e:
        logger.error(f"Erro no teste: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())