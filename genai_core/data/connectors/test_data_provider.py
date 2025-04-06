"""
Módulo para fornecer dados de teste para situações onde os conectores reais falham.
Isso é útil para testes automatizados e desenvolvimento.
"""

import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)

# Dados de teste para diferentes fontes
TEST_DATA = {
    "vendas": pd.DataFrame({
        'data': ['2025-01-01', '2025-01-02', '2025-01-03', '2025-01-04', '2025-01-05'],
        'cliente': ['Cliente A', 'Cliente B', 'Cliente A', 'Cliente C', 'Cliente B'],
        'produto': ['Produto X', 'Produto Y', 'Produto Z', 'Produto X', 'Produto Z'],
        'categoria': ['Eletronicos', 'Moveis', 'Eletronicos', 'Eletronicos', 'Moveis'],
        'valor': [100.0, 150.0, 200.0, 120.0, 180.0],
        'quantidade': [1, 2, 1, 3, 2]
    }),
    "dados": pd.DataFrame({
        'data': ['2025-01-01', '2025-01-02', '2025-01-03', '2025-01-04', '2025-01-05'],
        'cliente': ['Cliente A', 'Cliente B', 'Cliente A', 'Cliente C', 'Cliente B'],
        'produto': ['Produto X', 'Produto Y', 'Produto Z', 'Produto X', 'Produto Z'],
        'categoria': ['Eletronicos', 'Moveis', 'Eletronicos', 'Eletronicos', 'Moveis'],
        'valor': [100.0, 150.0, 200.0, 120.0, 180.0],
        'quantidade': [1, 2, 1, 3, 2]
    }),
    "clientes": pd.DataFrame({
        'nome': ['Cliente A', 'Cliente B', 'Cliente C', 'Cliente D'],
        'cidade': ['São Paulo', 'Rio de Janeiro', 'Belo Horizonte', 'Curitiba'],
        'tipo': ['Premium', 'Standard', 'Premium', 'Standard'],
        'limite_credito': [10000, 5000, 8000, 3000]
    })
}

def add_test_data_source(genai_core, source_id):
    """
    Adiciona uma fonte de dados de teste no GenAICore.
    Cria um objeto mock que irá retornar dados de teste pré-definidos.
    
    Args:
        genai_core: Instância do GenAICore
        source_id: Identificador da fonte de dados
    """
    # Primeiro, marca ambiente como modo de teste
    os.environ['GENAI_TEST_MODE'] = '1'
    
    # Cria uma classe mock para o conector
    class MockTestConnector:
        def __init__(self, source_id):
            self.source_id = source_id
            logger.info(f"Criando conector de teste para {source_id}")
            
        def connect(self):
            logger.info(f"Conectando ao conector de teste {self.source_id}")
            
        def read_data(self, query=None):
            logger.info(f"Lendo dados de teste para {self.source_id}")
            if self.source_id in TEST_DATA:
                logger.info(f"Retornando {len(TEST_DATA[self.source_id])} registros de teste")
                return TEST_DATA[self.source_id]
            else:
                logger.warning(f"Nenhum dado de teste disponível para {self.source_id}")
                return pd.DataFrame()
                
        def close(self):
            logger.info(f"Fechando conector de teste {self.source_id}")
            
        def is_connected(self):
            return True
            
        def get_schema(self):
            if self.source_id in TEST_DATA:
                df = TEST_DATA[self.source_id]
                schema = pd.DataFrame({
                    'column_name': df.columns,
                    'column_type': [str(df[col].dtype) for col in df.columns]
                })
                return schema
            else:
                return pd.DataFrame(columns=['column_name', 'column_type'])
            
        def sample_data(self, num_rows=5):
            if self.source_id in TEST_DATA:
                return TEST_DATA[self.source_id].head(num_rows)
            else:
                return pd.DataFrame()
        
    # Cria o conector mock
    mock_connector = MockTestConnector(source_id)
    
    # Adiciona o conector no cache do GenAICore
    genai_core.connectors[source_id] = mock_connector
    
    # Registra info nos logs
    logger.info(f"Dados de teste para {source_id} adicionados com sucesso")
    
    return mock_connector