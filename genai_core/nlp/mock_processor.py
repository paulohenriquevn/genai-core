"""
Implementação mock do processador NLP para testes sem APIs externas.
"""

import logging
import re
from typing import Dict, Any, Optional, List, Union

# Configuração de logging
logger = logging.getLogger(__name__)


class MockNLPProcessor:
    """
    Versão simplificada do processador NLP para testes.
    Simula respostas pré-definidas para consultas comuns.
    """
    
    def __init__(self):
        """Inicializa o processador mock com padrões de consultas pré-definidos."""
        self.patterns = [
            # Mostrar todos os dados
            {
                "regex": r"mostr[ae]|exib[ae]|list[ae]|apresent[ae]|selecion[ae] (tod[ao]s|cad[ao])",
                "intent": "consulta_dados",
                "sql_template": "SELECT * FROM {table} LIMIT 100",
                "parameters": {"table": "dados"},
                "expected_response_type": "table"
            },
            
            # Total por agrupamento
            {
                "regex": r"total|soma|montante|quanto.* por|agrupa[dr]|agrupad[oa]",
                "intent": "agregacao",
                "sql_template": "SELECT {group_col}, SUM({value_col}) as total FROM {table} GROUP BY {group_col} ORDER BY total DESC",
                "parameters": {"table": "dados", "group_col": "categoria", "value_col": "valor"},
                "expected_response_type": "table"
            },
            
            # Contagem por agrupamento
            {
                "regex": r"contagem|conte|quant[ao]s|númer[oa]|quantidade.* por",
                "intent": "agregacao",
                "sql_template": "SELECT {group_col}, COUNT(*) as contagem FROM {table} GROUP BY {group_col} ORDER BY contagem DESC",
                "parameters": {"table": "dados", "group_col": "categoria"},
                "expected_response_type": "table"
            },
            
            # Filtro por condição
            {
                "regex": r"(filtr[aoe]|onde|quando|que|cujo|com|da categoria|do tipo|da cidade) ([a-zA-ZÀ-ÿ\s]+)",
                "intent": "filtragem",
                "sql_template": "SELECT * FROM {table} WHERE {filter_col} = '{filter_value}'",
                "parameters": {"table": "dados", "filter_col": "categoria", "filter_value": "Eletrônicos"},
                "expected_response_type": "table"
            },
            
            # Maior ou mais
            {
                "regex": r"maior|mais|top|melhor|maior[ei]s|melhores",
                "intent": "classificacao",
                "sql_template": "SELECT * FROM {table} ORDER BY {order_col} DESC LIMIT {limit}",
                "parameters": {"table": "dados", "order_col": "valor", "limit": 5},
                "expected_response_type": "table"
            }
        ]
    
    def process(self, query: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Processa uma consulta em linguagem natural e retorna uma estrutura semântica simulada.
        
        Args:
            query: Consulta em linguagem natural
            context: Contexto adicional (opcional)
            
        Returns:
            Estrutura semântica simulada para a consulta
        """
        query_lower = query.lower()
        logger.info(f"Mock NLP processando: {query}")
        
        # Extrair informações das fontes de dados do contexto
        data_sources = {}
        if context and "data_sources" in context:
            data_sources = context["data_sources"]
        
        # Determinar a fonte de dados alvo
        target_source = None
        for source_id in data_sources.keys():
            if source_id.lower() in query_lower:
                target_source = source_id
                break
        
        # Se não encontrou explicitamente, usa a primeira fonte disponível
        if target_source is None and data_sources:
            target_source = list(data_sources.keys())[0]
            logger.debug(f"Choosing first available data source: {target_source}")
        elif target_source is None:
            target_source = "vendas"  # Fallback to vendas instead of dados
            logger.debug(f"No data sources available, using fallback: {target_source}")
            
        # Debug info
        logger.debug(f"Target source: {target_source}, Available sources: {data_sources.keys() if data_sources else 'None'}")
        
        # Tenta encontrar um padrão para a consulta
        for pattern in self.patterns:
            if re.search(pattern["regex"], query_lower):
                result = pattern.copy()
                
                # Ajusta a tabela alvo baseada no contexto
                if "table" in result["parameters"]:
                    result["parameters"]["table"] = target_source
                
                # Tenta extrair valores específicos da consulta
                
                # Detecta categorias ou tipos específicos
                category_match = re.search(r"(categoria|tipo) ['\"]?([a-zA-ZÀ-ÿ\s]+)['\"]?", query_lower)
                if category_match and "filter_col" in result["parameters"]:
                    result["parameters"]["filter_col"] = category_match.group(1)
                    result["parameters"]["filter_value"] = category_match.group(2).strip()
                
                # Detecta colunas para agrupamento
                group_match = re.search(r"por ([a-zA-ZÀ-ÿ\s]+)", query_lower)
                if group_match and "group_col" in result["parameters"]:
                    group_col = group_match.group(1).strip()
                    # Normaliza alguns termos comuns
                    if group_col in ["cliente", "clientes"]:
                        group_col = "cliente"
                    elif group_col in ["categoria", "categorias"]:
                        group_col = "categoria"
                    elif group_col in ["produto", "produtos"]:
                        group_col = "produto"
                    result["parameters"]["group_col"] = group_col
                
                # Adiciona metadados à resposta
                result["original_query"] = query
                result["data_source"] = target_source
                
                logger.info(f"Mock NLP encontrou padrão: {pattern['intent']}")
                return result
        
        # Fallback para consulta genérica se nenhum padrão corresponder
        logger.info("Mock NLP usando fallback genérico")
        return {
            "intent": "consulta_dados",
            "sql_template": "SELECT * FROM {table} LIMIT 10",
            "parameters": {"table": target_source},
            "expected_response_type": "table",
            "original_query": query,
            "data_source": target_source
        }