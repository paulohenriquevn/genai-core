# -*- coding: utf-8 -*-
"""
Módulo responsável pelo processamento de linguagem natural.
Converte perguntas em linguagem natural em estruturas semânticas
que podem ser usadas para gerar consultas SQL.
"""

import logging
import re
import json
from typing import Dict, Any, Optional, List, Union

from genai_core.config.settings import Settings

# Configuração de logging
logger = logging.getLogger(__name__)


class NLPProcessor:
    """
    Processa consultas em linguagem natural e extrai estruturas semânticas.
    Usa modelos de linguagem para entender a intenção e extrair entidades.
    """
    
    def __init__(self, settings: Settings):
        """
        Inicializa o processador NLP com as configurações fornecidas.
        
        Args:
            settings: Configurações do sistema
        """
        self.settings = settings
        
        # Implementação simplificada para teste
        # Em uma implementação real, aqui seria inicializada a conexão com LLMs
        
        logger.info("NLPProcessor inicializado com sucesso")
    
    def process(self, query: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Processa uma consulta em linguagem natural e extrai a estrutura semântica.
        
        Args:
            query: Consulta em linguagem natural
            context: Contexto adicional para a consulta (opcional)
            
        Returns:
            Estrutura semântica extraída da consulta
        """
        # Implementação simplificada para testes
        query_lower = query.lower()
        logger.info(f"Processando consulta: {query}")
        
        # Extrai informações das fontes de dados do contexto
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
        elif target_source is None:
            target_source = "dados"
        
        # Padrões básicos para detectar intenções em consultas
        
        # Consulta de total/soma
        if re.search(r'(total|soma|quanto|quantos|sum|total de vendas)', query_lower):
            if re.search(r'por (cliente|clientes|produto|produtos|categoria)', query_lower):
                # Agrupamento detectado
                group_match = re.search(r'por (\w+)', query_lower)
                group_col = group_match.group(1) if group_match else "cliente"
                
                return {
                    "intent": "agregacao",
                    "sql_template": "SELECT {group_col}, SUM({value_col}) as total FROM {table} GROUP BY {group_col} ORDER BY total DESC",
                    "parameters": {"table": target_source, "group_col": group_col, "value_col": "valor"},
                    "expected_response_type": "table",
                    "original_query": query,
                    "data_source": target_source
                }
            else:
                # Soma total simples
                return {
                    "intent": "agregacao",
                    "sql_template": "SELECT SUM({value_col}) as total FROM {table}",
                    "parameters": {"table": target_source, "value_col": "valor"},
                    "expected_response_type": "table",
                    "original_query": query,
                    "data_source": target_source
                }
        
        # Consulta de filtragem
        elif re.search(r'(categoria|tipo|cliente) (\w+)', query_lower):
            filter_match = re.search(r'(categoria|tipo|cliente) (\w+)', query_lower)
            if filter_match:
                filter_col = filter_match.group(1)
                filter_val = filter_match.group(2)
                
                return {
                    "intent": "filtragem",
                    "sql_template": "SELECT * FROM {table} WHERE {filter_col} = '{filter_val}'",
                    "parameters": {"table": target_source, "filter_col": filter_col, "filter_val": filter_val},
                    "expected_response_type": "table",
                    "original_query": query,
                    "data_source": target_source
                }
                
        # Consulta simples - retorna tudo
        else:
            return {
                "intent": "consulta_dados",
                "sql_template": "SELECT * FROM {table} LIMIT 100",
                "parameters": {"table": target_source},
                "expected_response_type": "table",
                "original_query": query,
                "data_source": target_source
            }
    
    def _extract_semantic_structure(self, llm_response: Dict[str, Any], original_query: str) -> Dict[str, Any]:
        """
        Extrai a estrutura semântica da resposta do LLM.
        
        Args:
            llm_response: Resposta do modelo de linguagem
            original_query: Consulta original para fallback
            
        Returns:
            Estrutura semântica extraída
        """
        # Implementação simplificada - em uma versão real, isto processaria a resposta do LLM
        return self.process(original_query)