# -*- coding: utf-8 -*-
"""
Módulo principal que orquestra o fluxo completo de processamento:
- Interpretação de linguagem natural
- Geração de SQL
- Execução de consultas
- Processamento de resultados
"""

import logging
import sys
import os
from typing import Dict, Any, Optional, Union, List

from genai_core.nlp.nlp_processor import NLPProcessor
from genai_core.sql.sql_generator import SQLGenerator
from genai_core.config.settings import Settings

# Configuração de logging
logger = logging.getLogger(__name__)


class GenAICore:
    """
    Classe principal que orquestra todo o fluxo de processamento
    de consultas em linguagem natural para resultados estruturados.
    """
    
    def __init__(self, settings: Optional[Settings] = None):
        """
        Inicializa o núcleo do sistema com as configurações fornecidas.
        
        Args:
            settings: Configurações do sistema (opcional)
        """
        self.settings = settings or Settings()
        self.nlp_processor = NLPProcessor(self.settings)
        self.sql_generator = SQLGenerator(self.settings)
        self.connectors = {}
        
        logger.info("GenAICore inicializado com sucesso")
    
    def process_query(self, query: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Processa uma consulta em linguagem natural e retorna os resultados.
        
        Args:
            query: Consulta em linguagem natural
            context: Contexto adicional para a consulta (opcional)
            
        Returns:
            Dict com os resultados da consulta
        """
        logger.info(f"Processando consulta: {query}")
        
        try:
            # Processa a linguagem natural
            semantic_structure = self.nlp_processor.process(query, context)
            
            # Gera o SQL a partir da estrutura semântica
            sql_query = self.sql_generator.generate(semantic_structure)
            
            # Executa a consulta SQL
            result = self.execute_query(sql_query, semantic_structure.get("data_source"))
            
            # Formata a resposta
            response = {
                "success": True,
                "data": result,
                "type": semantic_structure.get("expected_response_type", "table"),
                "query": query,
                "sql": sql_query
            }
            
            return response
            
        except Exception as e:
            logger.error(f"Erro ao processar consulta: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "query": query
            }
    
    def execute_query(self, sql_query: str, data_source: str) -> Dict[str, Any]:
        """
        Executa uma consulta SQL na fonte de dados especificada.
        
        Args:
            sql_query: Consulta SQL a ser executada
            data_source: Identificador da fonte de dados
            
        Returns:
            Dict com os resultados da consulta
        """
        # Obter o conector apropriado para a fonte de dados
        connector = self._get_connector(data_source)
        
        # Executar a consulta
        result = connector.execute_query(sql_query)
        
        return result
    
    def _get_connector(self, data_source: str) -> Any:
        """
        Obtém ou cria um conector para a fonte de dados especificada.
        
        Args:
            data_source: Identificador da fonte de dados
            
        Returns:
            Instância do conector para a fonte de dados
        """
        # Se o conector já estiver em cache, retorna ele
        if data_source in self.connectors:
            return self.connectors[data_source]
        
        # Caso contrário, cria um novo com base nas configurações
        data_source_config = self.settings.get_data_source_config(data_source)
        
        # Seleciona o tipo de conector com base no tipo de fonte de dados
        connector_type = data_source_config.get("type", "").lower()
        
        if connector_type == "csv":
            from genai_core.data.connectors.csv_connector import CSVConnector
            connector = CSVConnector(data_source_config)
        elif connector_type in ["excel", "xls", "xlsx"]:
            from genai_core.data.connectors.excel_connector import ExcelConnector
            connector = ExcelConnector(data_source_config)
        elif connector_type == "postgres":
            from genai_core.data.connectors.postgres_connector import PostgresConnector
            connector = PostgresConnector(data_source_config)
        elif connector_type in ["duckdb", "duckdb_csv", "duckdb_xls"]:
            from genai_core.data.connectors.duckdb_connector import DuckDBConnector
            connector = DuckDBConnector(data_source_config)
        else:
            raise ValueError(f"Tipo de fonte de dados não suportado: {connector_type}")
        
        # Estabelece a conexão
        connector.connect()
        
        # Armazena o conector em cache para uso futuro
        self.connectors[data_source] = connector
        
        return connector
    
    def load_data_source(self, data_source_config: Dict[str, Any]) -> str:
        """
        Carrega uma nova fonte de dados no sistema.
        
        Args:
            data_source_config: Configuração da fonte de dados
            
        Returns:
            ID da fonte de dados carregada
        """
        # Adiciona a fonte de dados às configurações
        data_source_id = self.settings.add_data_source(data_source_config)
        
        # Inicializa o conector (se ele for usado posteriormente, já estará em cache)
        connector = self._get_connector(data_source_id)
        
        return data_source_id
    
    def close(self):
        """Fecha todas as conexões e libera recursos."""
        for connector in self.connectors.values():
            if hasattr(connector, "close"):
                connector.close()
        
        self.connectors.clear()
        logger.info("Todos os conectores foram fechados")