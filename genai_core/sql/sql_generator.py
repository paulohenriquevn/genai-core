# -*- coding: utf-8 -*-
"""
Módulo responsável pela geração de consultas SQL a partir de estruturas semânticas.
Suporta diferentes dialetos SQL e fontes de dados.
"""

import logging
import re
from typing import Dict, Any, Optional, List, Union

from genai_core.config.settings import Settings

# Configuração de logging
logger = logging.getLogger(__name__)


class SQLGenerator:
    """
    Responsável por gerar consultas SQL a partir de estruturas semânticas.
    Suporta adaptação para diferentes dialetos SQL e otimizações específicas.
    """
    
    def __init__(self, settings: Settings):
        """
        Inicializa o gerador SQL com as configurações fornecidas.
        
        Args:
            settings: Configurações do sistema
        """
        self.settings = settings
        self.default_dialect = settings.get("sql_dialect", "duckdb")
        
        logger.info(f"SQLGenerator inicializado com dialeto padrão: {self.default_dialect}")
    
    def generate(self, semantic_structure: Dict[str, Any]) -> str:
        """
        Gera uma consulta SQL a partir de uma estrutura semântica.
        
        Args:
            semantic_structure: Estrutura semântica extraída da consulta
            
        Returns:
            Consulta SQL gerada
        """
        logger.info("Gerando SQL a partir da estrutura semântica")
        
        try:
            # Extrai o template SQL e os parâmetros
            sql_template = semantic_structure.get("sql_template", "SELECT * FROM {table} LIMIT 10")
            parameters = semantic_structure.get("parameters", {})
            
            # Determina o dialeto SQL a ser usado
            data_source_id = semantic_structure.get("data_source", "default")
            dialect = self.default_dialect
            
            # Realiza adaptações específicas para o dialeto SQL
            sql_template = self._adapt_to_dialect(sql_template, dialect)
            
            # Substitui os parâmetros no template
            sql_query = self._substitute_parameters(sql_template, parameters)
            
            # Realiza validação básica do SQL
            self._validate_sql(sql_query)
            
            logger.info(f"SQL gerado: {sql_query}")
            return sql_query
            
        except Exception as e:
            logger.error(f"Erro ao gerar SQL: {str(e)}")
            # Fornece um SQL básico em caso de erro
            table = semantic_structure.get("parameters", {}).get("table", "dados")
            return f"SELECT * FROM {table} LIMIT 10"
    
    def _adapt_to_dialect(self, sql_template: str, dialect: str) -> str:
        """
        Adapta o template SQL para um dialeto específico.
        
        Args:
            sql_template: Template SQL original
            dialect: Dialeto SQL alvo (duckdb, postgresql, sqlite, etc.)
            
        Returns:
            Template SQL adaptado
        """
        # Implementação simplificada - em uma versão completa, fazeria adaptações específicas
        return sql_template
    
    def _substitute_parameters(self, sql_template: str, parameters: Dict[str, Any]) -> str:
        """
        Substitui os parâmetros no template SQL.
        
        Args:
            sql_template: Template SQL com placeholders
            parameters: Parâmetros a serem substituídos
            
        Returns:
            SQL com os parâmetros substituídos
        """
        # Cópia do template para não modificar o original
        sql_query = sql_template
        
        # Substitui os parâmetros usando o formato {nome_parametro}
        for param_name, param_value in parameters.items():
            placeholder = f"{{{param_name}}}"
            
            # Trata os diferentes tipos de parâmetros
            if isinstance(param_value, str):
                # Para simplificar a depuração, não adiciona aspas
                formatted_value = param_value
            elif isinstance(param_value, (list, tuple)):
                # Formata listas como val1, val2, ...
                formatted_items = []
                for item in param_value:
                    if isinstance(item, str):
                        formatted_items.append(item)
                    else:
                        formatted_items.append(str(item))
                formatted_value = f"{', '.join(formatted_items)}"
            else:
                # Outros tipos (números, etc.)
                formatted_value = str(param_value)
            
            # Substitui o placeholder pelo valor formatado
            sql_query = sql_query.replace(placeholder, formatted_value)
        
        return sql_query
    
    def _validate_sql(self, sql_query: str) -> None:
        """
        Realiza validações básicas na consulta SQL.
        
        Args:
            sql_query: Consulta SQL a ser validada
            
        Raises:
            ValueError: Se a consulta não passar nas validações
        """
        # Implementação simplificada - em uma versão completa, faria validações mais completas
        if not sql_query.lower().strip().startswith("select"):
            raise ValueError("A consulta SQL deve começar com SELECT")