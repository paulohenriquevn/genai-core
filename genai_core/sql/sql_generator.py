# -*- coding: utf-8 -*-
"""
Módulo responsável pela geração de consultas SQL a partir de estruturas semânticas.
Suporta diferentes dialetos SQL e fontes de dados.
"""

import logging
import re
from typing import Dict, Any, Optional, List, Union, Callable
import string

# Configuração de logging
logger = logging.getLogger(__name__)


class SQLGenerator:
    """
    Responsável por gerar consultas SQL a partir de estruturas semânticas.
    
    Esta classe recebe a estrutura semântica de uma pergunta em linguagem natural
    e gera uma consulta SQL correspondente, considerando o schema das tabelas,
    os dialetos SQL suportados e as otimizações específicas.
    
    A geração de SQL é baseada em templates e funções de composição,
    evitando a concatenação direta de strings sempre que possível.
    """
    
    def __init__(self, settings: Optional[Any] = None):
        """
        Inicializa o gerador SQL.
        
        Args:
            settings: Configurações do sistema (opcional)
        """
        self.settings = settings
        self.default_dialect = "standard"
        
        if settings:
            self.default_dialect = settings.get("sql_dialect", "standard")
        
        # Registra os geradores de SQL específicos para cada tipo de consulta
        self._register_sql_generators()
        
        # Registra os adaptadores de dialeto
        self._register_dialect_adapters()
        
        logger.info(f"SQLGenerator inicializado com dialeto padrão: {self.default_dialect}")
    
    def _register_sql_generators(self) -> None:
        """
        Registra os geradores de SQL específicos para cada tipo de consulta.
        Cada gerador é uma função que recebe uma estrutura semântica e retorna um template SQL.
        """
        self.sql_generators = {
            # Consulta simples de dados
            "consulta_dados": self._generate_select_query,
            
            # Agregação (soma, contagem, etc.)
            "agregacao": self._generate_aggregation_query,
            
            # Filtragem com condições
            "filtragem": self._generate_filter_query,
            
            # Classificação (ordenação)
            "classificacao": self._generate_order_query,
            
            # Junção de tabelas
            "juncao": self._generate_join_query,
            
            # Fallback para consultas desconhecidas
            "default": lambda semantics, schema: "SELECT * FROM {table} LIMIT 10"
        }
    
    def _register_dialect_adapters(self) -> None:
        """
        Registra os adaptadores de dialeto para cada tipo de SQL suportado.
        Cada adaptador é uma função que recebe um template SQL e retorna uma versão adaptada.
        """
        self.dialect_adapters = {
            # SQLite
            "sqlite": self._adapt_for_sqlite,
            
            # PostgreSQL
            "postgresql": self._adapt_for_postgresql,
            
            # DuckDB
            "duckdb": self._adapt_for_duckdb,
            
            # MySQL
            "mysql": self._adapt_for_mysql,
            
            # SQL Server
            "sqlserver": self._adapt_for_sqlserver,
            
            # SQL padrão (ANSI)
            "standard": lambda sql: sql
        }
    
    def generate_sql(self, semantics: Dict[str, Any], schema: Optional[Dict[str, Any]] = None) -> str:
        """
        Gera uma consulta SQL a partir de uma estrutura semântica e schema.
        
        Este é o método principal da classe, que deve ser utilizado pelas aplicações
        para gerar SQL a partir de estruturas semânticas.
        
        Args:
            semantics: Estrutura semântica da pergunta, incluindo intenção e parâmetros
            schema: Schema das tabelas disponíveis (opcional)
            
        Returns:
            Consulta SQL gerada
        """
        logger.info("Gerando SQL a partir da estrutura semântica")
        
        try:
            # Determina o dialeto SQL a ser usado
            data_source_id = semantics.get("data_source", "default")
            dialect = self.default_dialect
            
            # Determina o tipo de consulta
            intent = semantics.get("intent", "default")
            
            # Obtém o gerador apropriado para o tipo de consulta
            generator = self.sql_generators.get(intent, self.sql_generators["default"])
            
            # Gera o template SQL usando o gerador específico
            sql_template = generator(semantics, schema)
            
            # Adapta o template para o dialeto específico
            sql_template = self._adapt_to_dialect(sql_template, dialect)
            
            # Preenche o template com os parâmetros
            sql_query = self._format_template(sql_template, semantics.get("parameters", {}))
            
            # Valida a consulta gerada
            self._validate_sql(sql_query)
            
            logger.info(f"SQL gerado: {sql_query}")
            return sql_query
            
        except Exception as e:
            logger.error(f"Erro ao gerar SQL: {str(e)}")
            # Fornece um SQL básico em caso de erro
            table = semantics.get("parameters", {}).get("table", "dados")
            return f"SELECT * FROM {table} LIMIT 10"
    
    def generate(self, semantic_structure: Dict[str, Any]) -> str:
        """
        Método legado para compatibilidade com código existente.
        
        Args:
            semantic_structure: Estrutura semântica extraída da consulta
            
        Returns:
            Consulta SQL gerada
        """
        try:
            # Extrai o template diretamente da estrutura semântica
            sql_template = semantic_structure.get("sql_template", "SELECT * FROM {table} LIMIT 10")
            parameters = semantic_structure.get("parameters", {})
            
            # Determina o dialeto SQL a ser usado
            data_source_id = semantic_structure.get("data_source", "default")
            dialect = self.default_dialect
            
            # Adapta o template para o dialeto específico
            sql_template = self._adapt_to_dialect(sql_template, dialect)
            
            # Para compatibilidade com a implementação anterior, usamos substituição manual
            # ao invés do format string (a implementação original não formatava strings com aspas)
            sql_query = sql_template
            
            # Substitui os parâmetros usando o formato {nome_parametro}
            for param_name, param_value in parameters.items():
                placeholder = f"{{{param_name}}}"
                
                # Trata os diferentes tipos de parâmetros
                if isinstance(param_value, str):
                    # A implementação original não adiciona aspas extras para strings que já eram parâmetros
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
            
            # Valida a consulta gerada
            self._validate_sql(sql_query)
            
            logger.info(f"SQL gerado (método legado): {sql_query}")
            return sql_query
            
        except Exception as e:
            logger.error(f"Erro ao gerar SQL (método legado): {str(e)}")
            # Fornece um SQL básico em caso de erro
            table = semantic_structure.get("parameters", {}).get("table", "dados")
            return f"SELECT * FROM {table} LIMIT 10"
    
    def _format_template(self, template: str, parameters: Dict[str, Any]) -> str:
        """
        Preenche um template SQL com os parâmetros fornecidos.
        Usa string.Formatter para manipulação segura de templates.
        
        Args:
            template: Template SQL com placeholders no formato {param}
            parameters: Dicionário de parâmetros a serem substituídos
            
        Returns:
            SQL formatado com os parâmetros substituídos
        """
        # Formata e escapa os parâmetros
        formatted_params = {}
        for key, value in parameters.items():
            formatted_params[key] = self._format_parameter(value)
        
        # Usa o string formatter para substituir os parâmetros de forma segura
        try:
            # Primeiro tenta com a formatação padrão
            return template.format(**formatted_params)
        except KeyError as e:
            # Se algum parâmetro estiver faltando, loga e fornece um valor padrão
            logger.warning(f"Parâmetro não encontrado: {str(e)}. Usando fallback.")
            formatted_params[str(e).strip("'")] = "NULL"
            return template.format(**formatted_params)
    
    def _format_parameter(self, value: Any) -> str:
        """
        Formata um parâmetro para uso seguro em SQL.
        
        Args:
            value: Valor do parâmetro a ser formatado
            
        Returns:
            Versão formatada e escapada do parâmetro
        """
        if value is None:
            return "NULL"
        
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        
        if isinstance(value, (int, float)):
            return str(value)
        
        if isinstance(value, str):
            # Aspas simples para strings e escape de aspas simples dentro da string
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
        
        if isinstance(value, (list, tuple)):
            # Formata elementos da lista e os une com vírgulas
            formatted_items = [self._format_parameter(item) for item in value]
            return ", ".join(formatted_items)
        
        # Fallback para outros tipos
        return str(value)
    
    def _adapt_to_dialect(self, sql: str, dialect: str) -> str:
        """
        Adapta uma consulta SQL para um dialeto específico.
        
        Args:
            sql: Consulta SQL original
            dialect: Dialeto alvo (mysql, postgresql, sqlite, duckdb, etc.)
            
        Returns:
            Consulta adaptada para o dialeto especificado
        """
        dialect = dialect.lower()
        adapter = self.dialect_adapters.get(dialect, self.dialect_adapters["standard"])
        return adapter(sql)
    
    def _adapt_for_sqlite(self, sql: str) -> str:
        """Adapta SQL para SQLite."""
        # SQLite não suporta alguns recursos avançados, faz as adaptações necessárias
        return sql
    
    def _adapt_for_postgresql(self, sql: str) -> str:
        """Adapta SQL para PostgreSQL."""
        # PostgreSQL tem sintaxe específica para algumas operações
        return sql
    
    def _adapt_for_duckdb(self, sql: str) -> str:
        """Adapta SQL para DuckDB."""
        # DuckDB é bastante compatível com SQL padrão, mas pode precisar de ajustes
        return sql
    
    def _adapt_for_mysql(self, sql: str) -> str:
        """Adapta SQL para MySQL."""
        # MySQL tem peculiaridades na sintaxe
        return sql
    
    def _adapt_for_sqlserver(self, sql: str) -> str:
        """Adapta SQL para SQL Server."""
        # SQL Server tem algumas diferenças de sintaxe
        return sql
    
    def _validate_sql(self, sql: str) -> None:
        """
        Realiza validações básicas na consulta SQL.
        
        Args:
            sql: Consulta SQL a ser validada
            
        Raises:
            ValueError: Se a consulta não passar nas validações
        """
        # Validações básicas
        sql_lower = sql.lower().strip()
        
        # Verifica se a consulta é SELECT (este gerador não suporta outros tipos de SQL)
        if not sql_lower.startswith("select"):
            raise ValueError("A consulta SQL deve começar com SELECT")
        
        # Verifica se a consulta tem uma cláusula FROM
        if " from " not in sql_lower:
            raise ValueError("A consulta SQL deve incluir uma cláusula FROM")
    
    def _generate_select_query(self, semantics: Dict[str, Any], schema: Optional[Dict[str, Any]]) -> str:
        """
        Gera uma consulta SELECT simples.
        
        Args:
            semantics: Estrutura semântica da consulta
            schema: Schema das tabelas (opcional)
            
        Returns:
            Template SQL para consulta SELECT
        """
        # Recupera os parâmetros relevantes
        table = semantics.get("parameters", {}).get("table", "dados")
        
        # Verifica se há um limite definido na estrutura semântica
        if "limit" in semantics.get("parameters", {}):
            limit = semantics.get("parameters", {}).get("limit")
        else:
            # Se não houver, define um padrão e adiciona aos parâmetros
            limit = 100
            semantics["parameters"]["limit"] = limit
        
        # Verifica se há colunas específicas a serem selecionadas
        columns = semantics.get("parameters", {}).get("columns", ["*"])
        column_str = ", ".join(columns) if isinstance(columns, list) else columns
        
        # Constrói a consulta com o limite incorporado diretamente (não como placeholder)
        return f"SELECT {column_str} FROM {{table}} LIMIT {limit}"
    
    def _generate_aggregation_query(self, semantics: Dict[str, Any], schema: Optional[Dict[str, Any]]) -> str:
        """
        Gera uma consulta de agregação (SUM, COUNT, AVG, etc.).
        
        Args:
            semantics: Estrutura semântica da consulta
            schema: Schema das tabelas (opcional)
            
        Returns:
            Template SQL para consulta de agregação
        """
        # Recupera os parâmetros relevantes
        table = semantics.get("parameters", {}).get("table", "dados")
        
        # Função de agregação, padrão é SUM
        if "agg_function" in semantics.get("parameters", {}):
            agg_function = semantics.get("parameters", {}).get("agg_function")
        else:
            # Para compatibilidade, busca o valor de "function" se "agg_function" não existir
            agg_function = semantics.get("parameters", {}).get("function", "SUM")
        
        # Coluna para agregação
        if "value_col" in semantics.get("parameters", {}):
            agg_column = semantics.get("parameters", {}).get("value_col")
        else:
            # Para compatibilidade, busca o valor em várias opções
            possible_keys = ["agg_col", "value_column", "column", "valor"]
            for key in possible_keys:
                if key in semantics.get("parameters", {}):
                    agg_column = semantics.get("parameters", {}).get(key)
                    break
            else:
                agg_column = "valor"  # Valor padrão se nenhum for encontrado
        
        # Alias para a coluna de resultado
        if "alias" in semantics.get("parameters", {}):
            alias = semantics.get("parameters", {}).get("alias")
        else:
            # Determina um alias apropriado baseado na função
            if agg_function.lower() == "sum":
                alias = "total"
            elif agg_function.lower() == "count":
                alias = "contagem"
            elif agg_function.lower() == "avg":
                alias = "media"
            else:
                alias = f"{agg_function.lower()}_{agg_column}"
        
        # Verifica se há agrupamento
        group_col = semantics.get("parameters", {}).get("group_col")
        
        # Inclui alias no semantics para estar disponível para o template
        semantics["parameters"]["alias"] = alias
        
        if group_col:
            # Consulta com agrupamento
            return f"SELECT {{group_col}}, {agg_function}({{value_col}}) as {alias} FROM {{table}} GROUP BY {{group_col}} ORDER BY {alias} DESC"
        else:
            # Agregação simples sem agrupamento
            return f"SELECT {agg_function}({{value_col}}) as {alias} FROM {{table}}"
    
    def _generate_filter_query(self, semantics: Dict[str, Any], schema: Optional[Dict[str, Any]]) -> str:
        """
        Gera uma consulta com filtros (WHERE).
        
        Args:
            semantics: Estrutura semântica da consulta
            schema: Schema das tabelas (opcional)
            
        Returns:
            Template SQL para consulta com filtros
        """
        # Recupera os parâmetros relevantes
        table = semantics.get("parameters", {}).get("table", "dados")
        filter_col = semantics.get("parameters", {}).get("filter_col")
        filter_value = semantics.get("parameters", {}).get("filter_value")
        operator = semantics.get("parameters", {}).get("operator", "=")
        
        # Constrói a cláusula WHERE
        if filter_col and filter_value is not None:
            return f"SELECT * FROM {{table}} WHERE {{filter_col}} {operator} {{filter_value}}"
        else:
            # Sem filtro
            return "SELECT * FROM {table} LIMIT 100"
    
    def _generate_order_query(self, semantics: Dict[str, Any], schema: Optional[Dict[str, Any]]) -> str:
        """
        Gera uma consulta com ordenação (ORDER BY).
        
        Args:
            semantics: Estrutura semântica da consulta
            schema: Schema das tabelas (opcional)
            
        Returns:
            Template SQL para consulta com ordenação
        """
        # Recupera os parâmetros relevantes
        table = semantics.get("parameters", {}).get("table", "dados")
        order_col = semantics.get("parameters", {}).get("order_col", "id")
        direction = semantics.get("parameters", {}).get("direction", "DESC")
        limit = semantics.get("parameters", {}).get("limit", 10)
        
        # Constrói a consulta com ordenação
        return f"SELECT * FROM {{table}} ORDER BY {{order_col}} {direction} LIMIT {{limit}}"
    
    def _generate_join_query(self, semantics: Dict[str, Any], schema: Optional[Dict[str, Any]]) -> str:
        """
        Gera uma consulta com junção de tabelas (JOIN).
        
        Args:
            semantics: Estrutura semântica da consulta
            schema: Schema das tabelas (opcional)
            
        Returns:
            Template SQL para consulta com junção
        """
        # Recupera os parâmetros relevantes
        table1 = semantics.get("parameters", {}).get("table1", "tabela1")
        table2 = semantics.get("parameters", {}).get("table2", "tabela2")
        join_type = semantics.get("parameters", {}).get("join_type", "INNER")
        join_condition = semantics.get("parameters", {}).get("join_condition", "{table1}.id = {table2}.{table1}_id")
        
        # Verifica as colunas a serem selecionadas
        select_cols = semantics.get("parameters", {}).get("columns", "*")
        if select_cols == "*" and schema:
            # Se temos schema e colunas não foram especificadas, seleciona todas com prefixo de tabela
            try:
                # Tenta construir uma lista de colunas qualificadas se o schema estiver disponível
                if schema and "data_sources" in schema:
                    data_source_id = semantics.get("data_source", "default")
                    if data_source_id in schema["data_sources"]:
                        data_source = schema["data_sources"][data_source_id]
                        if "tables" in data_source:
                            tables = data_source["tables"]
                            
                            cols1 = []
                            if table1 in tables and "columns" in tables[table1]:
                                cols1 = [f"{table1}.{col}" for col in tables[table1]["columns"].keys()]
                            
                            cols2 = []
                            if table2 in tables and "columns" in tables[table2]:
                                cols2 = [f"{table2}.{col}" for col in tables[table2]["columns"].keys()]
                            
                            if cols1 and cols2:
                                select_cols = ", ".join(cols1 + cols2)
            except Exception as e:
                logger.warning(f"Erro ao construir colunas para JOIN: {str(e)}")
                select_cols = "*"
        
        # Constrói a consulta com JOIN
        return f"SELECT {select_cols} FROM {{table1}} {join_type} JOIN {{table2}} ON {join_condition}"