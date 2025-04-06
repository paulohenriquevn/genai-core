import logging
import os
from typing import Dict, Any, Optional, Union, List, Type
import pandas as pd

from genai_core.nlp.nlp_processor import NLPProcessor
from genai_core.sql.sql_generator import SQLGenerator
from genai_core.data.connectors.data_connector import DataConnector

logger = logging.getLogger(__name__)

class QueryEngine:
    """
    Motor de consultas que coordena o pipeline completo: NLP → SQL → Execução.
    
    Esta classe é o ponto de entrada principal para o sistema, orquestrando
    o fluxo de processamento de consultas em linguagem natural.
    
    Attributes:
        nlp: Processador de linguagem natural
        sql_gen: Gerador de consultas SQL
        connector: Conector de dados para executar consultas
        cache: Cache opcional para consultas frequentes
    """
    
    def __init__(self, 
                nlp: NLPProcessor, 
                sql_gen: SQLGenerator, 
                connector: DataConnector,
                enable_cache: bool = False,
                debug_mode: bool = False):
        """
        Inicializa o motor de consultas.
        
        Args:
            nlp: Instância do processador de linguagem natural
            sql_gen: Instância do gerador de SQL
            connector: Instância do conector de dados
            enable_cache: Se deve ativar cache de consultas
            debug_mode: Se deve ativar modo de depuração
        """
        self.nlp = nlp
        self.sql_gen = sql_gen
        self.connector = connector
        self.cache = {} if enable_cache else None
        self.debug_mode = debug_mode
        
        # Verificar se o conector está inicializado corretamente
        if not connector.is_connected():
            logger.info("Conector não está conectado. Conectando...")
            connector.connect()
            
        logger.info("QueryEngine inicializado com sucesso")
        
    def get_schema(self) -> Dict[str, Any]:
        """
        Obtém o schema do conector de dados.
        
        Returns:
            Dicionário com informações do schema
        """
        try:
            schema_df = self.connector.get_schema()
            
            # Converter para dicionário com formato adequado para o NLP e SQL Generator
            schema = {
                "tables": [{
                    "name": self.connector.table_name if hasattr(self.connector, 'table_name') else "data",
                    "columns": []
                }]
            }
            
            # Adicionar colunas do schema
            if isinstance(schema_df, pd.DataFrame) and not schema_df.empty:
                for _, row in schema_df.iterrows():
                    if 'column_name' in schema_df.columns and 'column_type' in schema_df.columns:
                        schema["tables"][0]["columns"].append({
                            "name": row['column_name'],
                            "type": row['column_type']
                        })
                    
            return schema
            
        except Exception as e:
            logger.warning(f"Erro ao obter schema: {str(e)}")
            return {"tables": [{"name": "data", "columns": []}]}
    
    def run_query(self, question: str) -> pd.DataFrame:
        """
        Executa o pipeline completo de processamento de consulta.
        
        Este método coordena todo o fluxo:
        1. Analisa a pergunta em linguagem natural
        2. Gera a consulta SQL correspondente
        3. Executa a consulta no conector de dados
        
        Args:
            question: Pergunta em linguagem natural
            
        Returns:
            DataFrame com os resultados da consulta
            
        Raises:
            ValueError: Se a pergunta estiver vazia
            RuntimeError: Se houver erro no processamento
        """
        if not question or not question.strip():
            raise ValueError("A pergunta não pode estar vazia")
            
        # Verificar cache se estiver ativado
        if self.cache is not None and question in self.cache:
            logger.info(f"Usando resultado em cache para: '{question}'")
            return self.cache[question]
            
        try:
            logger.info(f"Processando consulta: '{question}'")
            
            # 1. Obter schema para melhorar a interpretação da pergunta
            schema = self.get_schema()
            
            # 2. Analisar a pergunta com o processador NLP
            parsed_data = self.nlp.parse_question(question, schema=schema)
            
            if self.debug_mode:
                logger.debug(f"Resultado do NLP: {parsed_data}")
                
            # 3. Gerar SQL a partir da estrutura semântica
            sql = self.sql_gen.generate_sql(parsed_data, schema=schema)
            
            logger.info(f"SQL gerado: {sql}")
            
            # 4. Executar a consulta SQL no conector
            result_df = self.connector.read_data(sql)
            
            # Armazenar no cache se estiver ativado
            if self.cache is not None:
                self.cache[question] = result_df.copy()
                
            return result_df
            
        except Exception as e:
            logger.error(f"Erro no pipeline de consulta: {str(e)}")
            raise RuntimeError(f"Erro ao processar consulta: {str(e)}")
    
    def execute_sql(self, sql: str) -> pd.DataFrame:
        """
        Executa uma consulta SQL diretamente, sem passar pelo processamento NLP.
        
        Útil para casos onde já se tem a consulta SQL pronta ou para debugging.
        
        Args:
            sql: Consulta SQL a ser executada
            
        Returns:
            DataFrame com os resultados
        """
        logger.info(f"Executando SQL diretamente: {sql}")
        
        try:
            return self.connector.read_data(sql)
        except Exception as e:
            logger.error(f"Erro ao executar SQL: {str(e)}")
            raise RuntimeError(f"Erro ao executar SQL: {str(e)}")
    
    def explain_query(self, question: str) -> Dict[str, Any]:
        """
        Explica o processamento de uma consulta sem executá-la.
        
        Útil para debugging e para entender como o sistema está interpretando a pergunta.
        
        Args:
            question: Pergunta em linguagem natural
            
        Returns:
            Dicionário com explicação do processamento
        """
        try:
            schema = self.get_schema()
            parsed_data = self.nlp.parse_question(question, schema=schema)
            sql = self.sql_gen.generate_sql(parsed_data, schema=schema)
            
            return {
                "question": question,
                "nlp_result": parsed_data,
                "sql": sql,
                "schema_used": schema
            }
        except Exception as e:
            logger.error(f"Erro ao explicar consulta: {str(e)}")
            return {
                "question": question,
                "error": str(e)
            }
    
    def close(self):
        """
        Fecha o conector e libera recursos.
        """
        try:
            if self.connector and hasattr(self.connector, 'close'):
                self.connector.close()
                logger.info("Conector fechado com sucesso")
        except Exception as e:
            logger.warning(f"Erro ao fechar conector: {str(e)}")


class GenAICore:
    """
    Classe principal do sistema GenAI Core.
    Gerencia fontes de dados e configurações do sistema.
    """
    
    def __init__(self, settings=None):
        """
        Inicializa o sistema GenAI Core.
        
        Args:
            settings: Configurações do sistema
        """
        self.settings = settings or {}
        self.connectors = {}
        
        # Inicializa componentes principais
        self.nlp_processor = NLPProcessor(model=self.settings.get("llm_type", "mock"))
        self.sql_generator = SQLGenerator(dialect=self.settings.get("sql_dialect", "duckdb"))
        self.query_engines = {}
        
        logger.info("GenAICore inicializado com sucesso")
        
    def load_data_source(self, config):
        """
        Carrega uma fonte de dados.
        
        Args:
            config: Configuração da fonte de dados
            
        Returns:
            ID da fonte carregada
        """
        source_id = config.get("id")
        if not source_id:
            raise ValueError("Configuração sem ID")
        
        # Importa o factory aqui para evitar import circular
        from genai_core.data.connectors.data_connector_factory import DataConnectorFactory
        
        # Cria o conector apropriado
        connector = DataConnectorFactory.create_connector(config)
        self.connectors[source_id] = connector
        
        # Cria um QueryEngine para esta fonte
        engine = QueryEngine(
            nlp=self.nlp_processor,
            sql_gen=self.sql_generator,
            connector=connector,
            debug_mode=self.settings.get("debug_mode", False)
        )
        self.query_engines[source_id] = engine
            
        logger.info(f"Fonte de dados {source_id} carregada com sucesso")
        return source_id
        
    def process_query(self, query: str, source_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Processa uma consulta em linguagem natural.
        
        Args:
            query: Consulta em linguagem natural
            source_id: ID da fonte de dados a consultar (opcional)
            
        Returns:
            Resultado da consulta
        """
        logger.info(f"Processando consulta: {query}")
        
        try:
            # Se source_id não for especificado, tenta inferir da consulta
            if not source_id:
                # Analisar a pergunta usando o NLP para detectar fonte
                semantics = self.nlp_processor.parse_question(query, schema={})
                source_id = semantics.get("data_source", next(iter(self.connectors.keys()), None))
            
            # Verifica se temos o conector/engine para essa fonte
            if not source_id or source_id not in self.query_engines:
                return {
                    "success": False, 
                    "error": f"Fonte de dados '{source_id}' não encontrada ou não especificada",
                    "type": "error"
                }
                
            # Usa o QueryEngine da fonte selecionada
            engine = self.query_engines[source_id]
            result_df = engine.run_query(query)
            
            # Converte para o formato de resposta
            return {
                "success": True,
                "type": "table",
                "data": {
                    "data": result_df.to_dict(orient="records")
                }
            }
                
        except Exception as e:
            logger.error(f"Erro ao processar consulta: {str(e)}")
            return {
                "success": False,
                "type": "error",
                "error": str(e)
            }
    
    def close(self):
        """
        Fecha todos os recursos e conexões.
        """
        for engine_id, engine in self.query_engines.items():
            try:
                engine.close()
            except Exception as e:
                logger.warning(f"Erro ao fechar engine {engine_id}: {str(e)}")
        
        self.query_engines = {}
        self.connectors = {}