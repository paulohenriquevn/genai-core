import logging
import pandas as pd
from typing import Dict, Any, Optional

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
        self.nlp = nlp
        self.sql_gen = sql_gen
        self.connector = connector
        self.cache = {} if enable_cache else None
        self.debug_mode = debug_mode
        
        if not connector.is_connected():
            logger.info("Conector não está conectado. Conectando...")
            connector.connect()
            
        logger.info("QueryEngine inicializado com sucesso")
        
    def get_schema(self) -> Dict[str, Any]:
        try:
            schema_df = self.connector.get_schema()
            schema = {
                "tables": [{
                    "name": self.connector.table_name if hasattr(self.connector, 'table_name') else "data",
                    "columns": []
                }]
            }
            
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
        if not question or not question.strip():
            raise ValueError("A pergunta não pode estar vazia")
            
        if self.cache is not None and question in self.cache:
            logger.info(f"Usando resultado em cache para: '{question}'")
            return self.cache[question]
            
        try:
            logger.info(f"Processando consulta: '{question}'")
            
            schema = self.get_schema()
            
            parsed_data = self.nlp.parse_question(question, schema=schema)
            
            if self.debug_mode:
                logger.debug(f"Resultado do NLP: {parsed_data}")
                
            sql = self.sql_gen.generate_sql(parsed_data, schema=schema)
            
            logger.info(f"SQL gerado: {sql}")
            
            result_df = self.connector.read_data(sql)
            
            if self.cache is not None:
                self.cache[question] = result_df.copy()
                
            return result_df
            
        except Exception as e:
            logger.error(f"Erro no pipeline de consulta: {str(e)}")
            raise RuntimeError(f"Erro ao processar consulta: {str(e)}")
    
    def execute_sql(self, sql: str) -> pd.DataFrame:
        
        logger.info(f"Executando SQL diretamente: {sql}")
        
        try:
            return self.connector.read_data(sql)
        except Exception as e:
            logger.error(f"Erro ao executar SQL: {str(e)}")
            raise RuntimeError(f"Erro ao executar SQL: {str(e)}")
    
    def explain_query(self, question: str) -> Dict[str, Any]:
        
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
        self.settings = settings or {}
        self.connectors = {}
        self.nlp_processor = NLPProcessor(model=self.settings.get("llm_type", "mock"))
        self.sql_generator = SQLGenerator(dialect=self.settings.get("sql_dialect", "duckdb"))
        self.query_engines = {}
        
        logger.info("GenAICore inicializado com sucesso")
        
    def load_data_source(self, config):
        source_id = config.get("id")
        if not source_id:
            raise ValueError("Configuração sem ID")
        
        from genai_core.data.connectors.data_connector_factory import DataConnectorFactory
        
        connector = DataConnectorFactory.create_connector(config)
        self.connectors[source_id] = connector
        
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
        logger.info(f"Processando consulta: {query}")
        
        try:
            if not source_id:
                semantics = self.nlp_processor.parse_question(query, schema={})
                source_id = semantics.get("data_source", next(iter(self.connectors.keys()), None))
            
            if not source_id or source_id not in self.query_engines:
                return {
                    "success": False, 
                    "error": f"Fonte de dados '{source_id}' não encontrada ou não especificada",
                    "type": "error"
                }
                
            # Usa o QueryEngine da fonte selecionada
            engine = self.query_engines[source_id]
            result_df = engine.run_query(query)
            
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
        for engine_id, engine in self.query_engines.items():
            try:
                engine.close()
            except Exception as e:
                logger.warning(f"Erro ao fechar engine {engine_id}: {str(e)}")
        
        self.query_engines = {}
        self.connectors = {}