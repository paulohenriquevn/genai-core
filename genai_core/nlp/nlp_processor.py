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
    
    Esta classe é responsável por:
    1. Normalizar e pré-processar o texto da consulta
    2. Identificar a intenção da consulta (agregação, filtragem, classificação, etc.)
    3. Extrair entidades e parâmetros relevantes
    4. Gerar uma estrutura semântica que possa ser convertida em SQL
    
    A implementação suporta diferentes modelos via injeção de dependência.
    """
    
    def __init__(self, model_name: str = None, settings: Optional[Settings] = None):
        """
        Inicializa o processador NLP com o modelo e configurações especificados.
        
        Args:
            model_name: Nome do modelo a ser utilizado (ex: "openai", "mock", etc.)
                        Se None, usa o valor definido nas configurações.
            settings: Configurações do sistema (opcional)
        """
        self.settings = settings or Settings()
        self.model_name = model_name or self.settings.get("llm_type", "mock")
        
        # Registra padrões de reconhecimento de intenções e entidades
        self._initialize_patterns()
        
        # Inicializa o cliente do modelo de linguagem
        self._initialize_model()
        
        logger.info(f"NLPProcessor inicializado com modelo: {self.model_name}")
    
    def _initialize_patterns(self):
        """
        Inicializa os padrões de reconhecimento para consultas comuns.
        Usado na abordagem baseada em regras quando um LLM não está disponível.
        """
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
    
    def _initialize_model(self):
        """
        Inicializa o cliente do modelo de linguagem com base no tipo configurado.
        Suporta diferentes tipos de modelos via injeção de dependência.
        """
        self.model_client = None
        model_type = self.model_name.lower()
        
        if model_type == "openai":
            try:
                from genai_core.llm.openai_client import OpenAIClient
                api_key = self.settings.get("openai_api_key", None)
                model = self.settings.get("openai_model", "gpt-3.5-turbo")
                self.model_client = OpenAIClient(api_key=api_key, model=model)
                logger.info(f"Inicializado cliente OpenAI com modelo: {model}")
            except ImportError:
                logger.warning("Módulo OpenAI não encontrado. Usando fallback baseado em regras.")
        
        elif model_type == "azure":
            try:
                from genai_core.llm.azure_client import AzureOpenAIClient
                api_key = self.settings.get("azure_api_key", None)
                endpoint = self.settings.get("azure_endpoint", None)
                deployment = self.settings.get("azure_deployment", None)
                self.model_client = AzureOpenAIClient(api_key=api_key, endpoint=endpoint, deployment=deployment)
                logger.info(f"Inicializado cliente Azure OpenAI com deployment: {deployment}")
            except ImportError:
                logger.warning("Módulo Azure OpenAI não encontrado. Usando fallback baseado em regras.")
        
        elif model_type == "mock":
            # No client needed for rule-based approach
            logger.info("Usando modo mock (baseado em regras)")
        
        else:
            logger.warning(f"Tipo de modelo desconhecido: {model_type}. Usando fallback baseado em regras.")
    
    def parse_question(self, question: str, schema: dict) -> dict:
        """
        Analisa uma pergunta em linguagem natural e retorna sua estrutura semântica.
        Este é o método principal da classe, que deve ser usado pelas aplicações.
        
        Args:
            question: Pergunta em linguagem natural a ser analisada
            schema: Schema das tabelas disponíveis, incluindo metadados das fontes de dados
                   Formato: {
                       "data_sources": {
                           "source_id": {
                               "tables": {"table_name": {"columns": {...}}},
                               "metadata": {...}
                           }
                       }
                   }
            
        Returns:
            Estrutura semântica da pergunta, contendo intenção, template SQL e parâmetros
        """
        # Cria um contexto para o processamento com os esquemas das fontes
        context = {"data_sources": {}}
        
        # Converte o schema para o formato esperado pelo processador
        if schema and "data_sources" in schema:
            context["data_sources"] = schema["data_sources"]
        
        # Tenta usar o modelo de linguagem se disponível
        if self.model_client:
            try:
                semantic_structure = self._parse_with_model(question, context)
                return semantic_structure
            except Exception as e:
                logger.warning(f"Erro ao usar modelo de linguagem: {str(e)}. Usando fallback baseado em regras.")
        
        # Fallback para abordagem baseada em regras
        return self._parse_with_rules(question, context)
    
    def _parse_with_model(self, question: str, context: dict) -> dict:
        """
        Analisa a pergunta usando um modelo de linguagem.
        
        Args:
            question: Pergunta em linguagem natural
            context: Contexto da análise, incluindo schema das fontes de dados
            
        Returns:
            Estrutura semântica da pergunta
        """
        # Preparar prompt para o modelo com o schema
        prompt = self._prepare_model_prompt(question, context)
        
        # Enviar para o modelo e obter resposta
        model_response = self.model_client.generate_completion(prompt)
        
        # Extrair estrutura semântica da resposta do modelo
        semantic_structure = self._extract_semantic_structure(model_response, question, context)
        
        return semantic_structure
    
    def _prepare_model_prompt(self, question: str, context: dict) -> str:
        """
        Prepara o prompt para enviar ao modelo de linguagem.
        
        Args:
            question: Pergunta em linguagem natural
            context: Contexto da análise, incluindo schema das fontes de dados
            
        Returns:
            Prompt formatado para o modelo
        """
        # Extrair informações de schema para o prompt
        schema_info = []
        for source_id, source_info in context.get("data_sources", {}).items():
            if "tables" in source_info:
                for table_name, table_info in source_info["tables"].items():
                    columns = table_info.get("columns", {})
                    schema_info.append(f"Tabela: {table_name} (fonte: {source_id})")
                    schema_info.append("Colunas: " + ", ".join(columns.keys()))
                    schema_info.append("")
        
        schema_text = "\n".join(schema_info)
        
        # Criar o prompt completo
        prompt = f"""Analise a seguinte pergunta em linguagem natural e extraia uma estrutura semântica que possa ser convertida em SQL.

Schema das tabelas disponíveis:
{schema_text}

Pergunta: {question}

Retorne uma estrutura semântica no formato JSON com os seguintes campos:
- intent: intenção da consulta (consulta_dados, agregacao, filtragem, classificacao, etc.)
- sql_template: template SQL com placeholders para parâmetros
- parameters: valores dos parâmetros a serem substituídos no template
- expected_response_type: tipo de resposta esperado (table, number, chart, etc.)
- data_source: identificador da fonte de dados alvo
"""
        
        return prompt
    
    def _extract_semantic_structure(self, model_response: str, question: str, context: dict) -> dict:
        """
        Extrai a estrutura semântica da resposta do modelo.
        
        Args:
            model_response: Resposta do modelo de linguagem
            question: Pergunta original para fallback
            context: Contexto da análise
            
        Returns:
            Estrutura semântica extraída
        """
        # Tenta extrair JSON da resposta do modelo
        try:
            # Busca por blocos JSON na resposta
            json_match = re.search(r'```json\s*(.*?)\s*```', model_response, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                # Tenta encontrar qualquer coisa que pareça um JSON
                json_match = re.search(r'(\{.*\})', model_response, re.DOTALL)
                if json_match:
                    json_str = json_match.group(1)
                else:
                    json_str = model_response
            
            # Parse do JSON
            semantic_structure = json.loads(json_str)
            
            # Adiciona a consulta original
            semantic_structure["original_query"] = question
            
            return semantic_structure
        
        except Exception as e:
            logger.error(f"Erro ao extrair estrutura semântica: {str(e)}")
            # Fallback para processamento baseado em regras
            return self._parse_with_rules(question, context)
    
    def _parse_with_rules(self, question: str, context: dict) -> dict:
        """
        Analisa a pergunta usando abordagem baseada em regras (não usa ML).
        
        Args:
            question: Pergunta em linguagem natural
            context: Contexto da análise, incluindo schema das fontes de dados
            
        Returns:
            Estrutura semântica da pergunta
        """
        query_lower = question.lower()
        logger.info(f"Processando consulta com regras: {question}")
        
        # Extrair informações das fontes de dados do contexto
        data_sources = {}
        if context and "data_sources" in context:
            data_sources = context["data_sources"]
        
        # Determinar a fonte de dados alvo
        target_source = self._determine_target_source(query_lower, data_sources)
        
        # Tenta encontrar um padrão para a consulta
        for pattern in self.patterns:
            if re.search(pattern["regex"], query_lower):
                result = pattern.copy()
                
                # Ajusta a tabela alvo baseada no contexto
                if "table" in result["parameters"]:
                    result["parameters"]["table"] = target_source
                
                # Extrai entidades específicas da pergunta
                result = self._extract_entities(result, query_lower)
                
                # Adiciona metadados à resposta
                result["original_query"] = question
                result["data_source"] = target_source
                
                logger.info(f"Padrão encontrado: {pattern['intent']}")
                return result
        
        # Fallback para consulta genérica se nenhum padrão corresponder
        logger.info("Usando fallback genérico")
        return {
            "intent": "consulta_dados",
            "sql_template": "SELECT * FROM {table} LIMIT 10",
            "parameters": {"table": target_source},
            "expected_response_type": "table",
            "original_query": question,
            "data_source": target_source
        }
    
    def _determine_target_source(self, query_lower: str, data_sources: dict) -> str:
        """
        Determina a fonte de dados alvo com base na consulta e nas fontes disponíveis.
        
        Args:
            query_lower: Consulta em lowercase
            data_sources: Fontes de dados disponíveis
            
        Returns:
            Identificador da fonte de dados alvo
        """
        # Tenta encontrar menção explícita a uma fonte
        for source_id in data_sources.keys():
            if source_id.lower() in query_lower:
                logger.debug(f"Fonte de dados encontrada na consulta: {source_id}")
                return source_id
        
        # Se não encontrou explicitamente, usa a primeira fonte disponível
        if data_sources:
            source_id = list(data_sources.keys())[0]
            logger.debug(f"Usando primeira fonte disponível: {source_id}")
            return source_id
        
        # Se não há fontes, usa um valor padrão
        logger.debug("Nenhuma fonte disponível, usando padrão: dados")
        return "dados"
    
    def _extract_entities(self, result: dict, query_lower: str) -> dict:
        """
        Extrai entidades específicas da consulta com base no padrão detectado.
        
        Args:
            result: Estrutura parcial do resultado
            query_lower: Consulta em lowercase
            
        Returns:
            Estrutura atualizada com entidades extraídas
        """
        # Detecta categorias ou tipos específicos
        category_match = re.search(r"(categoria|tipo|cliente) ['\"]?([a-zA-ZÀ-ÿ\s]+)['\"]?", query_lower)
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
        
        return result
    
    def process(self, query: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Método legado para compatibilidade com código existente.
        
        Args:
            query: Consulta em linguagem natural
            context: Contexto adicional para a consulta (opcional)
            
        Returns:
            Estrutura semântica extraída da consulta
        """
        logger.info(f"Processando consulta: {query}")
        
        # Adapta o contexto para o formato esperado pelo parse_question
        schema = {"data_sources": {}}
        if context and "data_sources" in context:
            schema["data_sources"] = context["data_sources"]
        
        # Delega para o novo método
        return self.parse_question(query, schema)