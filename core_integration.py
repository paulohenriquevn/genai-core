import os
import logging
import pandas as pd
import time
import json
from typing import Dict, List, Optional, Any, Union

# Importação dos componentes core
from core.code_executor import AdvancedDynamicCodeExecutor
from core.agent.state import AgentState, AgentMemory, AgentConfig
from core.prompts.generate_python_code_with_sql import GeneratePythonCodeWithSQLPrompt
from core.response.parser import ResponseParser
from core.response.base import BaseResponse
from core.response.dataframe import DataFrameResponse
from core.response.number import NumberResponse
from core.response.string import StringResponse
from core.response.chart import ChartResponse
from core.response.error import ErrorResponse
from core.user_query import UserQuery
from core.exceptions import QueryExecutionError

# Importação do módulo de integração com LLMs
from llm_integration import LLMIntegration, LLMQueryGenerator

# Importação do analisador de datasets
from utils.dataset_analyzer import DatasetAnalyzer

# Configura o logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("core_integration.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("core_integration")


class Dataset:
    """
    Representa um dataset com metadados e descrição para uso no motor de análise.
    Inclui análise automática de estrutura e relacionamentos.
    """
    
    def __init__(
        self, 
        dataframe: pd.DataFrame, 
        name: str, 
        description: str = "", 
        schema: Dict[str, str] = None,
        auto_analyze: bool = True
    ):
        """
        Inicializa um objeto Dataset.
        
        Args:
            dataframe: DataFrame Pandas com os dados
            name: Nome do dataset
            description: Descrição do conjunto de dados
            schema: Dicionário de metadados sobre as colunas (opcional)
            auto_analyze: Se True, faz análise automática da estrutura do dataset
        """
        self.dataframe = dataframe
        self.name = name
        self.description = description
        self.schema = schema or {}
        self.analyzed_metadata = {}
        self.primary_key = None
        self.column_types = {}
        self.potential_foreign_keys = []
        
        # Analisar automaticamente se solicitado
        if auto_analyze:
            self._analyze_structure()
    
    def _analyze_structure(self):
        """
        Analisa a estrutura do dataset para detectar metadados importantes.
        """
        # Usa o DatasetAnalyzer para obter metadados detalhados
        analyzer = DatasetAnalyzer()
        analyzer.add_dataset(self.name, self.dataframe)
        analysis_result = analyzer.analyze_all()
        
        # Extrai os metadados do dataset analisado
        if self.name in analysis_result.get("metadata", {}):
            dataset_meta = analysis_result["metadata"][self.name]
            
            # Armazena os metadados completos
            self.analyzed_metadata = dataset_meta
            
            # Extrai informações principais
            self.primary_key = dataset_meta.get("primary_key")
            self.potential_foreign_keys = dataset_meta.get("potential_foreign_keys", [])
            
            # Extrai tipos de dados sugeridos para cada coluna
            self.column_types = {}
            for col_name, col_meta in dataset_meta.get("columns", {}).items():
                self.column_types[col_name] = col_meta.get("suggested_type", "unknown")
                
                # Atualiza o schema com descrições mais ricas
                if col_meta.get("suggested_type") and "description" not in self.schema.get(col_name, {}):
                    # Usa a descrição gerada pelo analisador
                    self.schema[col_name] = col_meta.get("description", f"Column {col_name}")
    
    def to_json(self) -> Dict[str, Any]:
        """
        Converte o dataset para um formato JSON para uso em prompts.
        Inclui metadados avançados da análise automática.
        
        Returns:
            Dict com informações sobre o dataset
        """
        # Cria uma representação enriquecida para o LLM
        columns = []
        for col in self.dataframe.columns:
            # Tipo inferido da análise automática ou tipo pandas
            col_type = self.column_types.get(col, str(self.dataframe[col].dtype))
            
            # Amostra de dados
            sample = str(self.dataframe[col].iloc[0]) if len(self.dataframe) > 0 else ""
            
            # Descrição rica baseada na análise ou no schema fornecido
            description = self.schema.get(col, f"Column {col} of type {col_type}")
            
            # Adiciona informações sobre chaves e relacionamentos
            metadata = {}
            if col == self.primary_key:
                metadata["is_primary_key"] = True
                
            if col in self.potential_foreign_keys:
                metadata["is_foreign_key"] = True
                
            # Adiciona metadados detalhados da análise
            if self.analyzed_metadata and "columns" in self.analyzed_metadata:
                if col in self.analyzed_metadata["columns"]:
                    col_meta = self.analyzed_metadata["columns"][col]
                    
                    # Adiciona estatísticas relevantes
                    if "stats" in col_meta:
                        metadata["stats"] = col_meta["stats"]
                    
                    # Adiciona informações específicas do tipo
                    if col_type == "numeric" and "numeric_stats" in col_meta:
                        metadata["numeric_stats"] = col_meta["numeric_stats"]
                    elif col_type == "date" and "temporal_stats" in col_meta:
                        metadata["temporal_stats"] = col_meta["temporal_stats"]
                    elif col_type == "categorical" and "top_values" in col_meta:
                        metadata["top_values"] = col_meta["top_values"]
            
            # Monta o objeto de coluna completo
            column_info = {
                "name": col,
                "type": col_type,
                "sample": sample,
                "description": description
            }
            
            # Adiciona metadados se existirem
            if metadata:
                column_info["metadata"] = metadata
                
            columns.append(column_info)
        
        # Relações detectadas
        relationships = []
        if self.analyzed_metadata and "relationships" in self.analyzed_metadata:
            rel_info = self.analyzed_metadata["relationships"]
            if "outgoing" in rel_info:
                for rel in rel_info["outgoing"]:
                    relationships.append({
                        "from": f"{self.name}.{rel['source_column']}",
                        "to": f"{rel['target_dataset']}.{rel['target_column']}",
                        "type": rel.get("type", "many_to_one")
                    })
            if "incoming" in rel_info:
                for rel in rel_info["incoming"]:
                    relationships.append({
                        "from": f"{rel['source_dataset']}.{rel['source_column']}",
                        "to": f"{self.name}.{rel['target_column']}",
                        "type": rel.get("type", "many_to_one")
                    })
        
        # Estrutura completa com informações enriquecidas
        result = {
            "name": self.name,
            "description": self.description,
            "row_count": len(self.dataframe),
            "column_count": len(self.dataframe.columns),
            "columns": columns,
            "sample": self.dataframe.head(3).to_dict(orient="records")
        }
        
        # Adiciona informações de chave primária se detectada
        if self.primary_key:
            result["primary_key"] = self.primary_key
            
        # Adiciona relações se existirem
        if relationships:
            result["relationships"] = relationships
            
        return result
        
    def serialize_dataframe(self) -> Dict[str, Any]:
        """
        Serializa o dataframe para uso no prompt template.
        Método requerido pela integração com o template de prompt.
        
        Returns:
            Dict com informações do dataframe
        """
        return {
            "name": self.name,
            "description": self.description,
            "dataframe": self.dataframe
        }


class AnalysisEngine:
    """
    Motor de análise que integra componentes core para processamento de consultas em linguagem natural.
    
    Esta classe implementa:
    - Carregamento e gerenciamento de datasets
    - Execução segura de código
    - Geração de prompts para LLM
    - Processamento de consultas em linguagem natural
    - Tratamento de respostas e conversão de formatos
    """
    
    def __init__(
        self,
        agent_description: str = "Assistente de Análise de Dados Inteligente",
        default_output_type: str = "dataframe",
        direct_sql: bool = False,
        timeout: int = 30,
        max_output_size: int = 1024 * 1024,  # 1 MB
        model_type: str = "mock",
        model_name: Optional[str] = None,
        api_key: Optional[str] = None
    ):
        """
        Inicializa o motor de análise com configurações personalizadas.
        
        Args:
            agent_description: Descrição do agente para o LLM
            default_output_type: Tipo padrão de saída (dataframe, string, number, plot)
            direct_sql: Se True, executa SQL diretamente sem código Python
            timeout: Tempo limite para execução de código (segundos)
            max_output_size: Tamanho máximo da saída
            model_type: Tipo de modelo LLM (openai, anthropic, huggingface, local, mock)
            model_name: Nome específico do modelo LLM
            api_key: Chave de API para o modelo LLM
        """
        logger.info(f"Inicializando AnalysisEngine com output_type={default_output_type}, model_type={model_type}")
        
        # Inicialização dos componentes core
        self.code_executor = AdvancedDynamicCodeExecutor(
            timeout=timeout,
            max_output_size=max_output_size,
            allowed_imports=[
                "numpy", 
                "pandas", 
                "matplotlib", 
                "scipy", 
                "sympy", 
                "statistics", 
                "re", 
                "math", 
                "random", 
                "datetime", 
                "json", 
                "itertools", 
                "collections", 
                "io", 
                "base64"
            ]
        )
        
        # Configuração do agente
        agent_config = AgentConfig(direct_sql=direct_sql)
        agent_memory = AgentMemory(agent_description=agent_description)
        
        # Estado do agente (armazena datasets, memória e configurações)
        self.agent_state = AgentState(
            dfs=[],  # Será populado com objetos Dataset
            memory=agent_memory,
            config=agent_config,
            output_type=default_output_type
        )
        
        # Parser de respostas para validação e conversão
        self.response_parser = ResponseParser()
        
        # Armazena o último código gerado
        self.last_code_generated = ""
        
        # Dataset carregados (nome -> Dataset)
        self.datasets = {}
        
        # Inicializa o gerador de código LLM
        try:
            # Cria a integração LLM
            llm_integration = LLMIntegration(
                model_type=model_type,
                model_name=model_name,
                api_key=api_key
            )
            
            # Cria o gerador de consultas
            self.query_generator = LLMQueryGenerator(llm_integration=llm_integration)
            logger.info(f"Gerador LLM inicializado com modelo {model_type}" + (f" ({model_name})" if model_name else ""))
        except Exception as e:
            # Em caso de erro, usa o modo mock
            logger.warning(f"Erro ao inicializar LLM: {str(e)}. Usando modo mock.")
            self.query_generator = LLMQueryGenerator()
    
    def load_data(
        self, 
        data: Union[pd.DataFrame, str], 
        name: str, 
        description: str = None,
        schema: Dict[str, str] = None
    ) -> None:
        """
        Carrega um DataFrame ou arquivo CSV no motor de análise.
        
        Args:
            data: DataFrame ou caminho para arquivo CSV
            name: Nome do dataset
            description: Descrição do dataset (opcional)
            schema: Dicionário de metadados das colunas (opcional)
        """
        try:
            # Carrega dados se for um caminho de arquivo
            if isinstance(data, str):
                logger.info(f"Carregando dados do arquivo: {data}")
                
                # Determina o tipo de arquivo pela extensão
                if data.endswith('.csv'):
                    df = pd.read_csv(data)
                elif data.endswith(('.xls', '.xlsx')):
                    df = pd.read_excel(data)
                elif data.endswith('.json'):
                    df = pd.read_json(data)
                elif data.endswith('.parquet'):
                    df = pd.read_parquet(data)
                else:
                    raise ValueError(f"Formato de arquivo não suportado: {data}")
            else:
                # Usa DataFrame diretamente
                df = data
            
            # Define descrição padrão se não fornecida
            if description is None:
                if isinstance(data, str):
                    description = f"Dataset carregado de {os.path.basename(data)}"
                else:
                    description = f"Dataset {name}"
            
            # Preprocessa o DataFrame para garantir compatibilidade com SQL
            df = self._preprocess_dataframe_for_sql(df, name)
            
            # Cria objeto Dataset
            dataset = Dataset(dataframe=df, name=name, description=description, schema=schema)
            
            # Armazena para uso futuro e adiciona ao estado do agente
            self.datasets[name] = dataset
            
            # Atualiza a lista no estado do agente com objetos Dataset
            self.agent_state.dfs.append(dataset)
            
            logger.info(f"Dataset '{name}' carregado com {len(df)} linhas e {len(df.columns)} colunas")
        
        except Exception as e:
            logger.error(f"Erro ao carregar dados: {str(e)}")
            raise
    
    def _preprocess_dataframe_for_sql(self, df: pd.DataFrame, name: str) -> pd.DataFrame:
        """
        Prepara um DataFrame para uso em consultas SQL, garantindo compatibilidade com DuckDB.
        
        Args:
            df: DataFrame a ser preprocessado
            name: Nome do dataset (para logging)
            
        Returns:
            DataFrame preprocessado
        """
        try:
            # Cria cópia para evitar alterações no original
            processed_df = df.copy()
            
            # Converte colunas de data para o formato correto
            for col in processed_df.columns:
                # Verifica se a coluna parece ser uma data
                if processed_df[col].dtype == 'object':
                    try:
                        # Tenta usar expressão regular para identificar padrões de data
                        if processed_df[col].str.contains(r'\d{4}-\d{2}-\d{2}').any():
                            logger.info(f"Convertendo coluna {col} para datetime no dataset {name}")
                            processed_df[col] = pd.to_datetime(processed_df[col], errors='ignore')
                    except (AttributeError, TypeError):
                        # Ignora erros para colunas que não são strings ou com valores mistos
                        pass
            
            # Remove caracteres especiais dos nomes das colunas
            rename_map = {}
            for col in processed_df.columns:
                # Substitui espaços e caracteres especiais por underscores
                new_col = col
                if ' ' in col or any(c in col for c in '!@#$%^&*()-+?_=,<>/\\|{}[]'):
                    new_col = ''.join(c if c.isalnum() else '_' for c in col)
                    rename_map[col] = new_col
            
            # Renomeia colunas se necessário
            if rename_map:
                logger.info(f"Renomeando colunas com caracteres especiais no dataset {name}: {rename_map}")
                processed_df = processed_df.rename(columns=rename_map)
            
            # Verifica e corrige tipos de dados problemáticos
            for col in processed_df.columns:
                # Tenta converter colunas mistas para string quando apropriado
                if processed_df[col].dtype == 'object' and not pd.api.types.is_datetime64_any_dtype(processed_df[col]):
                    # Se a coluna tem valores mistos, converte para string
                    try:
                        unique_types = processed_df[col].apply(type).nunique()
                        if unique_types > 1:
                            logger.info(f"Convertendo coluna {col} com tipos mistos para string no dataset {name}")
                            processed_df[col] = processed_df[col].astype(str)
                    except:
                        # Em caso de erro, força para string
                        processed_df[col] = processed_df[col].astype(str)
            
            return processed_df
            
        except Exception as e:
            logger.warning(f"Erro durante preprocessamento do DataFrame {name}: {str(e)}")
            # Em caso de erro, retorna o DataFrame original
            return df
    
    def get_dataset(self, name: str) -> Optional[Dataset]:
        """
        Obtém um dataset pelo nome.
        
        Args:
            name: Nome do dataset
            
        Returns:
            Dataset ou None se não encontrado
        """
        return self.datasets.get(name)
    
    def list_datasets(self) -> List[str]:
        """
        Lista os nomes de todos os datasets carregados.
        
        Returns:
            Lista de nomes de datasets
        """
        return list(self.datasets.keys())
    
    def _generate_prompt(self, query: str) -> str:
        """
        Gera um prompt para o LLM com base na consulta do usuário.
        
        Args:
            query: Consulta em linguagem natural
            
        Returns:
            Prompt formatado para o LLM
        """
        # Adiciona a consulta à memória do agente
        self.agent_state.memory.add_message(query)
        
        # Cria o prompt usando a classe GeneratePythonCodeWithSQLPrompt
        prompt = GeneratePythonCodeWithSQLPrompt(
            context=self.agent_state,
            output_type=self.agent_state.output_type,
            last_code_generated=self.last_code_generated
        )
        
        # Renderiza o prompt completo
        rendered_prompt = prompt.render()
        logger.debug(f"Prompt gerado: {rendered_prompt[:500]}...")
        
        return rendered_prompt
    
    def process_query(self, query: str, retry_count: int = 0, max_retries: int = 2, feedback: str = None) -> BaseResponse:
        """
        Processa uma consulta em linguagem natural.
        
        Args:
            query: Consulta em linguagem natural
            retry_count: Contador de tentativas de rephrasing (uso interno)
            max_retries: Número máximo de tentativas antes de oferecer opções alternativas
            feedback: Feedback do usuário para melhorar a resposta (opcional)
            
        Returns:
            Objeto BaseResponse com o resultado da consulta
        """
        logger.info(f"Processando consulta: {query} (tentativa {retry_count+1})")
        
        # Se houver feedback do usuário, armazena para uso em futuras melhorias
        if feedback:
            self._store_user_feedback(query, feedback)
            logger.info(f"Feedback recebido para a consulta: '{feedback}'")
        
        try:
            # Cria objeto UserQuery
            user_query = UserQuery(query)
            
            # Verifica se há datasets carregados
            if not self.datasets:
                return ErrorResponse("Nenhum dataset carregado. Carregue dados antes de executar consultas.")
            
            # Verifica menções a dados inexistentes
            # Lista de palavras-chave que indicam consultas sobre entidades não existentes
            missing_entity_keywords = {
                'produtos': ['produtos', 'produto', 'estoque', 'inventário', 'item', 'itens', 'mercadoria'],
                'funcionários': ['funcionários', 'funcionário', 'funcionario', 'funcionarios', 'colaborador', 'colaboradores', 'empregado', 'empregados', 'staff', 'equipe'],
                'departamentos': ['departamento', 'departamentos', 'setor', 'setores', 'área', 'áreas', 'divisão', 'divisões'],
                'categorias': ['categoria', 'categorias', 'classe', 'classes', 'tipo de produto', 'tipos de produto']
            }
            
            # Verifica se a consulta menciona entidades não existentes
            for entity_type, keywords in missing_entity_keywords.items():
                if any(keyword in query.lower() for keyword in keywords) and not any(entity_type in ds.name.lower() for ds in self.datasets.values()):
                    # Gera sugestões de consultas alternativas baseadas nos dados disponíveis
                    alternative_queries = self._generate_alternative_queries()
                    datasets_desc = ", ".join([f"{name}" for name, _ in self.datasets.items()])
                    
                    return self._create_missing_entity_response(
                        entity_type, 
                        datasets_desc, 
                        alternative_queries
                    )
            
            # Gera o prompt para o LLM
            prompt = self._generate_prompt(query)
            
            # Gera código Python usando o LLM
            start_time = time.time()
            generated_code = self.query_generator.generate_code(prompt)
            generation_time = time.time() - start_time
            
            logger.info(f"Código gerado em {generation_time:.2f}s")
            self.last_code_generated = generated_code
            
            # Contexto para execução inclui os datasets
            execution_context = {
                'query': query,
                'datasets': {name: ds.dataframe for name, ds in self.datasets.items()},
                'retry_count': retry_count
            }
            
            # Configuração da função execute_sql_query
            if len(self.datasets) > 0:
                execution_context['execute_sql_query'] = self._create_sql_executor()
            
            # Executa o código gerado
            execution_result = self.code_executor.execute_code(
                generated_code,
                context=execution_context,
                output_type=self.agent_state.output_type
            )
            
            # Verifica se a execução foi bem-sucedida
            if not execution_result["success"]:
                error_msg = execution_result["error"]
                logger.error(f"Erro na execução de código: {error_msg}")
                
                # Verifica se o erro menciona tabelas inexistentes
                if "tabela" in error_msg.lower() and ("não encontrada" in error_msg.lower() or "não existe" in error_msg.lower()):
                    return self._handle_missing_table_error(error_msg)
                
                # Tenta corrigir o erro (opcional)
                if "correction_attempt" not in execution_context:
                    correction_result = self._attempt_error_correction(query, generated_code, error_msg, execution_context)
                    
                    # Se a correção também falhou e ainda não esgotamos as tentativas
                    if correction_result.type == "error" and retry_count < max_retries:
                        # Tenta reformular a consulta
                        rephrased_query = self._rephrase_query(query, error_msg)
                        logger.info(f"Consulta reformulada: {rephrased_query}")
                        
                        # Reinicia o processamento com a consulta reformulada
                        return self.process_query(rephrased_query, retry_count + 1, max_retries)
                    
                    # Se tentamos correção e ainda não funcionou, mas foi o último retry
                    if correction_result.type == "error" and retry_count >= max_retries:
                        # Oferece opções predefinidas
                        return self._offer_predefined_options(query, error_msg)
                    
                    return correction_result
                
                # Se chegou aqui, é uma falha após todas as tentativas
                if retry_count >= max_retries:
                    return self._offer_predefined_options(query, error_msg)
                
                return ErrorResponse(f"Erro ao processar consulta: {error_msg}")
            
            # Obtém o resultado da execução
            result = execution_result["result"]
            
            # Valida e processa a resposta
            try:
                # Formata o resultado para o formato esperado pelo parser
                formatted_result = self._format_result_for_parser(result)
                
                # Parse a resposta para o tipo apropriado
                response = self.response_parser.parse(
                    formatted_result, 
                    self.last_code_generated
                )
                
                # Armazena a consulta bem-sucedida para uso futuro
                self._store_successful_query(query, self.last_code_generated)
                
                logger.info(f"Consulta processada com sucesso. Tipo de resposta: {response.type}")
                return response
                
            except Exception as e:
                logger.error(f"Erro ao processar resposta: {str(e)}")
                
                # Se ainda temos tentativas disponíveis
                if retry_count < max_retries:
                    # Tenta reformular a consulta
                    rephrased_query = self._rephrase_query(query, str(e))
                    logger.info(f"Consulta reformulada após erro de processamento: {rephrased_query}")
                    
                    # Reinicia o processamento com a consulta reformulada
                    return self.process_query(rephrased_query, retry_count + 1, max_retries)
                
                return ErrorResponse(f"Erro no processamento da resposta: {str(e)}")
        
        except Exception as e:
            logger.error(f"Erro ao processar consulta: {str(e)}")
            
            # Se ainda temos tentativas disponíveis
            if retry_count < max_retries:
                # Tenta reformular a consulta
                rephrased_query = self._rephrase_query(query, str(e))
                logger.info(f"Consulta reformulada após exceção: {rephrased_query}")
                
                # Reinicia o processamento com a consulta reformulada
                return self.process_query(rephrased_query, retry_count + 1, max_retries)
            
            return ErrorResponse(f"Erro ao processar consulta: {str(e)}")
            
    def _create_missing_entity_response(self, entity_type: str, datasets_desc: str, alternative_queries: List[str]) -> StringResponse:
        """
        Cria uma resposta amigável para consultas sobre entidades não existentes.
        
        Args:
            entity_type: Tipo de entidade não encontrada
            datasets_desc: Descrição dos datasets disponíveis
            alternative_queries: Consultas alternativas sugeridas
            
        Returns:
            StringResponse com mensagem e sugestões
        """
        message = f"Não há dados sobre {entity_type} disponíveis. Os datasets disponíveis são: {datasets_desc}."
        
        if alternative_queries:
            message += "\n\nVocê pode tentar estas consultas alternativas:\n"
            for i, query in enumerate(alternative_queries[:3], 1):
                message += f"{i}. {query}\n"
            
        message += "\nPor favor, reformule sua consulta para usar os dados disponíveis."
        
        return StringResponse(message)
        
    def _handle_missing_table_error(self, error_msg: str) -> StringResponse:
        """
        Gera uma resposta amigável para erros de tabela não encontrada.
        
        Args:
            error_msg: Mensagem de erro original
            
        Returns:
            StringResponse com informações úteis
        """
        # Extrai o nome da tabela da mensagem de erro, se possível
        import re
        table_match = re.search(r"tabela '(\w+)'", error_msg)
        missing_table = table_match.group(1) if table_match else "mencionada"
        
        # Lista de datasets disponíveis com suas colunas
        datasets_info = []
        for name, ds in self.datasets.items():
            cols = ", ".join(ds.dataframe.columns[:5]) + ("..." if len(ds.dataframe.columns) > 5 else "")
            datasets_info.append(f"• {name}: {cols}")
        
        datasets_desc = "\n".join(datasets_info)
        
        message = f"""Não foi possível encontrar a tabela '{missing_table}' nos dados disponíveis.

Os datasets disponíveis são:
{datasets_desc}

Por favor, reformule sua consulta para usar apenas os datasets listados acima."""
        
        return StringResponse(message)
    
    def _rephrase_query(self, original_query: str, error_info: str) -> str:
        """
        Usa o LLM para reformular a consulta original baseado no erro encontrado.
        
        Args:
            original_query: Consulta original que falhou
            error_info: Informação sobre o erro
            
        Returns:
            Consulta reformulada
        """
        # Lista de datasets disponíveis
        available_datasets = ', '.join(self.datasets.keys())
        
        # Cria um prompt para o LLM reformular a consulta
        rephrase_prompt = f"""Por favor, reformule a seguinte consulta para que ela funcione com os datasets disponíveis.

CONSULTA ORIGINAL: "{original_query}"

ERRO ENCONTRADO: {error_info}

DATASETS DISPONÍVEIS: {available_datasets}

COLUNAS DISPONÍVEIS:
"""
        
        # Adiciona informações sobre as colunas disponíveis
        for name, dataset in self.datasets.items():
            rephrase_prompt += f"\n{name}: {', '.join(dataset.dataframe.columns)}"
        
        rephrase_prompt += """

Sua tarefa é reformular a consulta original para que ela:
1. Use apenas os datasets e colunas listados acima
2. Mantenha a intenção original da consulta
3. Evite os mesmos erros
4. Seja clara e direta

Por favor, forneça APENAS a consulta reformulada, sem explicações adicionais."""

        try:
            # Tenta reformular a consulta usando o LLM
            rephrased_query = self.query_generator.generate_code(rephrase_prompt)
            
            # Limpa a resposta, pegando apenas a primeira linha não vazia
            import re
            cleaned_query = re.sub(r'^[\s\'"]*|[\s\'"]*$', '', rephrased_query.split('\n')[0])
            
            # Se a limpeza resultar em string vazia, use uma linha subsequente
            if not cleaned_query:
                for line in rephrased_query.split('\n'):
                    line = re.sub(r'^[\s\'"]*|[\s\'"]*$', '', line)
                    if line:
                        cleaned_query = line
                        break
            
            # Garante que a consulta reformulada não seja o código Python gerado
            # (às vezes o LLM pode ignorar as instruções)
            if "import" in cleaned_query or "def " in cleaned_query or "result =" in cleaned_query:
                # Fallback para uma simplificação da consulta original
                return self._simplify_query(original_query)
            
            return cleaned_query if cleaned_query else original_query
            
        except Exception as e:
            logger.error(f"Erro ao reformular consulta: {str(e)}")
            # Em caso de erro, tenta uma simplificação básica
            return self._simplify_query(original_query)
            
    def _simplify_query(self, query: str) -> str:
        """
        Simplifica a consulta original para torná-la mais genérica.
        
        Args:
            query: Consulta original
            
        Returns:
            Consulta simplificada
        """
        # Substitui termos específicos por termos mais genéricos
        simplifications = [
            (r'produto[s]?', 'dados'),
            (r'funcionario[s]?|colaborador[es]?|empregado[s]?', 'pessoas'),
            (r'departamento[s]?|setor[es]?', 'grupos'),
            (r'categorias?', 'tipos'),
            (r'estoque', 'quantidade'),
            (r'inventário', 'dados'),
        ]
        
        simplified = query
        for pattern, replacement in simplifications:
            simplified = re.sub(pattern, replacement, simplified, flags=re.IGNORECASE)
        
        # Se a consulta foi modificada, retorna a versão simplificada
        if simplified != query:
            return simplified
        
        # Se não conseguiu simplificar, retorna uma consulta ainda mais genérica
        keywords = ['mostre', 'liste', 'exiba', 'apresente', 'qual', 'quais', 'como', 'onde', 'quando']
        for keyword in keywords:
            if keyword in query.lower():
                # Extrai apenas a parte após a palavra-chave
                parts = re.split(rf'{keyword}\s+', query.lower(), flags=re.IGNORECASE, maxsplit=1)
                if len(parts) > 1:
                    return f"{keyword} os dados disponíveis sobre {parts[1]}"
        
        # Último recurso: consulta completamente genérica
        return "Mostre um resumo dos dados disponíveis"
    
    def _generate_alternative_queries(self) -> List[str]:
        """
        Gera consultas alternativas baseadas nos datasets disponíveis.
        
        Returns:
            Lista de consultas sugeridas
        """
        alternatives = []
        
        # Consultas básicas para cada dataset
        for name in self.datasets.keys():
            alternatives.append(f"Mostre um resumo do dataset {name}")
            alternatives.append(f"Quais são as principais informações em {name}?")
        
        # Consultas mais específicas baseadas nos metadados dos datasets
        for name, dataset in self.datasets.items():
            # Consultas baseadas em tipos de colunas
            if hasattr(dataset, 'column_types'):
                # Identifica colunas numéricas para agregações
                numeric_cols = [col for col, type in dataset.column_types.items() 
                                if type in ['numeric', 'number', 'int', 'float']]
                
                # Identifica colunas categóricas para agrupamentos
                cat_cols = [col for col, type in dataset.column_types.items() 
                            if type in ['categorical', 'string', 'object']]
                
                # Identifica colunas de data para análises temporais
                date_cols = [col for col, type in dataset.column_types.items() 
                            if type in ['date', 'datetime']]
                
                # Gera consultas para agregações
                if numeric_cols and cat_cols:
                    alternatives.append(f"Qual é o total de {numeric_cols[0]} por {cat_cols[0]} em {name}?")
                
                # Gera consultas para ordenações
                if numeric_cols:
                    alternatives.append(f"Quais são os maiores valores de {numeric_cols[0]} em {name}?")
                
                # Gera consultas para análises temporais
                if date_cols:
                    alternatives.append(f"Como os dados de {name} variam ao longo do tempo?")
                    alternatives.append(f"Mostre os dados de {name} agrupados por mês")
            
            # Consultas baseadas em relacionamentos
            if hasattr(dataset, 'analyzed_metadata') and dataset.analyzed_metadata:
                if 'relationships' in dataset.analyzed_metadata:
                    rel_info = dataset.analyzed_metadata.get('relationships', {})
                    
                    if 'outgoing' in rel_info and rel_info['outgoing']:
                        for rel in rel_info['outgoing'][:2]:  # Limita a 2 relacionamentos
                            target = rel.get('target_dataset')
                            alternatives.append(f"Mostre dados de {name} relacionados com {target}")
        
        # Remove duplicatas e limita a 10 alternativas
        unique_alternatives = list(set(alternatives))
        return unique_alternatives[:10]
    
    def _offer_predefined_options(self, query: str, error_msg: str) -> StringResponse:
        """
        Oferece opções predefinidas de consultas quando todas as tentativas falharam.
        
        Args:
            query: Consulta original
            error_msg: Mensagem de erro
            
        Returns:
            StringResponse com opções predefinidas
        """
        # Gera alternativas
        alternatives = self._generate_alternative_queries()
        
        # Prepara a mensagem de resposta
        message = f"""Não foi possível processar a consulta: "{query}"

Erro: {error_msg}

Aqui estão algumas consultas alternativas que você pode tentar:

"""
        # Adiciona as alternativas numeradas
        for i, alt in enumerate(alternatives[:5], 1):
            message += f"{i}. {alt}\n"
            
        message += """
Você também pode:
• Simplificar sua consulta
• Especificar exatamente quais datasets quer consultar
• Fornecer feedback para melhorarmos o sistema
"""
        
        return StringResponse(message)
    
    def _store_successful_query(self, query: str, code: str) -> None:
        """
        Armazena consultas bem-sucedidas para uso futuro em sugestões.
        
        Args:
            query: Consulta que foi bem-sucedida
            code: Código gerado para a consulta
        """
        # Cria o diretório de cache se não existir
        cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "query_cache")
        os.makedirs(cache_dir, exist_ok=True)
        
        # Armazena em um arquivo JSON
        cache_file = os.path.join(cache_dir, "successful_queries.json")
        
        try:
            # Carrega o cache existente
            existing_cache = {}
            if os.path.exists(cache_file):
                with open(cache_file, 'r', encoding='utf-8') as f:
                    existing_cache = json.load(f)
            
            # Adiciona a nova consulta
            cleaned_query = query.strip().lower()
            existing_cache[cleaned_query] = {
                "timestamp": time.time(),
                "original_query": query,
                "code": code
            }
            
            # Salva o cache atualizado
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(existing_cache, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            logger.error(f"Erro ao armazenar consulta bem-sucedida: {str(e)}")
    
    def _store_user_feedback(self, query: str, feedback: str) -> None:
        """
        Armazena feedback do usuário para melhorias futuras.
        
        Args:
            query: Consulta relacionada ao feedback
            feedback: Texto do feedback
        """
        # Cria o diretório de feedback se não existir
        feedback_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "user_feedback")
        os.makedirs(feedback_dir, exist_ok=True)
        
        # Armazena em um arquivo JSON
        feedback_file = os.path.join(feedback_dir, "user_feedback.json")
        
        try:
            # Carrega o feedback existente
            existing_feedback = []
            if os.path.exists(feedback_file):
                with open(feedback_file, 'r', encoding='utf-8') as f:
                    existing_feedback = json.load(f)
            
            # Adiciona o novo feedback
            existing_feedback.append({
                "timestamp": time.time(),
                "query": query,
                "feedback": feedback
            })
            
            # Salva o feedback atualizado
            with open(feedback_file, 'w', encoding='utf-8') as f:
                json.dump(existing_feedback, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            logger.error(f"Erro ao armazenar feedback do usuário: {str(e)}")
    
    def process_query_with_feedback(self, query: str, feedback: str = None) -> BaseResponse:
        """
        Processa uma consulta e inclui feedback do usuário quando disponível.
        
        Args:
            query: Consulta em linguagem natural
            feedback: Feedback opcional do usuário sobre consultas anteriores
            
        Returns:
            Objeto BaseResponse com o resultado da consulta
        """
        return self.process_query(query, feedback=feedback)
    
    def _create_sql_executor(self):
        """
        Cria uma função para executar consultas SQL em datasets.
        
        Returns:
            Função que executa SQL em DataFrames com suporte a funções SQL compatíveis
        """
        # Integração com DuckDB para execução SQL mais robusta
        try:
            import duckdb
            import re
            from datetime import datetime
            
            def adapt_sql_query(sql_query: str) -> str:
                """
                Adapta uma consulta SQL para compatibilidade com DuckDB.
                
                Args:
                    sql_query: Consulta SQL original
                    
                Returns:
                    Consulta SQL adaptada para DuckDB
                """
                # Verificação de tabelas existentes
                table_names = list(self.datasets.keys())
                
                # Verifica se a consulta referencia tabelas inexistentes
                for table in re.findall(r'FROM\s+(\w+)', sql_query, re.IGNORECASE):
                    if table not in table_names:
                        logger.warning(f"Tabela '{table}' não encontrada nos datasets carregados")
                
                # Substitui funções de data incompatíveis
                # DATE_FORMAT(campo, '%Y-%m-%d') -> strftime('%Y-%m-%d', campo)
                sql_query = re.sub(
                    r'DATE_FORMAT\s*\(\s*([^,]+)\s*,\s*[\'"]([^\'"]+)[\'"]\s*\)',
                    r"strftime('\2', \1)",
                    sql_query
                )
                
                # TO_DATE(string) -> DATE(string)
                sql_query = re.sub(
                    r'TO_DATE\s*\(\s*([^)]+)\s*\)',
                    r'DATE(\1)',
                    sql_query
                )
                
                # Funções de string
                # CONCAT(a, b) -> a || b
                sql_query = re.sub(
                    r'CONCAT\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)',
                    r'(\1 || \2)',
                    sql_query
                )
                
                # SUBSTRING(x, start, len) -> SUBSTR(x, start, len)
                sql_query = re.sub(
                    r'SUBSTRING\s*\(',
                    r'SUBSTR(',
                    sql_query
                )
                
                # Funções de agregação
                # GROUP_CONCAT -> STRING_AGG
                sql_query = re.sub(
                    r'GROUP_CONCAT\s*\(',
                    r'STRING_AGG(',
                    sql_query
                )
                
                logger.debug(f"Consulta SQL adaptada: {sql_query}")
                return sql_query
            
            def check_table_existence(sql_query: str) -> None:
                """Verifica se as tabelas referenciadas existem."""
                table_refs = re.findall(r'FROM\s+(\w+)', sql_query, re.IGNORECASE)
                table_refs.extend(re.findall(r'JOIN\s+(\w+)', sql_query, re.IGNORECASE))
                
                for table in table_refs:
                    if table not in self.datasets:
                        raise ValueError(f"Tabela '{table}' não encontrada nos datasets carregados. " + 
                                    f"Datasets disponíveis: {', '.join(self.datasets.keys())}")
            
            def register_custom_sql_functions(con: duckdb.DuckDBPyConnection) -> None:
                """
                Registra funções SQL personalizadas no DuckDB para ampliar a compatibilidade
                com outros dialetos SQL, usando abordagem simplificada.
                
                Args:
                    con: Conexão DuckDB
                """
                try:
                    # Função utilitária para criar SQL functions de forma segura
                    def safe_create_function(sql):
                        try:
                            con.execute(sql)
                        except Exception as e:
                            logger.warning(f"Erro ao criar função SQL: {str(e)}")
                    
                    # GROUP_CONCAT para compatibilidade com MySQL
                    safe_create_function("CREATE OR REPLACE MACRO GROUP_CONCAT(x) AS STRING_AGG(x, ',')")
                    
                    # DATE_FORMAT simplificada (casos mais comuns)
                    safe_create_function("""
                    CREATE OR REPLACE MACRO DATE_FORMAT(d, f) AS
                    CASE 
                        WHEN f = '%Y-%m-%d' THEN strftime('%Y-%m-%d', d)
                        WHEN f = '%Y-%m' THEN strftime('%Y-%m', d)
                        WHEN f = '%Y' THEN strftime('%Y', d)
                        ELSE strftime('%Y-%m-%d', d)
                    END
                    """)
                    
                    # TO_DATE para converter para data
                    safe_create_function("CREATE OR REPLACE MACRO TO_DATE(d) AS TRY_CAST(d AS DATE)")
                    
                    # String concatenation helpers
                    safe_create_function("CREATE OR REPLACE MACRO CONCAT(a, b) AS a || b")
                    
                    # Concat with separator (simplified version)
                    safe_create_function("""
                    CREATE OR REPLACE MACRO CONCAT_WS(sep, a, b) AS
                    CASE 
                        WHEN a IS NULL AND b IS NULL THEN NULL
                        WHEN a IS NULL THEN b
                        WHEN b IS NULL THEN a
                        ELSE a || sep || b
                    END
                    """)
                    
                    # Register extract functions for date parts
                    safe_create_function("""
                    CREATE OR REPLACE MACRO YEAR(d) AS EXTRACT(YEAR FROM d)
                    """)
                    
                    safe_create_function("""
                    CREATE OR REPLACE MACRO MONTH(d) AS EXTRACT(MONTH FROM d)
                    """)
                    
                    safe_create_function("""
                    CREATE OR REPLACE MACRO DAY(d) AS EXTRACT(DAY FROM d)
                    """)
                    
                    logger.info("Funções SQL personalizadas registradas com sucesso")
                    
                except Exception as e:
                    logger.warning(f"Erro ao registrar funções SQL personalizadas: {str(e)}")
            
            def execute_sql(sql_query: str) -> pd.DataFrame:
                """Executa uma consulta SQL usando DuckDB com adaptações de compatibilidade."""
                try:
                    # Verifica se tabelas existem antes de executar
                    check_table_existence(sql_query)
                    
                    # Adapta a consulta para compatibilidade com DuckDB
                    adapted_query = adapt_sql_query(sql_query)
                    
                    # Estabelece conexão com todos os dataframes
                    con = duckdb.connect(database=':memory:')
                    
                    # Registra funções SQL personalizadas
                    register_custom_sql_functions(con)
                    
                    # Registra todos os datasets
                    for name, dataset in self.datasets.items():
                        # Registra o dataframe
                        con.register(name, dataset.dataframe)
                        
                        # Cria visualizações otimizadas para funções de data
                        con.execute(f"""
                        CREATE OR REPLACE VIEW {name}_date_view AS 
                        SELECT * FROM {name}
                        """)
                    
                    # Executa a consulta
                    result = con.execute(adapted_query).fetchdf()
                    
                    # Registra a consulta SQL para debugging
                    sql_logger = logging.getLogger("sql_logger")
                    sql_logger.info(f"Consulta SQL executada: {adapted_query}")
                    
                    return result
                except Exception as e:
                    logger.error(f"Erro SQL: {str(e)}")
                    raise QueryExecutionError(f"Erro ao executar SQL: {str(e)}")
        
        except ImportError:
            # Fallback para pandas se DuckDB não estiver disponível
            logger.warning("DuckDB não encontrado, usando pandas para consultas SQL")
            
            def execute_sql(sql_query: str) -> pd.DataFrame:
                """Executa uma consulta SQL básica usando pandas."""
                try:
                    # Para o modo pandas, suporta apenas SELECT * FROM dataset
                    import re
                    match = re.search(r'FROM\s+(\w+)', sql_query, re.IGNORECASE)
                    
                    if not match:
                        raise ValueError("Consulta SQL inválida. Formato esperado: SELECT * FROM dataset")
                    
                    dataset_name = match.group(1)
                    
                    if dataset_name not in self.datasets:
                        raise ValueError(f"Dataset '{dataset_name}' não encontrado")
                    
                    # Registra a consulta SQL para debugging
                    sql_logger = logging.getLogger("sql_logger")
                    sql_logger.info(f"Consulta SQL simulada: {sql_query}")
                    
                    # Retorna o dataset inteiro (limitação do modo pandas)
                    return self.datasets[dataset_name].dataframe
                except Exception as e:
                    logger.error(f"Erro SQL: {str(e)}")
                    raise QueryExecutionError(f"Erro ao executar SQL: {str(e)}")
        
        return execute_sql
    
    def _attempt_error_correction(self, query: str, original_code: str, error_msg: str, context: Dict[str, Any]) -> BaseResponse:
        """
        Tenta corrigir um código com erro usando o LLM, com suporte especial para erros de SQL.
        
        Args:
            query: Consulta original
            original_code: Código com erro
            error_msg: Mensagem de erro
            context: Contexto de execução
            
        Returns:
            Resposta após tentativa de correção
        """
        logger.info(f"Tentando corrigir erro: {error_msg}")
        
        # Verifica se é um erro relacionado a SQL
        is_sql_error = any(keyword in error_msg.lower() for keyword in 
                          ['sql', 'query', 'syntax', 'column', 'table', 'from', 'select', 
                           'date', 'function', 'duckdb', 'type', 'conversion'])
        
        # Lista de datasets disponíveis
        datasets_list = ", ".join(self.datasets.keys())
        
        # Adiciona sugestões específicas para erros de SQL
        sql_correction_tips = ""
        if is_sql_error:
            sql_correction_tips = f"""
            DICAS PARA CORREÇÃO DE SQL:
            
            1. Datasets disponíveis: {datasets_list}
            2. Use strftime('%Y-%m-%d', coluna) em vez de DATE_FORMAT
            3. Use DATE(string) em vez de TO_DATE
            4. Use coluna1 || coluna2 em vez de CONCAT
            5. Verifique se todas as tabelas mencionadas no SQL existem
            6. DuckDB é sensível a tipos - converta dados quando necessário
            7. Certifique-se de que as colunas referenciadas existem nas tabelas
            8. Verifique a sintaxe SQL - DuckDB segue o padrão PostgreSQL
            """
        
        # Cria um prompt para correção
        correction_prompt = f"""
        O código gerado para a consulta "{query}" falhou com o seguinte erro:
        
        ERROR:
        {error_msg}
        
        CÓDIGO ORIGINAL:
        {original_code}
        
        {sql_correction_tips}
        
        Por favor, corrija o código levando em conta o erro. Forneça apenas o código Python corrigido,
        não explicações. Lembre-se que o resultado deve ser um dicionário no formato:
        result = {{"type": tipo, "value": valor}}
        onde o tipo pode ser "string", "number", "dataframe", ou "plot".
        
        Se a consulta mencionar dados que não existem (como 'produtos'), adapte para usar dados disponíveis
        ou retorne uma mensagem explicando que esses dados não estão disponíveis.
        """
        
        try:
            # Gera código corrigido
            corrected_code = self.query_generator.generate_code(correction_prompt)
            logger.info("Código corrigido gerado")
            
            # Se for um erro de SQL, tenta extrair a consulta para validação
            if is_sql_error:
                import re
                sql_matches = re.findall(r'execute_sql_query\([\'"](.+?)[\'"]\)', corrected_code)
                
                if sql_matches:
                    # Pega a primeira consulta SQL encontrada
                    sql_query = sql_matches[0]
                    logger.info(f"Validando consulta SQL corrigida: {sql_query}")
                    
                    # Verifica se a consulta menciona tabelas inexistentes
                    for table in re.findall(r'FROM\s+(\w+)', sql_query, re.IGNORECASE):
                        if table not in self.datasets:
                            # Se a tabela não existir, modifica o código para retornar uma mensagem amigável
                            logger.warning(f"Correção ainda referencia tabela inexistente: {table}")
                            corrected_code = f"""
                            result = {{
                                "type": "string",
                                "value": "Não foi possível processar a consulta porque a tabela '{table}' não está disponível. Tabelas disponíveis: {datasets_list}"
                            }}
                            """
                            break
            
            # Marca o contexto para evitar loop infinito
            context_with_flag = context.copy()
            context_with_flag['correction_attempt'] = True
            
            # Executa o código corrigido
            execution_result = self.code_executor.execute_code(
                corrected_code,
                context=context_with_flag,
                output_type=self.agent_state.output_type
            )
            
            # Verifica se a correção foi bem-sucedida
            if not execution_result["success"]:
                # Se a primeira correção falhar, tenta uma correção mais simples para casos graves
                error_msg = execution_result["error"]
                logger.error(f"Primeira correção falhou: {error_msg}")
                
                # Tentativa de fallback - gera uma resposta mais simples
                simplified_correction = f"""
                # A consulta apresentou problemas técnicos. Criando uma resposta simplificada.
                
                result = {{
                    "type": "string",
                    "value": "Não foi possível processar a consulta '{query}' devido a limitações técnicas. Erro: {error_msg}"
                }}
                """
                
                # Executa a versão simplificada
                fallback_result = self.code_executor.execute_code(
                    simplified_correction,
                    context=context_with_flag,
                    output_type=self.agent_state.output_type
                )
                
                if not fallback_result["success"]:
                    return ErrorResponse(f"Erro ao processar consulta (após todas as tentativas de correção): {error_msg}")
                else:
                    # Usa o resultado do fallback
                    result = fallback_result["result"]
                    formatted_result = self._format_result_for_parser(result)
                    response = self.response_parser.parse(formatted_result, simplified_correction)
                    return response
            
            # Processa o resultado da execução corrigida
            result = execution_result["result"]
            formatted_result = self._format_result_for_parser(result)
            response = self.response_parser.parse(formatted_result, corrected_code)
            
            logger.info(f"Consulta corrigida e processada com sucesso. Tipo de resposta: {response.type}")
            return response
            
        except Exception as e:
            logger.error(f"Erro durante tentativa de correção: {str(e)}")
            
            # Em caso de erro na correção, cria uma resposta de erro mais amigável
            try:
                simplified_response = f"""
                result = {{
                    "type": "string",
                    "value": "Não foi possível processar a consulta devido a problemas técnicos. Por favor, tente reformular sua pergunta de forma mais simples ou verifique se os dados mencionados existem."
                }}
                """
                
                # Tenta executar a resposta simplificada
                fallback_execution = self.code_executor.execute_code(
                    simplified_response,
                    context=context,
                    output_type=self.agent_state.output_type
                )
                
                if fallback_execution["success"]:
                    result = fallback_execution["result"]
                    formatted_result = self._format_result_for_parser(result)
                    return self.response_parser.parse(formatted_result, simplified_response)
            except:
                pass
                
            # Se tudo falhar, retorna erro
            return ErrorResponse(f"Erro ao processar consulta: {error_msg} (Correção falhou: {str(e)})")
    
    def _generate_prompt(self, query: str) -> str:
        """
        Gera um prompt detalhado para o LLM com informações sobre datasets disponíveis.
        Inclui metadados avançados e relacionamentos detectados pelo analisador.
        
        Args:
            query: Consulta em linguagem natural
            
        Returns:
            Prompt formatado
        """
        # Adiciona a consulta ao histórico
        self.agent_state.memory.add_message(query)
        
        # Coleta todas as relações detectadas entre datasets
        all_relationships = []
        for name, dataset in self.datasets.items():
            if hasattr(dataset, 'analyzed_metadata') and dataset.analyzed_metadata:
                if 'relationships' in dataset.analyzed_metadata:
                    rel_info = dataset.analyzed_metadata['relationships']
                    
                    # Relações outgoing
                    if 'outgoing' in rel_info:
                        for rel in rel_info['outgoing']:
                            all_relationships.append(
                                f"- {name}.{rel['source_column']} → {rel['target_dataset']}.{rel['target_column']}"
                            )
                    
        # Informações detalhadas dos datasets com metadados enriquecidos
        datasets_info = []
        for name, dataset in self.datasets.items():
            # Informações básicas
            dataset_info = [
                f"Dataset '{name}':",
                f"  - Descrição: {dataset.description}",
                f"  - Registros: {len(dataset.dataframe)}",
                f"  - Colunas: {len(dataset.dataframe.columns)}"
            ]
            
            # Adiciona informação de chave primária se detectada
            if hasattr(dataset, 'primary_key') and dataset.primary_key:
                dataset_info.append(f"  - Chave Primária: {dataset.primary_key}")
            
            # Adiciona informações de colunas com tipos detectados
            column_info = []
            for col in dataset.dataframe.columns:
                # Usa o tipo detectado pelo analisador quando disponível
                if hasattr(dataset, 'column_types') and col in dataset.column_types:
                    col_type = dataset.column_types[col]
                else:
                    col_type = str(dataset.dataframe[col].dtype)
                
                # Marca colunas especiais (chaves primárias, estrangeiras)
                suffix = ""
                if hasattr(dataset, 'primary_key') and col == dataset.primary_key:
                    suffix = " (PK)"
                elif hasattr(dataset, 'potential_foreign_keys') and col in dataset.potential_foreign_keys:
                    suffix = " (FK)"
                
                column_info.append(f"    * {col}: {col_type}{suffix}")
            
            dataset_info.append("  - Detalhes das colunas:")
            dataset_info.extend(column_info)
            
            datasets_info.append("\n".join(dataset_info))
        
        # Junta todas as informações dos datasets
        datasets_info_text = "\n\n".join(datasets_info)
        
        # Exemplos de valores para cada dataset
        dataset_samples = "\n".join([
            f"Exemplos de '{name}':\n{dataset.dataframe.head(2).to_string()}\n"
            for name, dataset in self.datasets.items()
        ])
        
        # Informações sobre relacionamentos detectados
        relationships_info = ""
        if all_relationships:
            relationships_info = """
            ## Relacionamentos Detectados Entre Datasets
            
            Os seguintes relacionamentos foram detectados entre os datasets:
            
            """ + "\n".join(all_relationships)
        
        # Informações sobre funções SQL suportadas
        sql_functions_info = """
        ## Funções SQL Suportadas
        
        O sistema usa DuckDB para executar consultas SQL e foi expandido para suportar funções de vários dialetos SQL:
        
        ### Funções de Data
        - DATE_FORMAT(coluna, formato) - Formata data/hora no estilo MySQL (ex: DATE_FORMAT(data, '%Y-%m-%d'))
        - strftime(formato, coluna) - Formata data/hora no estilo SQLite (ex: strftime('%Y-%m-%d', data))
        - DATE(string) - Converte string para data (ex: DATE '2023-01-01' ou DATE(coluna))
        - TO_DATE(string) - Converte string para data no estilo PostgreSQL
        - DATE_PART(parte, data) - Extrai parte específica de data (ex: DATE_PART('year', data))
        - DATEADD(parte, n, data) - Adiciona intervalo de tempo a uma data no estilo SQL Server
        - EXTRACT(parte FROM data) - Extrai parte de data no estilo PostgreSQL
        
        ### Funções de String
        - CONCAT(a, b) - Concatena strings no estilo MySQL/PostgreSQL
        - a || b - Concatena strings no estilo SQLite/PostgreSQL
        - CONCAT_WS(separador, a, b, ...) - Concatena strings com separador
        - SUBSTR(string, inicio, tamanho) - Extrai substring
        - SUBSTRING(string, inicio, tamanho) - Mesmo que SUBSTR
        - LOWER(string) - Converte para minúsculas
        - UPPER(string) - Converte para maiúsculas
        - TRIM(string) - Remove espaços do início e fim
        
        ### Funções de Agregação
        - COUNT(), SUM(), AVG(), MIN(), MAX() - Funções de agregação padrão
        - GROUP_CONCAT(coluna) - Concatena valores agrupados com vírgula (estilo MySQL)
        - STRING_AGG(coluna, separador) - Concatena valores com separador (estilo PostgreSQL)
        
        ### Funções de Casting e Conversão
        - CAST(valor AS tipo) - Converte para outro tipo de dados
        - valor::tipo - Converte para outro tipo no estilo PostgreSQL
        - CONVERT(tipo, valor) - Converte para outro tipo no estilo SQL Server/MySQL
        """
        
        # Exemplos de consultas SQL baseados nas tabelas reais
        sql_examples = self._generate_sql_examples()
        
        # Construindo o prompt completo
        prompt = f"""
        # Instruções para Geração de Código Python

        Você deve gerar código Python para responder à seguinte consulta:
        
        CONSULTA: "{query}"
        
        ## Datasets Disponíveis

        {datasets_info_text}
        
        {relationships_info}
        
        ## Exemplos de Dados

        {dataset_samples}
        
        {sql_functions_info}
        
        ## Exemplos de Consultas SQL Válidas
        
        {sql_examples}
        
        ## Requisitos

        1. Use a função `execute_sql_query(sql_query)` para executar consultas SQL
        2. A função execute_sql_query retorna um DataFrame pandas
        3. O código DEVE definir uma variável `result` no formato: {{"type": tipo, "value": valor}}
        4. Tipos válidos são: "string", "number", "dataframe", ou "plot"
        5. Para visualizações, use matplotlib e salve o gráfico com plt.savefig()

        ## Importante

        - Importe apenas as bibliotecas necessárias
        - Use SQL para consultas e agregações sempre que possível
        - Para visualizações, use o tipo "plot" e salve o gráfico em um arquivo
        - Defina result = {{"type": "tipo_aqui", "value": valor_aqui}} ao final
        - NÃO inclua comentários explicativos, apenas o código funcional
        - Use apenas os datasets indicados acima, NÃO tente usar tabelas inexistentes
        - Aproveite os relacionamentos detectados para fazer JOINs entre tabelas relacionadas
        - Adapte consultas SQL para compatibilidade com DuckDB usando as funções listadas acima
        """
        
        return prompt
        
    def _generate_sql_examples(self) -> str:
        """
        Gera exemplos de consultas SQL baseados nos datasets disponíveis.
        
        Returns:
            str: Texto com exemplos de consultas SQL
        """
        examples = []
        available_datasets = list(self.datasets.keys())
        
        if not available_datasets:
            return "Nenhum dataset disponível para gerar exemplos."
        
        # Exemplo básico de seleção
        if len(available_datasets) > 0:
            ds_name = available_datasets[0]
            ds = self.datasets[ds_name]
            columns = list(ds.dataframe.columns)
            
            if columns:
                examples.append(f"- SELECT * FROM {ds_name} LIMIT 5")
                examples.append(f"- SELECT {', '.join(columns[:3])} FROM {ds_name}")
        
        # Exemplo de filtro
        if len(available_datasets) > 0:
            ds_name = available_datasets[0]
            ds = self.datasets[ds_name]
            
            # Tenta encontrar uma coluna numérica para filtro
            numeric_col = None
            for col in ds.dataframe.columns:
                if pd.api.types.is_numeric_dtype(ds.dataframe[col]):
                    numeric_col = col
                    break
            
            if numeric_col:
                examples.append(f"- SELECT * FROM {ds_name} WHERE {numeric_col} > 100")
        
        # Exemplo de agregação
        if len(available_datasets) > 0:
            ds_name = available_datasets[0]
            ds = self.datasets[ds_name]
            
            # Tenta encontrar colunas categóricas e numéricas
            numeric_col = None
            categorical_col = None
            
            for col in ds.dataframe.columns:
                if pd.api.types.is_numeric_dtype(ds.dataframe[col]):
                    numeric_col = col
                elif pd.api.types.is_object_dtype(ds.dataframe[col]) and ds.dataframe[col].nunique() < 20:
                    categorical_col = col
            
            if numeric_col and categorical_col:
                examples.append(f"- SELECT {categorical_col}, SUM({numeric_col}) as total FROM {ds_name} GROUP BY {categorical_col}")
        
        # Exemplo de JOIN se tivermos relacionamentos
        joins_added = False
        
        for ds_name, ds in self.datasets.items():
            if hasattr(ds, 'analyzed_metadata') and 'relationships' in ds.analyzed_metadata:
                rel_info = ds.analyzed_metadata['relationships']
                
                if 'outgoing' in rel_info and rel_info['outgoing']:
                    # Pega a primeira relação
                    rel = rel_info['outgoing'][0]
                    source_col = rel['source_column']
                    target_dataset = rel['target_dataset']
                    target_col = rel['target_column']
                    
                    # Verifica se temos o dataset alvo
                    if target_dataset in self.datasets:
                        # Pega algumas colunas de cada dataset
                        source_cols = list(ds.dataframe.columns)[:2]
                        target_cols = list(self.datasets[target_dataset].dataframe.columns)[:2]
                        
                        join_example = f"""- SELECT s.{source_cols[0]}, t.{target_cols[0]}
  FROM {ds_name} s
  JOIN {target_dataset} t ON s.{source_col} = t.{target_col}"""
                        
                        examples.append(join_example)
                        joins_added = True
                        break
            
            if joins_added:
                break
        
        # Exemplo com data se tivermos uma coluna de data
        date_example_added = False
        for ds_name, ds in self.datasets.items():
            # Procura uma coluna com "data" no nome
            date_col = None
            for col in ds.dataframe.columns:
                if "data" in col.lower() or "date" in col.lower() or "dt_" in col.lower():
                    date_col = col
                    break
            
            if date_col:
                examples.append(f"- SELECT strftime('%Y-%m', {date_col}) as mes, COUNT(*) as total FROM {ds_name} GROUP BY mes")
                date_example_added = True
                break
        
        # Adiciona exemplo mais simples se não achou coluna de data
        if not date_example_added and len(available_datasets) > 0:
            examples.append("- SELECT EXTRACT(YEAR FROM CURRENT_DATE) as ano_atual")
        
        # Retorna todos os exemplos formatados
        if examples:
            return "\n".join(examples)
        else:
            return "Exemplos não disponíveis para os datasets atuais."
    
    def _format_result_for_parser(self, result: Any) -> Dict[str, Any]:
        """
        Formata o resultado da execução para o formato esperado pelo parser.
        
        Args:
            result: Resultado da execução
            
        Returns:
            Dicionário com 'type' e 'value'
        """
        # Se já estiver no formato esperado
        if isinstance(result, dict) and "type" in result and "value" in result:
            # Verifica se o tipo é 'plot' e o valor não é uma string de caminho válido
            if result["type"] == "plot":
                value = result["value"]
                if not isinstance(value, str) or (not value.endswith(('.png', '.jpg', '.svg', '.pdf')) and "data:image" not in value):
                    # Tenta salvar a imagem se for uma figura matplotlib
                    try:
                        import matplotlib.pyplot as plt
                        if isinstance(value, plt.Figure):
                            filename = f"plot_{int(time.time())}.png"
                            value.savefig(filename)
                            result["value"] = filename
                            logger.info(f"Figura matplotlib salva automaticamente como {filename}")
                        else:
                            # Fallback para string se não for um caminho ou figura válida
                            logger.warning(f"Valor inválido para tipo 'plot'. Convertendo para string.")
                            return {"type": "string", "value": "Não foi possível gerar uma visualização válida. Valor não é um caminho para imagem ou figura."}
                    except Exception as e:
                        logger.error(f"Erro ao processar visualização: {str(e)}")
                        return {"type": "string", "value": f"Erro ao processar visualização: {str(e)}"}
            
            return result
        
        # Infere o tipo com base no valor
        if isinstance(result, pd.DataFrame):
            return {"type": "dataframe", "value": result}
        elif isinstance(result, (int, float)):
            return {"type": "number", "value": result}
        elif isinstance(result, str):
            # Verifica se parece ser um caminho para um plot
            if result.endswith(('.png', '.jpg', '.svg', '.pdf')) or "data:image" in result:
                return {"type": "plot", "value": result}
            else:
                return {"type": "string", "value": result}
        else:
            # Verifica se é uma figura matplotlib
            try:
                import matplotlib.pyplot as plt
                if hasattr(result, 'savefig') or isinstance(result, plt.Figure):
                    filename = f"plot_{int(time.time())}.png"
                    plt.savefig(filename)
                    plt.close()
                    return {"type": "plot", "value": filename}
            except:
                pass
                
            # Tentativa genérica para outros tipos
            return {"type": "string", "value": str(result)}
    
    def execute_direct_query(
        self, 
        query: str, 
        dataset_name: Optional[str] = None
    ) -> BaseResponse:
        """
        Executa uma consulta SQL diretamente em um dataset.
        
        Args:
            query: Consulta SQL
            dataset_name: Nome do dataset alvo (opcional se houver apenas um)
            
        Returns:
            Resultado da consulta
        """
        logger.info(f"Executando consulta SQL direta: {query}")
        
        try:
            # Determina qual dataset usar
            if dataset_name:
                if dataset_name not in self.datasets:
                    return ErrorResponse(f"Dataset '{dataset_name}' não encontrado")
                df = self.datasets[dataset_name].dataframe
            elif len(self.datasets) == 1:
                # Se há apenas um dataset, usa ele
                df = next(iter(self.datasets.values())).dataframe
            else:
                # Se há múltiplos datasets e nenhum especificado
                return ErrorResponse("Múltiplos datasets disponíveis. Especifique qual usar.")
            
            # Executa a consulta SQL usando pandas
            # Em uma implementação real, usaríamos o DuckDB ou SQLite para suporte SQL real
            result_df = pd.read_sql_query(query, df)
            
            # Retorna como DataFrameResponse
            return DataFrameResponse(result_df)
        
        except Exception as e:
            logger.error(f"Erro ao executar consulta SQL: {str(e)}")
            return ErrorResponse(f"Erro ao executar consulta SQL: {str(e)}")
    
    def generate_chart(
        self, 
        data: Union[pd.DataFrame, pd.Series], 
        chart_type: str, 
        x: Optional[str] = None, 
        y: Optional[str] = None,
        title: Optional[str] = None,
        save_path: Optional[str] = None
    ) -> ChartResponse:
        """
        Gera uma visualização a partir de um DataFrame.
        
        Args:
            data: DataFrame ou Series para visualização
            chart_type: Tipo de gráfico (bar, line, scatter, hist, etc.)
            x: Coluna para eixo x (opcional)
            y: Coluna para eixo y (opcional)
            title: Título do gráfico (opcional)
            save_path: Caminho para salvar o gráfico (opcional)
            
        Returns:
            ChartResponse com a visualização
        """
        try:
            import matplotlib.pyplot as plt
            
            # Configura o gráfico
            plt.figure(figsize=(10, 6))
            
            # Determina o tipo de gráfico
            if chart_type == 'bar':
                if x and y:
                    data.plot(kind='bar', x=x, y=y)
                else:
                    data.plot(kind='bar')
            elif chart_type == 'line':
                if x and y:
                    data.plot(kind='line', x=x, y=y)
                else:
                    data.plot(kind='line')
            elif chart_type == 'scatter':
                if x and y:
                    data.plot(kind='scatter', x=x, y=y)
                else:
                    # Scatter requer x e y
                    raise ValueError("Scatter plot requer especificação de x e y")
            elif chart_type == 'hist':
                if y:
                    data[y].plot(kind='hist')
                else:
                    data.plot(kind='hist')
            elif chart_type == 'boxplot':
                data.boxplot()
            elif chart_type == 'pie':
                if y:
                    data.plot(kind='pie', y=y)
                else:
                    data.plot(kind='pie')
            else:
                raise ValueError(f"Tipo de gráfico não suportado: {chart_type}")
            
            # Adiciona título se fornecido
            if title:
                plt.title(title)
            
            # Ajusta o layout
            plt.tight_layout()
            
            # Determina caminho para salvar
            if not save_path:
                # Gera nome baseado no tipo e título
                title_slug = "chart" if not title else title.replace(" ", "_").lower()
                save_path = f"{title_slug}_{chart_type}.png"
            
            # Salva o gráfico
            plt.savefig(save_path)
            plt.close()
            
            # Retorna resposta com o caminho
            logger.info(f"Gráfico gerado e salvo em: {save_path}")
            return ChartResponse(save_path)
            
        except Exception as e:
            logger.error(f"Erro ao gerar gráfico: {str(e)}")
            raise ValueError(f"Falha ao gerar gráfico: {str(e)}")
    
    def sanitize_query(self, query: str) -> str:
        """
        Sanitiza uma consulta do usuário removendo conteúdo potencialmente perigoso.
        
        Args:
            query: Consulta do usuário
            
        Returns:
            Consulta sanitizada
        """
        # Remove comandos SQL perigosos
        dangerous_patterns = [
            r'DROP\s+TABLE',
            r'DELETE\s+FROM',
            r'TRUNCATE\s+TABLE',
            r'ALTER\s+TABLE',
            r'CREATE\s+TABLE',
            r'UPDATE\s+.+\s+SET',
            r'INSERT\s+INTO',
            r'EXECUTE\s+',
            r'EXEC\s+',
            r';.*--'
        ]
        
        sanitized_query = query
        
        # Verifica e remove padrões perigosos
        for pattern in dangerous_patterns:
            import re
            sanitized_query = re.sub(pattern, "[REMOVIDO]", sanitized_query, flags=re.IGNORECASE)
        
        return sanitized_query