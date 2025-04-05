"""
Motor de análise principal que integra todos os componentes do sistema.
"""

import os
import sys
import logging
import pandas as pd
import time
import json
from typing import Dict, List, Optional, Any, Union

# Adiciona o diretório raiz ao path para facilitar importações
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# Tenta importar os conectores no início do módulo
try:
    from connector.data_connector_factory import DataConnectorFactory
    from connector.datasource_config import DataSourceConfig
    _CONNECTORS_AVAILABLE = True
except ImportError:
    _CONNECTORS_AVAILABLE = False

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

# Importação dos módulos refatorados
from core.engine.dataset import Dataset
from core.engine.sql_executor import SQLExecutor
from core.engine.alternative_flow import AlternativeFlow
from core.engine.feedback_manager import FeedbackManager

# Importação do módulo de integração com LLMs
from llm_integration import LLMIntegration, LLMQueryGenerator

# Configura o logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("analysis_engine.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("analysis_engine")


class AnalysisEngine:
    """
    Motor de análise que integra componentes core para processamento de consultas em linguagem natural.
    
    Esta classe implementa:
    - Carregamento e gerenciamento de datasets
    - Execução segura de código
    - Geração de prompts para LLM
    - Processamento de consultas em linguagem natural
    - Tratamento de respostas e conversão de formatos
    
    O design adota o padrão de fachada (Facade), orquestrando componentes especializados:
    - Dataset: Gerencia dados e metadados
    - SQLExecutor: Processa consultas SQL com adaptação de dialetos
    - AlternativeFlow: Provê fluxos alternativos para erros e sugestões
    - FeedbackManager: Gerencia feedback do usuário e otimização de consultas
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
        
        # Inicializa componentes modulares
        self.feedback_manager = FeedbackManager()
        
        # Inicializa o módulo de fluxo alternativo (usado para tratamento de erros e sugestões)
        self.alternative_flow = None  # Será inicializado depois que tivermos datasets
        
        # Inicializa o executor SQL (será configurado após carregar datasets)
        self.sql_executor = None
        
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
                
                # Determina o tipo de arquivo pela extensão e cria o conector apropriado
                if data.endswith('.csv'):
                    # Verificamos se os módulos já foram importados no início do arquivo
                    if _CONNECTORS_AVAILABLE:
                        try:
                            config = DataSourceConfig.from_dict({
                                "id": name,
                                "source_type": "duckdb_csv",
                                "path": data
                            })
                            
                            connector = DataConnectorFactory.create_connector(config)
                            connector.connect()
                            df = connector.read_data()
                            # Armazenamos o conector para consultas futuras
                            self.datasets[name + "_connector"] = connector
                            logger.info(f"Usando DuckDB para carregar arquivo CSV: {data}")
                        except Exception as e:
                            logger.warning(f"Erro ao usar DuckDB para CSV: {str(e)}. Usando pandas diretamente.")
                            df = pd.read_csv(data)
                    else:
                        logger.warning(f"Conectores não disponíveis. Usando pandas diretamente.")
                        df = pd.read_csv(data)
                    
                elif data.endswith(('.xls', '.xlsx')):
                    # Verificamos se os módulos já foram importados no início do arquivo
                    if _CONNECTORS_AVAILABLE:
                        try:
                            config = DataSourceConfig.from_dict({
                                "id": name,
                                "source_type": "duckdb_xls",
                                "path": data,
                                "sheet_name": "all",
                                "create_combined_view": True
                            })
                            
                            connector = DataConnectorFactory.create_connector(config)
                            connector.connect()
                            df = connector.read_data()
                            # Armazenamos o conector para consultas futuras
                            self.datasets[name + "_connector"] = connector
                            logger.info(f"Usando DuckDB para carregar arquivo Excel: {data}")
                        except Exception as e:
                            logger.warning(f"Erro ao usar DuckDB para Excel: {str(e)}. Usando pandas diretamente.")
                            df = pd.read_excel(data)
                    else:
                        logger.warning(f"Conectores não disponíveis. Usando pandas diretamente.")
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
            
            # Cria o objeto Dataset para armazenar os dados e metadados
            dataset = Dataset(df, name, description, schema)
            self.datasets[name] = dataset
            
            # Atualiza a lista de dataframes no estado do agente
            self.agent_state.dfs = list(self.datasets.values())
            
            # Inicializa/atualiza o executor SQL com os novos dados
            if self.sql_executor is None:
                self.sql_executor = SQLExecutor(datasets=self.datasets)
            else:
                self.sql_executor.update_datasets(self.datasets)
            
            # Inicializa/atualiza o módulo de fluxo alternativo
            if self.alternative_flow is None:
                self.alternative_flow = AlternativeFlow(
                    datasets=self.datasets,
                    llm_generator=self.query_generator
                )
            else:
                self.alternative_flow.update_datasets(self.datasets)
            
            logger.info(f"Dataset '{name}' carregado com {len(df)} registros e {len(df.columns)} colunas.")
            
        except Exception as e:
            error_msg = f"Erro ao carregar dados: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e
    
    def __del__(self):
        """
        Método destrutor para limpar recursos.
        """
        # Fecha conexões de conectores
        for key, value in self.datasets.items():
            if key.endswith("_connector") and hasattr(value, 'close'):
                try:
                    value.close()
                except:
                    pass

        # Fecha as conexões com DuckDB se existirem
        if hasattr(self, 'sql_executor') and self.sql_executor:
            try:
                self.sql_executor.close()
            except:
                pass

        # Fecha as conexões LLM se existirem
        if hasattr(self, 'query_generator') and self.query_generator:
            try:
                self.query_generator.close_connections()
            except:
                pass
    
    def generate_analysis(self, result: BaseResponse, query: str) -> str:
        """
        Gera uma análise automatizada do resultado de uma consulta.
        
        Args:
            result: Objeto de resposta obtido
            query: Consulta original
            
        Returns:
            str: Texto com análise do resultado
        """
        if result.type == "dataframe":
            df = result.value
            analysis = [f"A consulta retornou {len(df)} registros com {len(df.columns)} colunas."]
            
            # Análise adicional se tivermos poucas linhas
            if len(df) <= 10:
                analysis.append("Conjunto de resultados pequeno, pode ser necessário expandir a consulta para obter mais dados.")
                
            # Verifica valores nulos
            null_counts = df.isnull().sum()
            if null_counts.any():
                cols_with_nulls = [f"{col} ({null_counts[col]} valores)" for col in null_counts.index if null_counts[col] > 0]
                if cols_with_nulls:
                    analysis.append(f"Colunas com valores nulos: {', '.join(cols_with_nulls)}")
                    
            # Verifica colunas numéricas
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            if numeric_cols:
                # Calcula estatísticas básicas para colunas numéricas
                stats = df[numeric_cols].describe().to_dict()
                for col in numeric_cols[:2]:  # Limita a 2 colunas para não sobrecarregar
                    col_stats = stats[col]
                    analysis.append(f"Estatísticas para '{col}': Min={col_stats['min']:.2f}, Média={col_stats['mean']:.2f}, Max={col_stats['max']:.2f}")
            
            return "\n".join(analysis)
            
        elif result.type == "chart":
            analysis = [f"Visualização gerada com base na consulta: '{query}'."]
            
            # Se temos informações de séries no gráfico
            if result.chart_format == "apex" and isinstance(result.value, dict):
                config = result.value
                if "series" in config:
                    series_count = len(config["series"]) if isinstance(config["series"], list) else 1
                    analysis.append(f"O gráfico contém {series_count} série(s) de dados.")
                    
                if "title" in config and "text" in config["title"]:
                    analysis.append(f"Título do gráfico: {config['title']['text']}.")
                    
            return "\n".join(analysis)
            
        elif result.type == "number":
            return f"O valor numérico obtido foi {result.value}."
            
        elif result.type == "string":
            return f"A resposta obtida é: '{result.value[:100]}{'...' if len(result.value) > 100 else ''}'."
            
        else:
            return f"Consulta processada com sucesso."
    
    def generate_chart(
        self,
        data: pd.DataFrame,
        chart_type: str = "bar",
        x: Optional[Union[str, List[str]]] = None,
        y: Optional[Union[str, List[str]]] = None,
        title: Optional[str] = None,
        chart_format: str = "apex"
    ) -> ChartResponse:
        """
        Gera um gráfico a partir de um DataFrame.
        
        Args:
            data: DataFrame com os dados para o gráfico
            chart_type: Tipo de gráfico (bar, line, pie, area, scatter, etc)
            x: Coluna(s) ou índice para o eixo X
            y: Coluna(s) para o eixo Y
            title: Título do gráfico
            chart_format: Formato de saída ('apex' para ApexCharts)
            
        Returns:
            ChartResponse: Resposta contendo o gráfico gerado
        """
        # Implementação para gráficos do tipo ApexCharts
        if chart_format.lower() == "apex":
            return self._generate_apex_chart(data, chart_type, x, y, title)
            
        # Fallback para implementação matplotlib (ou outros no futuro)
        return self._generate_basic_chart(data, chart_type, x, y, title)
    
    def _generate_apex_chart(
        self,
        data: pd.DataFrame,
        chart_type: str,
        x: Optional[Union[str, List[str]]],
        y: Optional[Union[str, List[str]]],
        title: Optional[str]
    ) -> ChartResponse:
        """
        Gera um gráfico no formato ApexCharts (JSON).
        
        Args:
            data: DataFrame com os dados
            chart_type: Tipo de gráfico
            x: Coluna(s) para o eixo X
            y: Coluna(s) para o eixo Y
            title: Título do gráfico
            
        Returns:
            ChartResponse: Objeto de resposta com configuração ApexCharts
        """
        # Usa o conversor apropriado de utils
        from utils.chart_converters import dataframe_to_apex
        
        # Conversão de tipos de gráfico para formato compatível
        chart_type_mapping = {
            "bar": "bar",
            "column": "bar",
            "line": "line",
            "area": "area",
            "pie": "pie",
            "donut": "donut",
            "scatter": "scatter",
            "bubble": "bubble",
            "heatmap": "heatmap",
            "radialBar": "radialBar",
            "radar": "radar",
            "candlestick": "candlestick"
        }
        
        # Padroniza o tipo de gráfico
        normalized_type = chart_type_mapping.get(chart_type.lower(), "bar")
        
        # Configura o título
        if title is None:
            title = f"Gráfico de {normalized_type.capitalize()}"
            if y and isinstance(y, str):
                title = f"{title} - {y}"
        
        # Gera o gráfico com ApexCharts
        try:
            apex_config = dataframe_to_apex(
                df=data,
                chart_type=normalized_type,
                x_column=x,
                y_columns=y if isinstance(y, list) else [y] if y else None,
                title=title
            )
            
            # Retorna com formato apex
            return ChartResponse(
                value={"format": "apex", "config": apex_config},
                chart_format="apex"
            )
            
        except Exception as e:
            logger.error(f"Erro ao gerar gráfico ApexCharts: {str(e)}")
            # Fallback para implementação básica
            return self._generate_basic_chart(data, chart_type, x, y, title)
    
    def _generate_basic_chart(
        self,
        data: pd.DataFrame,
        chart_type: str,
        x: Optional[Union[str, List[str]]],
        y: Optional[Union[str, List[str]]],
        title: Optional[str]
    ) -> ChartResponse:
        """
        Gera um gráfico simples como fallback.
        
        Args:
            data: DataFrame com os dados
            chart_type: Tipo de gráfico
            x: Coluna(s) para o eixo X
            y: Coluna(s) para o eixo Y
            title: Título do gráfico
            
        Returns:
            ChartResponse: Objeto de resposta com o gráfico
        """
        # Implementação básica (texto) para quando falha a implementação principal
        message = f"Gráfico do tipo {chart_type} com dados de {len(data)} registros."
        
        if x and y:
            message += f" Eixo X: {x}, Eixo Y: {y}."
            
        if title:
            message += f" Título: {title}."
            
        return ChartResponse(value=message)
    
    def process_query(self, query: str, retry_count: int = 0, max_retries: int = 3, feedback: str = None) -> BaseResponse:
        """
        Processa uma consulta em linguagem natural e retorna o resultado.
        
        Args:
            query: Consulta em linguagem natural
            retry_count: Contador de tentativas (para recursão)
            max_retries: Número máximo de tentativas permitidas
            feedback: Feedback opcional do usuário sobre consultas anteriores
            
        Returns:
            BaseResponse: Resposta formatada (DataFrame, Chart, Number, String, etc.)
        """
        try:
            # Aplicar filtragem de segurança na consulta
            safe_query = self._sanitize_query(query)
            
            # Criar objeto de consulta do usuário
            user_query = UserQuery(safe_query)
            
            # Inicializar alternativeFlow se necessário
            if self.alternative_flow is None and self.datasets:
                self.alternative_flow = AlternativeFlow(
                    datasets=self.datasets,
                    llm_generator=self.query_generator
                )
            
            # Aplicar fluxo alternativo se disponível
            if self.alternative_flow and not retry_count:
                # Verifica se a consulta menciona dados ou entidades inexistentes
                alternative_result = self.alternative_flow.pre_query_check(user_query)
                if alternative_result:
                    return alternative_result
            
            # Preparar o prompt para o LLM
            prompt_generator = GeneratePythonCodeWithSQLPrompt(
                datasets=self.datasets,
                agent_state=self.agent_state,
                output_type=self.agent_state.output_type
            )
            
            system_message = prompt_generator.generate_system_message()
            user_message = prompt_generator.generate_user_message(safe_query, feedback)
            
            # Gerar código Python para resolver a consulta
            code = self.query_generator.generate_code(
                system_message=system_message,
                user_message=user_message
            )
            
            # Armazenar o código gerado para referência futura
            self.last_code_generated = code
            
            # Executar o código gerado
            execution_context = {
                "datasets": self.datasets,
                "sql_executor": self.sql_executor,
                "agent_state": self.agent_state,
                "agent_memory": self.agent_state.memory
            }
            
            result = self.code_executor.execute_code(code, execution_context)
            
            # Interpretar o resultado da execução
            result = self._format_result_for_parser(result)
            
            # Converte o resultado para o tipo de resposta adequado
            response = self.response_parser.parse_response(result)
            
            # Se o resultado for um erro, registra e tenta fluxo alternativo
            if response.type == "error" and self.alternative_flow and retry_count < max_retries:
                alternative_result = self.alternative_flow.handle_error(
                    query=user_query, 
                    error=response.value, 
                    code=code
                )
                
                if alternative_result:
                    return alternative_result
                
                # Se não há resultado alternativo, tenta reformular a consulta
                rephrased_query = self.alternative_flow.rephrase_query(user_query, response.value)
                
                if rephrased_query and rephrased_query != query:
                    logger.info(f"Consulta reformulada após exceção: {rephrased_query}")
                    
                    # Reinicia o processamento com a consulta reformulada
                    return self.process_query(rephrased_query, retry_count + 1, max_retries)
            
            return response
            
        except Exception as e:
            logger.error(f"Erro ao processar consulta: {str(e)}")
            
            # Tenta processar com fluxo alternativo
            if self.alternative_flow and retry_count < max_retries:
                alternative_result = self.alternative_flow.handle_error(
                    query=UserQuery(query), 
                    error=str(e), 
                    code=self.last_code_generated
                )
                
                if alternative_result:
                    return alternative_result
                
                # Se não há resultado alternativo, tenta reformular a consulta
                rephrased_query = self.alternative_flow.rephrase_query(UserQuery(query), str(e))
                
                if rephrased_query and rephrased_query != query:
                    logger.info(f"Consulta reformulada após exceção: {rephrased_query}")
                    
                    # Reinicia o processamento com a consulta reformulada
                    return self.process_query(rephrased_query, retry_count + 1, max_retries)
            
            return ErrorResponse(f"Erro ao processar consulta: {str(e)}")
    
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
            # Verifica se é um gráfico
            if result["type"] == "chart":
                # Suporte a format apex
                if isinstance(result["value"], dict) and "format" in result["value"]:
                    if result["value"]["format"] == "apex" and "config" in result["value"]:
                        # Já está no formato correto para ApexCharts
                        return result
                
                # Compatibilidade com parâmetro antigo 'plot'
                return {"type": "chart", "value": result["value"]}
                
            # Compatibilidade com tipo 'plot' antigo
            elif result["type"] == "plot":
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
                
                # Converte 'plot' antigo para 'chart' com formato 'image'
                return {"type": "chart", "value": result["value"]}
            
            return result
        
        # Infere o tipo com base no valor
        if isinstance(result, pd.DataFrame):
            return {"type": "dataframe", "value": result}
        elif isinstance(result, (int, float)):
            return {"type": "number", "value": result}
        elif isinstance(result, str):
            # Verifica se parece ser um caminho para um plot
            if result.endswith(('.png', '.jpg', '.svg', '.pdf')) or "data:image" in result:
                return {"type": "chart", "value": result}
            else:
                return {"type": "string", "value": result}
        elif isinstance(result, dict) and "chart" in result and "series" in result:
            # Detecta um possível config do ApexCharts direto
            return {
                "type": "chart", 
                "value": {
                    "format": "apex",
                    "config": result
                }
            }
        else:
            # Verifica se é uma figura matplotlib
            try:
                import matplotlib.pyplot as plt
                if hasattr(result, 'savefig') or isinstance(result, plt.Figure):
                    filename = f"plot_{int(time.time())}.png"
                    plt.savefig(filename)
                    plt.close()
                    return {"type": "chart", "value": filename}
            except:
                pass
            
            # Se não conseguimos identificar, retorna como string
            return {"type": "string", "value": str(result)}
    
    def _sanitize_query(self, query: str) -> str:
        """
        Remove conteúdo potencialmente inseguro da consulta.
        
        Args:
            query: Consulta original
            
        Returns:
            str: Consulta sanitizada
        """
        if not query:
            return ""
            
        # Convertemos para string (caso seja outro tipo)
        sanitized_query = str(query)
        
        # Lista de padrões potencialmente inseguros
        unsafe_patterns = [
            r'(?:import|from).*(?:os|sys|subprocess|exec|eval)',
            r'__import__\(',
            r'open\(.+?,.*?[\'"]w[\'"]',
            r'exec\(',
            r'eval\(',
            r'subprocess',
            r'sys\.',
            r'getattr\(',
            r'setattr\(',
            r'globals\(\)',
            r'locals\(\)',
        ]
        
        # Removemos ou substituímos padrões inseguros
        import re
        for pattern in unsafe_patterns:
            sanitized_query = re.sub(pattern, "[REMOVIDO]", sanitized_query, flags=re.IGNORECASE)
        
        return sanitized_query