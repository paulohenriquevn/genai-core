import os
import json
import logging
import time
from typing import Dict, Any, Optional, Union
from enum import Enum

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("llm_integration")

class ModelType(Enum):
    """Tipos de modelos de IA suportados"""
    OPENAI = "openai"
    HUGGINGFACE = "huggingface"
    ANTHROPIC = "anthropic"
    LOCAL = "local"
    MOCK = "mock"  # Para testes sem IA real

class LLMIntegration:
    """
    Classe para integração com modelos de linguagem.
    
    Esta classe fornece uma interface comum para interagir com diferentes
    provedores de modelos de linguagem, abstraindo os detalhes específicos
    de cada API.
    """
    
    def __init__(
        self,
        model_type: Union[ModelType, str],
        model_name: Optional[str] = None,
        api_key: Optional[str] = None,
        api_endpoint: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Inicializa a integração com o modelo de linguagem.
        
        Args:
            model_type: Tipo do modelo (openai, huggingface, anthropic, local, mock)
            model_name: Nome específico do modelo
            api_key: Chave de API para o provedor
            api_endpoint: Endpoint da API (opcional)
            config: Configurações adicionais
        """
        # Converte string para enum se necessário
        if isinstance(model_type, str):
            model_type = ModelType(model_type.lower())
        
        self.model_type = model_type
        self.model_name = model_name
        self.api_key = api_key or os.environ.get(f"{model_type.value.upper()}_API_KEY")
        self.api_endpoint = api_endpoint
        self.config = config or {}
        
        # Mantém estado do cliente do modelo
        self.client = None
        
        # Inicializa a conexão com o modelo
        self._initialize_model()
        
        logger.info(f"Integração com modelo {model_type.value} ({model_name}) inicializada")
    
    def _initialize_model(self):
        """Inicializa a conexão com o modelo com base no tipo"""
        try:
            if self.model_type == ModelType.OPENAI:
                self._initialize_openai()
            elif self.model_type == ModelType.HUGGINGFACE:
                self._initialize_huggingface()
            elif self.model_type == ModelType.ANTHROPIC:
                self._initialize_anthropic()
            elif self.model_type == ModelType.LOCAL:
                self._initialize_local()
            elif self.model_type == ModelType.MOCK:
                # Nada a fazer para o modelo mock
                pass
            else:
                raise ValueError(f"Tipo de modelo não suportado: {self.model_type}")
        except Exception as e:
            logger.error(f"Erro ao inicializar modelo {self.model_type.value}: {str(e)}")
            # Usa o modelo mock como fallback em caso de erro
            self.model_type = ModelType.MOCK
    
    def _initialize_openai(self):
        """Inicializa a conexão com a API da OpenAI"""
        try:
            import openai
            
            if self.api_key:
                openai.api_key = self.api_key
            
            if self.api_endpoint:
                openai.api_base = self.api_endpoint
            
            # Verifica se o modelo especificado existe
            if not self.model_name:
                self.model_name = "gpt-3.5-turbo"  # Modelo padrão
            
            # Define o cliente
            self.client = openai
            
            logger.info(f"Conexão com OpenAI inicializada (modelo: {self.model_name})")
        except ImportError:
            logger.error("Módulo OpenAI não encontrado. Instale com: pip install openai")
            self.model_type = ModelType.MOCK
        except Exception as e:
            logger.error(f"Erro ao inicializar OpenAI: {str(e)}")
            self.model_type = ModelType.MOCK
    
    def _initialize_huggingface(self):
        """Inicializa a conexão com a API da Hugging Face"""
        try:
            from huggingface_hub import InferenceClient
            
            if not self.model_name:
                self.model_name = "codellama/CodeLlama-34b-Instruct-hf"  # Modelo padrão
            
            # Inicializa o cliente
            self.client = InferenceClient(
                model=self.model_name,
                token=self.api_key
            )
            
            logger.info(f"Conexão com Hugging Face inicializada (modelo: {self.model_name})")
        except ImportError:
            logger.error("Módulo huggingface_hub não encontrado. Instale com: pip install huggingface_hub")
            self.model_type = ModelType.MOCK
        except Exception as e:
            logger.error(f"Erro ao inicializar Hugging Face: {str(e)}")
            self.model_type = ModelType.MOCK
    
    def _initialize_anthropic(self):
        """Inicializa a conexão com a API da Anthropic"""
        try:
            import anthropic
            
            if not self.model_name:
                self.model_name = "claude-3-opus-20240229"  # Modelo padrão
            
            # Inicializa o cliente
            self.client = anthropic.Anthropic(api_key=self.api_key)
            
            logger.info(f"Conexão com Anthropic inicializada (modelo: {self.model_name})")
        except ImportError:
            logger.error("Módulo anthropic não encontrado. Instale com: pip install anthropic")
            self.model_type = ModelType.MOCK
        except Exception as e:
            logger.error(f"Erro ao inicializar Anthropic: {str(e)}")
            self.model_type = ModelType.MOCK
    
    def _initialize_local(self):
        """Inicializa um modelo local"""
        try:
            # Tenta usar LangChain para carregar modelos locais
            from langchain.llms import LlamaCpp, GPT4All
            
            if not self.model_name:
                # Procura por modelos locais comuns
                model_paths = [
                    "models/llama-2-7b-chat.ggmlv3.q4_0.bin",
                    "models/mistral-7b-instruct-v0.1.Q4_0.gguf",
                    "models/gpt4all-j-v1.3-groovy.bin"
                ]
                
                for path in model_paths:
                    if os.path.exists(path):
                        self.model_name = path
                        break
            
            if not self.model_name or not os.path.exists(self.model_name):
                logger.error("Modelo local não encontrado")
                self.model_type = ModelType.MOCK
                return
            
            # Carrega o modelo apropriado com base na extensão
            if self.model_name.endswith((".bin", ".gguf", ".ggmlv3")):
                self.client = LlamaCpp(
                    model_path=self.model_name,
                    temperature=0.2,
                    max_tokens=2000,
                    n_ctx=2048,
                    top_p=0.95
                )
            else:
                self.client = GPT4All(
                    model=self.model_name,
                    temp=0.2,
                    max_tokens=2000
                )
            
            logger.info(f"Modelo local inicializado: {self.model_name}")
        except ImportError:
            logger.error("Módulos LangChain, llama-cpp-python ou gpt4all não encontrados")
            self.model_type = ModelType.MOCK
        except Exception as e:
            logger.error(f"Erro ao inicializar modelo local: {str(e)}")
            self.model_type = ModelType.MOCK
    
    def generate_code(self, prompt: str) -> str:
        """
        Gera código Python/SQL com base no prompt fornecido.
        
        Args:
            prompt: Prompt para o modelo de linguagem
            
        Returns:
            str: Código Python/SQL gerado
        """
        try:
            # Chama a API apropriada baseada no tipo de modelo
            if self.model_type == ModelType.OPENAI:
                return self._generate_openai(prompt)
            elif self.model_type == ModelType.HUGGINGFACE:
                return self._generate_huggingface(prompt)
            elif self.model_type == ModelType.ANTHROPIC:
                return self._generate_anthropic(prompt)
            elif self.model_type == ModelType.LOCAL:
                return self._generate_local(prompt)
            else:
                raise ValueError(f"Tipo de modelo não suportado: {self.model_type}")
        except Exception as e:
            logger.error(f"Erro na geração de código: {str(e)}")
            # Em caso de erro, retorna um código básico como fallback
            return self._generate_fallback(prompt)
    
    def _generate_openai(self, prompt: str) -> str:
        """Gera código usando a API da OpenAI"""
        # Configura o sistema para focar em geração de código Python/SQL
        system_message = """Você é um assistente especializado em gerar código Python para análise de dados.
        Sempre use 'execute_sql_query' para executar consultas SQL.
        Defina sempre um resultado com a estrutura: {"type": tipo, "value": valor}
        onde tipo é um dos seguintes: "string", "number", "dataframe", ou "chart".
        
        Para visualizações, use o formato ApexCharts para criar gráficos interativos:
        result = {
            "type": "chart",
            "value": {
                "format": "apex",
                "config": {
                    "chart": {"type": "bar"},  # ou "line", "pie", "scatter", "area", "radar", etc.
                    "series": [...],           # dados para o gráfico
                    "xaxis": {"categories": [...]},
                    "title": {"text": "Título do Gráfico"}
                }
            }
        }
        
        Quando o usuário pedir visualizações ou gráficos, SEMPRE use o formato ApexCharts acima.
        Exemplos de prompt que pedem visualização: "Visualize os dados", "Mostre um gráfico", "Crie uma visualização", etc.
        
        Forneça apenas o código Python, sem explicações adicionais."""
        
        try:
            # Chama a API
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=1500
            )
            
            # Extrai o código da resposta
            code = response.choices[0].message.content.strip()
            
            # Limpa o código (remove markdown e explicações)
            code = self._clean_code(code)
            
            return code
        except Exception as e:
            logger.error(f"Erro na API OpenAI: {str(e)}")
            raise
    
    def _generate_huggingface(self, prompt: str) -> str:
        """Gera código usando a API da Hugging Face"""
        # Prepara o prompt com instruções claras
        enhanced_prompt = f"""Gere código Python para análise de dados com as seguintes características:
        - Use 'execute_sql_query' para consultas SQL
        - Defina um resultado com a estrutura: {{"type": tipo, "value": valor}}
        - Tipos possíveis: "string", "number", "dataframe", ou "chart"
        - Apenas o código Python, sem explicações
        
        Para visualizações, use o formato ApexCharts para criar gráficos interativos:
        result = {{
            "type": "chart",
            "value": {{
                "format": "apex",
                "config": {{
                    "chart": {{"type": "bar"}},  # ou "line", "pie", "scatter", "area", "radar", etc.
                    "series": [...],           # dados para o gráfico
                    "xaxis": {{"categories": [...]}},
                    "title": {{"text": "Título do Gráfico"}}
                }}
            }}
        }}
        
        Quando o usuário pedir visualizações ou gráficos, SEMPRE use o formato ApexCharts acima.
        
        CONSULTA:
        {prompt}
        
        CÓDIGO PYTHON:
        ```python
        """
        
        try:
            # Chama a API
            response = self.client.text_generation(
                prompt=enhanced_prompt,
                max_new_tokens=1500,
                temperature=0.2,
                top_p=0.95,
                repetition_penalty=1.2
            )
            
            # Extrai e limpa o código
            code = response.strip()
            if "```" in code:
                code = code.split("```")[0]
            
            code = self._clean_code(code)
            
            return code
        except Exception as e:
            logger.error(f"Erro na API Hugging Face: {str(e)}")
            raise
    
    def _generate_anthropic(self, prompt: str) -> str:
        """Gera código usando a API da Anthropic"""
        # Configura o sistema para focar em geração de código Python/SQL
        system_message = """Você é um assistente especializado em gerar código Python para análise de dados.
        Sempre use 'execute_sql_query' para executar consultas SQL.
        Defina sempre um resultado com a estrutura: {"type": tipo, "value": valor}
        onde tipo é um dos seguintes: "string", "number", "dataframe", ou "chart".
        
        Para visualizações, use o formato ApexCharts para criar gráficos interativos:
        result = {
            "type": "chart",
            "value": {
                "format": "apex",
                "config": {
                    "chart": {"type": "bar"},  # ou "line", "pie", "scatter", "area", "radar", etc.
                    "series": [...],           # dados para o gráfico
                    "xaxis": {"categories": [...]},
                    "title": {"text": "Título do Gráfico"}
                }
            }
        }
        
        Quando o usuário pedir visualizações ou gráficos, SEMPRE use o formato ApexCharts acima.
        Exemplos de prompt que pedem visualização: "Visualize os dados", "Mostre um gráfico", "Crie uma visualização", etc.
        
        Forneça apenas o código Python, sem explicações adicionais."""
        
        try:
            # Chama a API
            response = self.client.messages.create(
                model=self.model_name,
                system=system_message,
                messages=[
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=1500
            )
            
            # Extrai o código da resposta
            code = response.content[0].text
            
            # Limpa o código (remove markdown e explicações)
            code = self._clean_code(code)
            
            return code
        except Exception as e:
            logger.error(f"Erro na API Anthropic: {str(e)}")
            raise
    
    def _generate_local(self, prompt: str) -> str:
        """Gera código usando um modelo local"""
        # Prepara o prompt com instruções claras
        enhanced_prompt = f"""Gere código Python para análise de dados com as seguintes características:
        - Use 'execute_sql_query' para consultas SQL
        - Defina um resultado com a estrutura: {{"type": tipo, "value": valor}}
        - Tipos possíveis: "string", "number", "dataframe", ou "chart"
        - Apenas o código Python, sem explicações
        
        Para visualizações, use o formato ApexCharts para criar gráficos interativos:
        result = {{
            "type": "chart",
            "value": {{
                "format": "apex",
                "config": {{
                    "chart": {{"type": "bar"}},  # ou "line", "pie", "scatter", "area", "radar", etc.
                    "series": [...],           # dados para o gráfico
                    "xaxis": {{"categories": [...]}},
                    "title": {{"text": "Título do Gráfico"}}
                }}
            }}
        }}
        
        Quando o usuário pedir visualizações ou gráficos, SEMPRE use o formato ApexCharts acima.
        
        CONSULTA:
        {prompt}
        
        CÓDIGO PYTHON:
        """
        
        try:
            # Gera o código
            response = self.client.generate(enhanced_prompt)
            
            # Limpa o código
            code = self._clean_code(response)
            
            return code
        except Exception as e:
            logger.error(f"Erro no modelo local: {str(e)}")
            raise
    
    def _generate_fallback(self, prompt: str) -> str:
        """Gera um código fallback básico quando tudo falha"""
        # Verifica se o prompt parece estar pedindo uma visualização
        visualization_keywords = ["gráfico", "chart", "plot", "visualização", "visualize", 
                                  "mostr", "exib", "gere uma visualização", "crie um gráfico"]
        
        is_viz_request = any(keyword in prompt.lower() for keyword in visualization_keywords)
        
        if is_viz_request:
            # Fallback para solicitação de visualização (usando ApexCharts)
            return """
import pandas as pd

# Consulta simples para fallback de visualização
df_result = execute_sql_query('''
    SELECT * FROM vendas
    LIMIT 10
''')

# Prepara dados para o gráfico (usando as primeiras duas colunas numéricas)
numeric_cols = df_result.select_dtypes(include=['number']).columns.tolist()
if len(numeric_cols) >= 1:
    y_column = numeric_cols[0]
    
    # Define um resultado com gráfico ApexCharts
    result = {
        "type": "chart",
        "value": {
            "format": "apex",
            "config": {
                "chart": {"type": "bar"},
                "series": [{"name": y_column, "data": df_result[y_column].tolist()}],
                "xaxis": {"categories": [str(x) for x in range(len(df_result))]},
                "title": {"text": "Visualização dos Dados"}
            }
        }
    }
else:
    # Fallback caso não haja colunas numéricas
    result = {
        "type": "string",
        "value": f"Não foi possível gerar visualização. Dados disponíveis: {len(df_result)} registros."
    }
"""
        else:
            # Fallback padrão para outros tipos de consultas
            return """
import pandas as pd

# Consulta simples para fallback
df_result = execute_sql_query('''
    SELECT * FROM vendas
    LIMIT 5
''')

# Define um resultado básico
result = {
    "type": "string",
    "value": f"Consulta retornou {len(df_result)} registros. Colunas: {', '.join(df_result.columns)}"
}
"""
    
    def _clean_code(self, code: str) -> str:
        """
        Limpa o código, removendo marcações e extraindo apenas o bloco de código Python.
        
        Args:
            code: Código bruto da resposta do modelo
            
        Returns:
            str: Código Python limpo
        """
        # Remove marcações de bloco de código Markdown
        if "```python" in code:
            parts = code.split("```python")
            if len(parts) > 1:
                code = parts[1]
        
        if "```" in code:
            code = code.split("```")[0]
        
        # Remove outros marcadores Markdown comuns
        code = code.replace("```", "").strip()
        
        # Remove explicações antes ou depois do código
        if "import" in code:
            # Encontra o primeiro import
            import_index = code.find("import")
            code = code[import_index:]
        
        # Remove comentários que são explicações
        lines = code.split("\n")
        cleaned_lines = []
        for line in lines:
            # Mantém comentários dentro do código, mas remove explicações longas
            if line.strip().startswith("#") and len(line) > 80:
                continue
            cleaned_lines.append(line)
        
        code = "\n".join(cleaned_lines)
        
        return code.strip()

# Função de utilitário para criar uma instância de integração LLM com base em configurações
def create_llm_integration(config_path: Optional[str] = None) -> LLMIntegration:
    """
    Cria uma instância de integração LLM com base em um arquivo de configuração
    ou variáveis de ambiente.
    
    Args:
        config_path: Caminho para o arquivo de configuração JSON
        
    Returns:
        LLMIntegration: Instância configurada
    """
    # Configuração padrão
    default_config = {
        "model_type": "mock",
        "model_name": None,
        "api_key": None,
        "api_endpoint": None,
        "config": {}
    }
    
    # Tenta carregar configurações do arquivo
    config = default_config.copy()
    if config_path and os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                file_config = json.load(f)
                config.update(file_config)
        except Exception as e:
            logger.error(f"Erro ao carregar configuração do arquivo {config_path}: {str(e)}")
    
    # Verifica variáveis de ambiente
    env_model_type = os.environ.get("LLM_MODEL_TYPE")
    env_model_name = os.environ.get("LLM_MODEL_NAME")
    env_api_key = os.environ.get("LLM_API_KEY")
    
    if env_model_type:
        config["model_type"] = env_model_type
    if env_model_name:
        config["model_name"] = env_model_name
    if env_api_key:
        config["api_key"] = env_api_key
    
    # Cria e retorna a instância
    return LLMIntegration(
        model_type=config["model_type"],
        model_name=config["model_name"],
        api_key=config["api_key"],
        api_endpoint=config["api_endpoint"],
        config=config["config"]
    )


# Classe para integrar o LLM com o motor de consulta
class LLMQueryGenerator:
    """
    Classe para integrar modelos de linguagem com o motor de consulta.
    
    Esta classe fornece métodos para gerar código Python/SQL a partir de
    consultas em linguagem natural usando modelos de IA.
    """
    
    def __init__(
        self,
        llm_integration: Optional[LLMIntegration] = None,
        config_path: Optional[str] = None
    ):
        """
        Inicializa o gerador de consultas com um modelo de IA.
        
        Args:
            llm_integration: Instância de integração LLM
            config_path: Caminho para arquivo de configuração
        """
        # Usa a instância fornecida ou cria uma nova
        self.llm = llm_integration or create_llm_integration(config_path)
        
        # Estatísticas de uso
        self.query_count = 0
        self.total_generation_time = 0
        self.error_count = 0
        
        logger.info(f"Gerador de consultas inicializado com modelo {self.llm.model_type.value}")
    
    def generate_code(self, prompt: str) -> str:
        """
        Gera código Python/SQL a partir de um prompt.
        
        Args:
            prompt: Prompt para o modelo de IA
            
        Returns:
            str: Código Python/SQL gerado
        """
        self.query_count += 1
        
        try:
            # Registra o tempo de início
            start_time = time.time()
            
            # Gera o código
            code = self.llm.generate_code(prompt)
            
            # Atualiza estatísticas
            generation_time = time.time() - start_time
            self.total_generation_time += generation_time
            
            logger.info(f"Código gerado em {generation_time:.2f}s")
            
            return code
        except Exception as e:
            # Registra o erro
            self.error_count += 1
            logger.error(f"Erro ao gerar código: {str(e)}")
            
            # Retorna um código fallback
            return self.llm._generate_fallback(prompt)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Retorna estatísticas de uso.
        
        Returns:
            Dict[str, Any]: Estatísticas de uso
        """
        avg_time = self.total_generation_time / max(1, self.query_count)
        
        return {
            "query_count": self.query_count,
            "error_count": self.error_count,
            "avg_generation_time": avg_time,
            "error_rate": (self.error_count / max(1, self.query_count)) * 100,
            "model_type": self.llm.model_type.value,
            "model_name": self.llm.model_name
        }