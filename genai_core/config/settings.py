"""
Módulo para gerenciamento de configurações e variáveis de ambiente.
Centraliza as configurações de todo o sistema.
"""

import os
import json
import logging
from typing import Dict, Any, Optional, List, Union
import uuid

# Configuração de logging
logger = logging.getLogger(__name__)


class Settings:
    """
    Classe para gerenciar configurações do sistema.
    Carrega configurações de arquivos e variáveis de ambiente.
    """
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Inicializa as configurações do sistema.
        
        Args:
            config_file: Caminho para arquivo de configuração (opcional)
        """
        # Configurações padrão
        self._configs = {
            # Configurações gerais
            "debug": False,
            "log_level": "info",
            
            # Configurações do LLM
            "llm_type": "mock",  # openai, anthropic, huggingface, mock
            "llm_model": None,
            "llm_api_key": None,
            
            # Configurações de SQL
            "sql_dialect": "duckdb",
            "max_sql_query_length": 10000,
            "use_specialized_sql_model": False,
            "sqlcoder_api_key": None,
            "sqlcoder_model_version": "defog/sqlcoder-7b-2",
            
            # Fontes de dados
            "data_sources": {}
        }
        
        # Carrega configurações do arquivo
        if config_file and os.path.exists(config_file):
            self._load_from_file(config_file)
        
        # Carrega configurações das variáveis de ambiente
        self._load_from_env()
        
        logger.info("Configurações carregadas com sucesso")
    
    def _load_from_file(self, config_file: str) -> None:
        """
        Carrega configurações a partir de um arquivo JSON.
        
        Args:
            config_file: Caminho para o arquivo de configuração
        """
        try:
            with open(config_file, 'r') as f:
                config_data = json.load(f)
                
            # Atualiza as configurações com os valores do arquivo
            self._configs.update(config_data)
            
            logger.info(f"Configurações carregadas do arquivo: {config_file}")
            
        except Exception as e:
            logger.error(f"Erro ao carregar configurações do arquivo: {str(e)}")
    
    def _load_from_env(self) -> None:
        """Carrega configurações a partir de variáveis de ambiente."""
        # Mapeamento de variáveis de ambiente para configurações
        env_mapping = {
            "GENAI_DEBUG": ("debug", lambda x: x.lower() in ['true', '1', 'yes']),
            "GENAI_LOG_LEVEL": ("log_level", str),
            
            # LLM
            "GENAI_LLM_TYPE": ("llm_type", str),
            "GENAI_LLM_MODEL": ("llm_model", str),
            "GENAI_LLM_API_KEY": ("llm_api_key", str),
            "OPENAI_API_KEY": ("llm_api_key", str),  # Fallback para OpenAI
            
            # SQL
            "GENAI_SQL_DIALECT": ("sql_dialect", str),
            "GENAI_MAX_SQL_QUERY_LENGTH": ("max_sql_query_length", int),
            "GENAI_USE_SPECIALIZED_SQL_MODEL": ("use_specialized_sql_model", lambda x: x.lower() in ['true', '1', 'yes']),
            "DEFOG_API_KEY": ("sqlcoder_api_key", str),
            "GENAI_SQLCODER_MODEL_VERSION": ("sqlcoder_model_version", str)
        }
        
        # Processa cada variável de ambiente
        for env_var, (config_key, converter) in env_mapping.items():
            if env_var in os.environ:
                try:
                    value = os.environ[env_var]
                    converted_value = converter(value)
                    self._configs[config_key] = converted_value
                    
                    # Não loga chaves de API para manter segurança
                    if "api_key" in config_key:
                        logger.info(f"Variável de ambiente carregada: {env_var} = ******")
                    else:
                        logger.info(f"Variável de ambiente carregada: {env_var} = {converted_value}")
                        
                except Exception as e:
                    logger.error(f"Erro ao carregar variável de ambiente {env_var}: {str(e)}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Obtém uma configuração pelo nome.
        
        Args:
            key: Nome da configuração
            default: Valor padrão se a configuração não existir
            
        Returns:
            Valor da configuração ou o valor padrão
        """
        return self._configs.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """
        Define uma configuração.
        
        Args:
            key: Nome da configuração
            value: Valor da configuração
        """
        self._configs[key] = value
        logger.info(f"Configuração definida: {key} = {value if 'api_key' not in key else '******'}")
    
    def get_all_data_sources(self) -> Dict[str, Dict[str, Any]]:
        """
        Obtém todas as fontes de dados configuradas.
        
        Returns:
            Dicionário com todas as fontes de dados
        """
        return self._configs.get("data_sources", {})
    
    def get_data_source_config(self, data_source_id: str) -> Dict[str, Any]:
        """
        Obtém a configuração de uma fonte de dados específica.
        
        Args:
            data_source_id: ID da fonte de dados
            
        Returns:
            Configuração da fonte de dados
            
        Raises:
            ValueError: Se a fonte de dados não existir
        """
        data_sources = self._configs.get("data_sources", {})
        
        if data_source_id not in data_sources:
            raise ValueError(f"Fonte de dados não encontrada: {data_source_id}")
            
        return data_sources[data_source_id]
    
    def add_data_source(self, data_source_config: Dict[str, Any]) -> str:
        """
        Adiciona uma nova fonte de dados.
        
        Args:
            data_source_config: Configuração da fonte de dados
            
        Returns:
            ID da fonte de dados adicionada
        """
        # Gera um ID se não for fornecido
        data_source_id = data_source_config.get("id") or str(uuid.uuid4())
        
        # Garante que o ID é único
        data_sources = self._configs.get("data_sources", {})
        if data_source_id in data_sources:
            logger.warning(f"Fonte de dados {data_source_id} já existe, será sobrescrita")
        
        # Adiciona a fonte de dados
        data_sources[data_source_id] = data_source_config
        self._configs["data_sources"] = data_sources
        
        logger.info(f"Fonte de dados adicionada: {data_source_id}")
        
        return data_source_id
    
    def remove_data_source(self, data_source_id: str) -> None:
        """
        Remove uma fonte de dados.
        
        Args:
            data_source_id: ID da fonte de dados
            
        Raises:
            ValueError: Se a fonte de dados não existir
        """
        data_sources = self._configs.get("data_sources", {})
        
        if data_source_id not in data_sources:
            raise ValueError(f"Fonte de dados não encontrada: {data_source_id}")
            
        # Remove a fonte de dados
        del data_sources[data_source_id]
        
        logger.info(f"Fonte de dados removida: {data_source_id}")
    
    def save_to_file(self, file_path: str) -> None:
        """
        Salva as configurações em um arquivo JSON.
        
        Args:
            file_path: Caminho para o arquivo
        """
        try:
            # Cria o diretório se não existir
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Salva as configurações
            with open(file_path, 'w') as f:
                json.dump(self._configs, f, indent=2)
                
            logger.info(f"Configurações salvas em: {file_path}")
            
        except Exception as e:
            logger.error(f"Erro ao salvar configurações: {str(e)}")
            raise