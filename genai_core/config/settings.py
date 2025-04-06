"""
Módulo de configurações do GenAI Core.

Este módulo centraliza todas as configurações e variáveis sensíveis da aplicação,
utilizando variáveis de ambiente com valores de fallback.
"""

import logging
import os
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Configurações de API Keys
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
HF_API_KEY = os.getenv("HF_API_KEY", "")  # Hugging Face

# Configurações de banco de dados
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://postgres:postgres@localhost:5432/genai")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", ":memory:")

# Configurações da aplicação
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

# Configurações de cache
CACHE_ENABLED = os.getenv("CACHE_ENABLED", "True").lower() in ("true", "1", "t")
CACHE_TTL = int(os.getenv("CACHE_TTL", "3600"))  # Tempo em segundos

class Settings:
    """
    Classe para gerenciar configurações do sistema.
    Mantida para compatibilidade com código existente.
    """
    
    def __init__(self):
        """
        Inicializa as configurações com valores padrão e das variáveis de ambiente.
        """
        self.config: Dict[str, Any] = {
            'openai_api_key': OPENAI_API_KEY,
            'anthropic_api_key': ANTHROPIC_API_KEY,
            'google_api_key': GOOGLE_API_KEY,
            'hf_api_key': HF_API_KEY,
            'postgres_url': POSTGRES_URL,
            'duckdb_path': DUCKDB_PATH,
            'environment': ENVIRONMENT,
            'cache_enabled': CACHE_ENABLED,
            'cache_ttl': CACHE_TTL,
        }
        self.data_sources: Dict[str, Any] = {}
        
        # Log de inicialização (com mascaramento das chaves sensíveis)
        self._log_keys()
        logger.info("Configurações carregadas com sucesso")
    
    def _log_keys(self) -> None:
        """
        Registra no log as chaves API carregadas (mascaradas por segurança).
        """
        for key in ('openai_api_key', 'anthropic_api_key', 'google_api_key', 'hf_api_key'):
            if self.config.get(key):
                masked_key = '*' * 6 + self.config[key][-4:] if len(self.config[key]) > 4 else '*' * 6
                logger.info(f"Variável carregada: {key} = {masked_key}")
    
    def set(self, key: str, value: Any) -> None:
        """
        Define uma configuração.
        
        Args:
            key: Chave da configuração
            value: Valor da configuração
        """
        self.config[key] = value
        logger.info(f"Configuração definida: {key}")
        
    def get(self, key: str, default: Any = None) -> Any:
        """
        Obtém o valor de uma configuração.
        
        Args:
            key: Chave da configuração
            default: Valor padrão se a chave não existir
            
        Returns:
            Valor da configuração ou o padrão
        """
        return self.config.get(key, default)
        
    def add_data_source(self, source_id: str, config: Dict[str, Any]) -> None:
        """
        Adiciona configuração para uma fonte de dados.
        
        Args:
            source_id: ID da fonte de dados
            config: Configuração da fonte
        """
        self.data_sources[source_id] = config
        logger.info(f"Fonte de dados adicionada: {source_id}")
        
    def get_data_source_config(self, source_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtém configuração de uma fonte de dados.
        
        Args:
            source_id: ID da fonte de dados
            
        Returns:
            Configuração da fonte de dados
        """
        return self.data_sources.get(source_id)