"""
Módulo de configurações do GenAI Core.
"""

import logging
import os

logger = logging.getLogger(__name__)

class Settings:
    """
    Classe para gerenciar configurações do sistema.
    """
    
    def __init__(self):
        """
        Inicializa as configurações com valores padrão.
        """
        self.config = {}
        self.data_sources = {}
        
        # Carrega variáveis de ambiente
        if os.environ.get('OPENAI_API_KEY'):
            # Mascara a chave de API para segurança ao logar
            key_value = os.environ.get('OPENAI_API_KEY')
            masked_key = '*' * 6
            logger.info(f"Variável de ambiente carregada: OPENAI_API_KEY = {masked_key}")
            self.config['openai_api_key'] = key_value
            
        logger.info("Configurações carregadas com sucesso")
    
    def set(self, key, value):
        """
        Define uma configuração.
        
        Args:
            key: Chave da configuração
            value: Valor da configuração
        """
        self.config[key] = value
        logger.info(f"Configuração definida: {key} = {value}")
        
    def get(self, key, default=None):
        """
        Obtém o valor de uma configuração.
        
        Args:
            key: Chave da configuração
            default: Valor padrão se a chave não existir
            
        Returns:
            Valor da configuração ou o padrão
        """
        return self.config.get(key, default)
        
    def add_data_source(self, source_id, config):
        """
        Adiciona configuração para uma fonte de dados.
        
        Args:
            source_id: ID da fonte de dados
            config: Configuração da fonte
        """
        self.data_sources[source_id] = config
        logger.info(f"Fonte de dados adicionada: {source_id}")
        
    def get_data_source_config(self, source_id):
        """
        Obtém configuração de uma fonte de dados.
        
        Args:
            source_id: ID da fonte de dados
            
        Returns:
            Configuração da fonte de dados
        """
        return self.data_sources.get(source_id)