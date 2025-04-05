"""
Módulo de conectores para diferentes fontes de dados.
Este módulo fornece interfaces e implementações para acessar diversos tipos de dados.
"""

# Importações relativas explícitas para evitar ambiguidade
from .csv_connector import CsvConnector
from .data_connector_factory import DataConnectorFactory
from .data_connector import DataConnector
from .datasource_config import DataSourceConfig
from .duckdb_csv_connector import DuckDBCsvConnector
from .duckdb_xls_connector import DuckDBXlsConnector
from .xls_connector import XlsConnector
from .exceptions import ConfigurationException, DataConnectionException, DataReadException
from .metadata import ColumnMetadata, DatasetMetadata
from .postgres_connector import PostgresConnector

# Definição clara das classes públicas exportadas pelo módulo
__all__ = [
    "DataConnector",       # Classe abstrata base para conectores
    "DataSourceConfig",    # Configuração de fonte de dados
    "DataConnectorFactory", # Factory para criar conectores
    "CsvConnector",        # Conector para arquivos CSV
    "PostgresConnector",   # Conector para PostgreSQL
    "DuckDBCsvConnector",  # Conector para DuckDB com CSV
    "XlsConnector",        # Conector para arquivos Excel
    "DuckDBXlsConnector",  # Conector para DuckDB com Excel
    "ConfigurationException", # Exceção para problemas de configuração
    "DataConnectionException", # Exceção para problemas de conexão
    "DataReadException",   # Exceção para problemas de leitura
    "ColumnMetadata",      # Metadados de colunas
    "DatasetMetadata"      # Metadados de datasets
]