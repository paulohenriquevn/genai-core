# Export main classes for easier imports
from genai_core.data.connectors.data_connector import DataConnector
from genai_core.data.connectors.data_connector_factory import DataConnectorFactory
from genai_core.data.connectors.datasource_config import DataSourceConfig
from genai_core.data.connectors.exceptions import (
    ConfigurationException, 
    DataConnectionException, 
    DataReadException,
    SemanticLayerException
)

# Export connectors
from genai_core.data.connectors.duckdb_connector import DuckDBConnector
from genai_core.data.connectors.postgres_connector import PostgresConnector

# Export test utilities
from genai_core.data.connectors.test_data_provider import add_test_data_source