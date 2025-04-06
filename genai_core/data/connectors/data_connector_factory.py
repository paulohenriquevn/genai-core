import os
import logging
import importlib
from typing import Dict, Any, Type, Union

from genai_core.data.connectors.data_connector import DataConnector

logger = logging.getLogger(__name__)

class DataConnectorFactory:
    """
    Factory for creating data connectors.
    """
    # Registry of connector types to their implementation classes
    _connector_registry = {
        'postgres': ('genai_core.data.connectors.postgres_connector', 'PostgresConnector'),
        'duckdb': ('genai_core.data.connectors.duckdb_connector', 'DuckDBConnector'),
        'csv': ('genai_core.data.connectors.duckdb_connector', 'DuckDBConnector'),
        'excel': ('genai_core.data.connectors.duckdb_connector', 'DuckDBConnector'),
        'xlsx': ('genai_core.data.connectors.duckdb_connector', 'DuckDBConnector'),
        'xls': ('genai_core.data.connectors.duckdb_connector', 'DuckDBConnector'),
        'parquet': ('genai_core.data.connectors.duckdb_connector', 'DuckDBConnector'),
        'json': ('genai_core.data.connectors.duckdb_connector', 'DuckDBConnector')
    }
    
    # Cache of loaded connector classes
    _connector_classes = {}
    
    @classmethod
    def register_connector(cls, source_type: str, connector_info: Union[tuple, Type[DataConnector]]) -> None:
        """
        Register a new connector type.
        
        Args:
            source_type: Data source type identifier
            connector_info: Tuple of (module_path, class_name) or connector class
        """
        cls._connector_registry[source_type] = connector_info
        logger.info(f"Registered connector for type: {source_type}")
    
    @classmethod
    def _load_connector_class(cls, module_path: str, class_name: str) -> Type[DataConnector]:
        """
        Dynamically load a connector class.
        
        Args:
            module_path: Path to the module
            class_name: Name of the class
            
        Returns:
            Type[DataConnector]: The connector class
            
        Raises:
            ValueError: If the class cannot be loaded
        """
        cache_key = f"{module_path}.{class_name}"
        
        # Check cache first
        if cache_key in cls._connector_classes:
            return cls._connector_classes[cache_key]
        
        try:
            # Import the module
            module = importlib.import_module(module_path)
            
            # Get the class
            connector_class = getattr(module, class_name)
            
            # Cache for future use
            cls._connector_classes[cache_key] = connector_class
            
            return connector_class
            
        except (ImportError, AttributeError) as e:
            logger.error(f"Error loading connector class {class_name} from {module_path}: {str(e)}")
            raise ValueError(f"Failed to load connector class: {str(e)}")
    
    @classmethod
    def create_connector(cls, config: Dict[str, Any]) -> DataConnector:
        """
        Create a connector instance.
        
        Args:
            config: Configuration for the connector
            
        Returns:
            DataConnector: The created connector
            
        Raises:
            ValueError: If the connector type is not supported
        """
        # Get connector type from config
        source_type = config.get("type", config.get("source_type"))
        
        if not source_type:
            if "path" in config:
                # Try to infer type from file extension
                path = config["path"]
                ext = os.path.splitext(path)[1].lower().replace(".", "")
                if ext in ["csv", "xlsx", "xls", "parquet", "json"]:
                    source_type = ext
                else:
                    source_type = "csv"  # Default to CSV for unknown extensions
            else:
                raise ValueError("Missing source_type in connector configuration")
        
        # Check if connector type is supported
        if source_type not in cls._connector_registry:
            raise ValueError(f"Unsupported connector type: {source_type}")
        
        # Get connector info
        connector_info = cls._connector_registry[source_type]
        
        # Create connector instance
        if isinstance(connector_info, tuple) and len(connector_info) == 2:
            # It's a module and class name
            module_path, class_name = connector_info
            connector_class = cls._load_connector_class(module_path, class_name)
            return connector_class(config)
        elif isinstance(connector_info, type) and issubclass(connector_info, DataConnector):
            # It's already a class
            return connector_info(config)
        else:
            raise ValueError(f"Invalid connector specification for {source_type}")