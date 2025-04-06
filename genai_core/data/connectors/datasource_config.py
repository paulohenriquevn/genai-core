from typing import Dict, Any, Optional

class DataSourceConfig:
    """
    Configuration for a data source.
    
    Attributes:
        id: Unique identifier for the data source
        type: Type of data source (e.g., 'postgres', 'csv', 'duckdb')
        params: Parameters specific to the data source type
    """
    
    def __init__(self, source_id: str, source_type: str, params: Dict[str, Any] = None):
        """
        Initialize a data source configuration.
        
        Args:
            source_id: Unique identifier for the data source
            source_type: Type of data source
            params: Parameters specific to the data source type
        """
        self.source_id = source_id
        self.source_type = source_type
        self.params = params or {}
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'DataSourceConfig':
        """
        Create a DataSourceConfig from a dictionary.
        
        Args:
            config_dict: Dictionary with configuration
            
        Returns:
            DataSourceConfig: Created configuration
            
        Raises:
            ValueError: If required fields are missing
        """
        # Extract the source ID (required)
        source_id = config_dict.get("id", config_dict.get("source_id"))
        if not source_id:
            raise ValueError("Missing 'id' or 'source_id' in datasource configuration")
        
        # Extract the source type (required)
        source_type = config_dict.get("type", config_dict.get("source_type"))
        if not source_type:
            # Try to infer from file extension if path is provided
            path = config_dict.get("path", "")
            if path and "." in path:
                import os
                extension = os.path.splitext(path)[1].lower().replace(".", "")
                if extension in ["csv", "xlsx", "xls", "parquet", "json"]:
                    source_type = extension
                else:
                    source_type = "csv"  # Default to CSV for unknown extensions
            else:
                raise ValueError("Missing 'type' or 'source_type' in datasource configuration")
        
        # Extract parameters
        params = {}
        for key, value in config_dict.items():
            if key not in ["id", "source_id", "type", "source_type"]:
                params[key] = value
        
        # Create and return the configuration
        return cls(source_id, source_type, params)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary.
        
        Returns:
            Dict[str, Any]: Dictionary representation
        """
        result = {
            "id": self.source_id,
            "type": self.source_type
        }
        
        # Add all parameters
        result.update(self.params)
        
        return result
    
    def copy(self) -> Dict[str, Any]:
        """
        Create a copy of the configuration as a dictionary.
        
        Returns:
            Dict[str, Any]: Copy of the configuration
        """
        return {
            "id": self.source_id,
            "type": self.source_type,
            **self.params
        }