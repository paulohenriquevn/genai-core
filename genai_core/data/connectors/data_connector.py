import pandas as pd
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class DataConnector:
    """Base interface for all data connectors."""
    
    def connect(self) -> None:
        """
        Establish connection to the data source.
        
        Raises:
            DataConnectionException: If connection fails.
        """
        pass
    
    def read_data(self, query: Optional[str] = None) -> pd.DataFrame:
        """
        Read data from the source according to the specified query.
        
        Args:
            query: Query to filter/transform data.
            
        Returns:
            pd.DataFrame: DataFrame with the read data.
            
        Raises:
            DataReadException: If reading fails.
        """
        pass
    
    def close(self) -> None:
        """
        Close the connection to the data source.
        
        Raises:
            DataConnectionException: If closing the connection fails.
        """
        pass
    
    def is_connected(self) -> bool:
        """
        Check if the connection is active.
        
        Returns:
            bool: True if connected, False otherwise.
        """
        pass
    
    def get_schema(self) -> pd.DataFrame:
        """
        Return the schema of the data source.
        
        Returns:
            pd.DataFrame: DataFrame with schema information.
        """
        pass
        
    def apply_semantic_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformations defined in the semantic schema.
        
        Args:
            df: Input DataFrame.
            
        Returns:
            pd.DataFrame: Transformed DataFrame.
        """
        # This will be implemented by subclasses if they support semantic transformations
        return df