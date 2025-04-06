import pandas as pd
import logging
from typing import Optional, Dict, Any, Union

from genai_core.data.connectors.data_connector import DataConnector
from genai_core.data.connectors.datasource_config import DataSourceConfig
from genai_core.data.connectors.exceptions import ConfigurationException, DataConnectionException, DataReadException

logger = logging.getLogger(__name__)

class PostgresConnector(DataConnector):
    """
    Connector for PostgreSQL databases.
    
    Attributes:
        config: Configuration parameters
        connection: PostgreSQL connection
        engine: SQLAlchemy engine
        schema: Database schema
        table_name: Main table name
    """
    
    def __init__(self, config: Union[Dict[str, Any], DataSourceConfig]):
        """
        Initialize the PostgreSQL connector.
        
        Args:
            config: Configuration parameters
        """
        # Convert dict to DataSourceConfig if needed
        if isinstance(config, dict):
            self.config = DataSourceConfig.from_dict(config)
        else:
            self.config = config
            
        self.connection = None
        self.engine = None
        self.schema = self.config.params.get('schema', 'public')
        self.table_name = self.config.params.get('table', self.config.params.get('table_name'))
        
        # Validate required parameters
        required_params = ['host', 'port', 'database', 'user', 'password']
        missing_params = [param for param in required_params if param not in self.config.params]
        
        if missing_params:
            msg = f"Missing required parameters for PostgreSQL connection: {', '.join(missing_params)}"
            raise ConfigurationException(msg)
        
        if not self.table_name:
            msg = "Missing 'table' or 'table_name' parameter for PostgreSQL connection"
            raise ConfigurationException(msg)
    
    def connect(self) -> None:
        """
        Establish connection with PostgreSQL.
        
        Raises:
            DataConnectionException: If connection fails
        """
        try:
            # Import SQLAlchemy
            import sqlalchemy
            from sqlalchemy import create_engine
            
            # Build connection string
            host = self.config.params['host']
            port = self.config.params['port']
            database = self.config.params['database']
            user = self.config.params['user']
            password = self.config.params['password']
            
            connection_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
            
            # Create engine
            self.engine = create_engine(connection_str)
            
            # Test connection
            self.connection = self.engine.connect()
            logger.info(f"Connected to PostgreSQL: {host}:{port}/{database}")
            
        except ImportError as e:
            msg = "SQLAlchemy not installed. Install with: pip install sqlalchemy psycopg2-binary"
            logger.error(msg)
            raise DataConnectionException(msg) from e
        except Exception as e:
            msg = f"Error connecting to PostgreSQL: {str(e)}"
            logger.error(msg)
            raise DataConnectionException(msg) from e
    
    def read_data(self, query: Optional[str] = None) -> pd.DataFrame:
        """
        Read data from PostgreSQL, optionally applying an SQL query.
        
        Args:
            query: Optional SQL query to execute
            
        Returns:
            pd.DataFrame: DataFrame with results
            
        Raises:
            DataReadException: If reading fails
        """
        if not self.is_connected():
            self.connect()
        
        try:
            # If no query is provided, select all from the main table
            if not query:
                query = f"SELECT * FROM {self.schema}.{self.table_name}"
            
            logger.info(f"Executing PostgreSQL query: {query}")
            
            # Execute the query
            result_df = pd.read_sql(query, self.connection)
            return result_df
            
        except Exception as e:
            msg = f"Error reading data from PostgreSQL: {str(e)}"
            logger.error(msg)
            raise DataReadException(msg) from e
    
    def get_schema(self) -> pd.DataFrame:
        """
        Return the schema (structure) of the data.
        
        Returns:
            pd.DataFrame: DataFrame with schema information
            
        Raises:
            DataConnectionException: If not connected
            DataReadException: If getting schema fails
        """
        if not self.is_connected():
            raise DataConnectionException("Not connected to data source. Call connect() first.")
            
        try:
            # Get schema information from information_schema
            query = f"""
            SELECT 
                column_name, 
                data_type as column_type
            FROM 
                information_schema.columns
            WHERE 
                table_schema = '{self.schema}'
                AND table_name = '{self.table_name}'
            ORDER BY 
                ordinal_position
            """
            
            return pd.read_sql(query, self.connection)
            
        except Exception as e:
            msg = f"Error getting schema from PostgreSQL: {str(e)}"
            logger.error(msg)
            raise DataReadException(msg) from e
    
    def close(self) -> None:
        """
        Close the PostgreSQL connection.
        
        Raises:
            DataConnectionException: If closing fails
        """
        if self.connection:
            try:
                self.connection.close()
                self.engine.dispose()
                self.connection = None
                self.engine = None
                logger.info("PostgreSQL connection closed")
            except Exception as e:
                msg = f"Error closing PostgreSQL connection: {str(e)}"
                logger.warning(msg)
                raise DataConnectionException(msg) from e
    
    def is_connected(self) -> bool:
        """
        Check if the connector is active.
        
        Returns:
            bool: True if connected, False otherwise
        """
        if not self.connection or not self.engine:
            return False
            
        try:
            # Check if the connection is active
            self.connection.execute("SELECT 1")
            return True
        except Exception:
            return False