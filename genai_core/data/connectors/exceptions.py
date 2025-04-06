class ConfigurationException(Exception):
    """Exception raised for errors in the configuration."""
    pass

class DataConnectionException(Exception):
    """Exception raised for errors in connecting to data sources."""
    pass

class DataReadException(Exception):
    """Exception raised for errors in reading data."""
    pass

class SemanticLayerException(Exception):
    """Exception raised for errors in the semantic layer."""
    pass