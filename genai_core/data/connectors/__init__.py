"""
Conectores para diferentes fontes de dados
"""

from genai_core.data.connectors.csv_connector import CSVConnector
from genai_core.data.connectors.duckdb_connector import DuckDBConnector
from genai_core.data.connectors.excel_connector import ExcelConnector
from genai_core.data.connectors.postgres_connector import PostgresConnector

__all__ = [
    "CSVConnector",
    "DuckDBConnector", 
    "ExcelConnector",
    "PostgresConnector"
]