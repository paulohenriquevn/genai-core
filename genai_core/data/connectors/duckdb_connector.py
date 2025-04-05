# -*- coding: utf-8 -*-
"""
Módulo para conectar com DuckDB para processamento otimizado de dados.
"""

import os
import logging
import pandas as pd
from typing import Dict, Any, Optional, List, Union

# Configuração de logging
logger = logging.getLogger(__name__)


class DuckDBConnector:
    """
    Conector para DuckDB.
    Permite processar arquivos locais (CSV, Excel) através do DuckDB para consultas SQL otimizadas.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Inicializa o conector DuckDB.
        
        Args:
            config: Configuração do conector
                - path: Caminho para o arquivo ou diretório
                - table_name: Nome da tabela (opcional)
                - file_type: Tipo de arquivo ('csv', 'xls', 'xlsx', etc.)
                - options: Opções para o DuckDB (opcional)
        """
        self.config = config
        self.connection = None
        self.table_name = config.get("table_name") or f"data_{hash(config.get('path', ''))}"
        self._connected = False
        self.file_paths = []
        
        # Determina o tipo de arquivo
        self.file_type = config.get("file_type", "").lower()
        if not self.file_type:
            # Infere o tipo a partir do path
            path = config.get("path", "")
            if path.endswith(".csv"):
                self.file_type = "csv"
            elif path.endswith((".xls", ".xlsx")):
                self.file_type = "excel"
            else:
                self.file_type = "csv"  # Padrão
        
        # Valida a configuração
        self._validate_config()
        
        logger.info(f"DuckDBConnector inicializado com path: {self.config.get('path')}")
    
    def _validate_config(self) -> None:
        """Valida a configuração do conector."""
        # Verifica se o path foi fornecido
        if "path" not in self.config:
            raise ValueError("O path do arquivo é obrigatório")
            
        # Verifica se o arquivo existe
        path = self.config["path"]
        if not os.path.exists(path):
            raise FileNotFoundError(f"Arquivo ou diretório não encontrado: {path}")
    
    def connect(self) -> None:
        """
        Estabelece conexão com o DuckDB e registra os arquivos como tabelas.
        """
        try:
            # Importa o DuckDB
            import duckdb
            
            # Inicializa a conexão em memória
            self.connection = duckdb.connect(database=':memory:')
            
            # Carrega os arquivos
            path = self.config["path"]
            
            # Verifica se é um diretório ou arquivo
            if os.path.isdir(path):
                self._load_directory(path)
            else:
                self._load_file(path)
                
            self._connected = True
            logger.info(f"Conexão DuckDB estabelecida com sucesso: {path}")
            
        except ImportError:
            logger.error("DuckDB não está disponível. Instale com: pip install duckdb")
            raise
        except Exception as e:
            logger.error(f"Erro ao conectar com DuckDB: {str(e)}")
            raise
    
    def _load_file(self, file_path: str) -> None:
        """
        Carrega um único arquivo no DuckDB.
        
        Args:
            file_path: Caminho para o arquivo
        """
        try:
            # Guarda o caminho do arquivo
            self.file_paths = [file_path]
            
            # Carrega o arquivo baseado no tipo
            if self.file_type == "csv":
                self._load_csv_file(file_path)
            elif self.file_type in ["excel", "xls", "xlsx"]:
                self._load_excel_file(file_path)
            else:
                raise ValueError(f"Tipo de arquivo não suportado: {self.file_type}")
                
        except Exception as e:
            logger.error(f"Erro ao carregar arquivo {file_path}: {str(e)}")
            raise
    
    def _load_directory(self, dir_path: str) -> None:
        """
        Carrega todos os arquivos de um diretório no DuckDB.
        
        Args:
            dir_path: Caminho para o diretório
        """
        import glob
        
        try:
            # Determina o padrão de busca baseado no tipo de arquivo
            if self.file_type == "csv":
                pattern = "*.csv"
            elif self.file_type in ["excel", "xls", "xlsx"]:
                pattern = "*.xls*"  # Corresponde a .xls e .xlsx
            else:
                pattern = "*.*"
                
            # Busca por arquivos no diretório
            search_pattern = os.path.join(dir_path, self.config.get("pattern", pattern))
            self.file_paths = glob.glob(search_pattern)
            
            if not self.file_paths:
                raise FileNotFoundError(f"Nenhum arquivo encontrado em: {dir_path} (padrão: {pattern})")
            
            # Carrega cada arquivo
            if self.file_type == "csv":
                self._load_csv_files(self.file_paths)
            elif self.file_type in ["excel", "xls", "xlsx"]:
                self._load_excel_files(self.file_paths)
            else:
                raise ValueError(f"Tipo de arquivo não suportado: {self.file_type}")
                
        except Exception as e:
            logger.error(f"Erro ao carregar diretório {dir_path}: {str(e)}")
            raise
    
    def _load_csv_file(self, file_path: str) -> None:
        """
        Carrega um arquivo CSV no DuckDB.
        
        Args:
            file_path: Caminho para o arquivo CSV
        """
        try:
            # Obtém opções para o CSV
            delimiter = self.config.get("delimiter", ",")
            header = 0 if self.config.get("header", True) else None
            
            # Carrega o CSV usando pandas
            df = pd.read_csv(file_path, delimiter=delimiter, header=header)
            
            # Registra o DataFrame no DuckDB
            self.connection.register(self.table_name, df)
            
            # Cria uma visualização para compatibilidade
            self.connection.execute(f"CREATE OR REPLACE VIEW dados AS SELECT * FROM {self.table_name}")
            
            logger.info(f"Arquivo CSV carregado no DuckDB: {file_path}")
        except Exception as e:
            logger.error(f"Erro ao carregar CSV {file_path}: {str(e)}")
            raise
    
    def _load_csv_files(self, file_paths: List[str]) -> None:
        """
        Carrega múltiplos arquivos CSV no DuckDB.
        
        Args:
            file_paths: Lista de caminhos para arquivos CSV
        """
        if self.config.get("create_combined_view", True):
            # Carrega o primeiro arquivo para criar a tabela
            first_file = file_paths[0]
            self._load_csv_file(first_file)
            
            # Carrega os arquivos restantes
            for file_path in file_paths[1:]:
                # Nome da tabela temporária
                temp_table = f"temp_{hash(file_path)}"
                
                # Obtém opções para o CSV
                delimiter = self.config.get("delimiter", ",")
                header = self.config.get("header", True)
                
                # Carrega o arquivo em uma tabela temporária
                sql_query = f"""
                CREATE OR REPLACE TABLE {temp_table} AS
                SELECT * FROM read_csv_auto(
                    '{file_path}',
                    delim='{delimiter}',
                    header={str(header).lower()},
                    auto_detect=true
                )
                """
                self.connection.execute(sql_query)
                
                # Combina com a tabela principal
                self.connection.execute(f"INSERT INTO {self.table_name} SELECT * FROM {temp_table}")
                
                # Remove a tabela temporária
                self.connection.execute(f"DROP TABLE {temp_table}")
                
                logger.info(f"Arquivo CSV adicional carregado: {file_path}")
        else:
            # Carrega cada arquivo em uma tabela separada
            for file_path in file_paths:
                # Nome da tabela baseado no nome do arquivo
                file_name = os.path.basename(file_path)
                table_name = os.path.splitext(file_name)[0]
                
                # Obtém opções para o CSV
                delimiter = self.config.get("delimiter", ",")
                header = self.config.get("header", True)
                
                # Carrega o arquivo
                sql_query = f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_csv_auto(
                    '{file_path}',
                    delim='{delimiter}',
                    header={str(header).lower()},
                    auto_detect=true
                )
                """
                self.connection.execute(sql_query)
                
                logger.info(f"Arquivo CSV carregado como tabela separada: {table_name}")
                
            # Cria uma visualização combinada
            if len(file_paths) > 0:
                first_file = os.path.basename(file_paths[0])
                first_table = os.path.splitext(first_file)[0]
                self.connection.execute(f"CREATE OR REPLACE VIEW dados AS SELECT * FROM {first_table}")
                self.table_name = first_table
                
                logger.info(f"Usando primeira tabela como principal: {first_table}")
    
    def _load_excel_file(self, file_path: str) -> None:
        """
        Carrega um arquivo Excel no DuckDB.
        
        Args:
            file_path: Caminho para o arquivo Excel
        """
        try:
            # Determina as planilhas a carregar
            sheet_name = self.config.get("sheet_name")
            
            # Se for "all", carrega todas as planilhas
            if sheet_name == "all":
                import pandas as pd
                
                # Lê o Excel para obter as planilhas
                xlsx = pd.ExcelFile(file_path)
                sheet_names = xlsx.sheet_names
                
                # Carrega cada planilha
                for sheet in sheet_names:
                    # Constrói o nome da tabela
                    table_name = f"{self.table_name}_{sheet}"
                    
                    # Carrega a planilha usando pandas e registra no DuckDB
                    df = pd.read_excel(file_path, sheet_name=sheet)
                    self.connection.register(table_name, df)
                    
                    logger.info(f"Planilha carregada: {sheet} ({len(df)} registros)")
                
                # Cria uma visão combinada se solicitado
                if self.config.get("create_combined_view", True) and len(sheet_names) > 0:
                    # Usa a primeira planilha como base
                    self.connection.execute(f"CREATE OR REPLACE VIEW dados AS SELECT * FROM {self.table_name}_{sheet_names[0]}")
                    
                    logger.info(f"Usando primeira planilha como principal: {sheet_names[0]}")
            else:
                # Carrega uma planilha específica
                sheet_name = sheet_name if sheet_name is not None else 0
                
                # Carrega com pandas e registra no DuckDB
                import pandas as pd
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                self.connection.register(self.table_name, df)
                
                # Cria uma visualização para compatibilidade
                self.connection.execute(f"CREATE OR REPLACE VIEW dados AS SELECT * FROM {self.table_name}")
                
                logger.info(f"Planilha carregada: {sheet_name} ({len(df)} registros)")
                
        except Exception as e:
            logger.error(f"Erro ao carregar arquivo Excel {file_path}: {str(e)}")
            raise
    
    def _load_excel_files(self, file_paths: List[str]) -> None:
        """
        Carrega múltiplos arquivos Excel no DuckDB.
        
        Args:
            file_paths: Lista de caminhos para arquivos Excel
        """
        try:
            # Carrega cada arquivo
            for file_path in file_paths:
                # Nome da tabela baseado no nome do arquivo
                file_name = os.path.basename(file_path)
                base_table_name = os.path.splitext(file_name)[0]
                
                # Determina as planilhas a carregar
                sheet_name = self.config.get("sheet_name")
                
                # Se for "all", carrega todas as planilhas
                if sheet_name == "all":
                    import pandas as pd
                    
                    # Lê o Excel para obter as planilhas
                    xlsx = pd.ExcelFile(file_path)
                    sheet_names = xlsx.sheet_names
                    
                    # Carrega cada planilha
                    for sheet in sheet_names:
                        # Constrói o nome da tabela
                        table_name = f"{base_table_name}_{sheet}"
                        
                        # Carrega a planilha usando pandas e registra no DuckDB
                        df = pd.read_excel(file_path, sheet_name=sheet)
                        self.connection.register(table_name, df)
                        
                        logger.info(f"Planilha carregada: {sheet} ({len(df)} registros)")
                    
                    # Cria uma visão para o arquivo se houver planilhas
                    if len(sheet_names) > 0:
                        # Usa a primeira planilha como base para o arquivo
                        self.connection.execute(f"CREATE OR REPLACE VIEW {base_table_name} AS SELECT * FROM {base_table_name}_{sheet_names[0]}")
                        
                        logger.info(f"Visão criada para arquivo: {base_table_name}")
                else:
                    # Carrega uma planilha específica
                    sheet_name = sheet_name if sheet_name is not None else 0
                    
                    # Carrega com pandas e registra no DuckDB
                    import pandas as pd
                    df = pd.read_excel(file_path, sheet_name=sheet_name)
                    self.connection.register(base_table_name, df)
                    
                    logger.info(f"Arquivo Excel carregado: {base_table_name} ({len(df)} registros)")
            
            # Cria uma visão combinada
            if self.config.get("create_combined_view", True) and len(file_paths) > 0:
                # Usa o primeiro arquivo como base
                first_file = os.path.basename(file_paths[0])
                first_table = os.path.splitext(first_file)[0]
                self.connection.execute(f"CREATE OR REPLACE VIEW dados AS SELECT * FROM {first_table}")
                self.table_name = first_table
                
                logger.info(f"Usando primeiro arquivo como principal: {first_table}")
                
        except Exception as e:
            logger.error(f"Erro ao carregar arquivos Excel: {str(e)}")
            raise
    
    def read_data(self) -> pd.DataFrame:
        """
        Lê os dados da tabela principal.
        
        Returns:
            DataFrame com os dados
            
        Raises:
            RuntimeError: Se o conector não estiver conectado
        """
        if not self._connected:
            self.connect()
            
        # Lê a tabela principal
        result = self.connection.execute(f"SELECT * FROM {self.table_name}").fetchdf()
        return result
    
    def execute_query(self, sql_query: str) -> Dict[str, Any]:
        """
        Executa uma consulta SQL no DuckDB.
        
        Args:
            sql_query: Consulta SQL a ser executada
            
        Returns:
            Resultado da consulta
            
        Raises:
            RuntimeError: Se o conector não estiver conectado
        """
        if not self._connected:
            self.connect()
            
        try:
            # Executa a consulta
            result_df = self.connection.execute(sql_query).fetchdf()
            
            # Converte para o formato de saída
            result = {
                "data": result_df.to_dict(orient='records'),
                "columns": list(result_df.columns),
                "count": len(result_df)
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Erro ao executar consulta SQL: {str(e)}")
            raise
    
    def get_schema(self) -> Dict[str, Dict[str, str]]:
        """
        Obtém o schema das tabelas registradas.
        
        Returns:
            Dicionário com o schema (tabela -> {coluna -> tipo})
            
        Raises:
            RuntimeError: Se o conector não estiver conectado
        """
        if not self._connected:
            self.connect()
            
        try:
            schema = {}
            
            # Obtém todas as tabelas e visualizações
            tables = self.connection.execute("""
                SELECT table_name, table_type 
                FROM information_schema.tables 
                WHERE table_schema = 'main'
            """).fetchall()
            
            for table_name, table_type in tables:
                # Obtém o schema da tabela
                columns = self.connection.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = 'main' AND table_name = '{table_name}'
                """).fetchall()
                
                # Cria o dicionário de colunas
                table_schema = {col_name: data_type for col_name, data_type in columns}
                schema[table_name] = table_schema
            
            return schema
            
        except Exception as e:
            logger.error(f"Erro ao obter schema: {str(e)}")
            raise
    
    def close(self) -> None:
        """Fecha a conexão e libera recursos."""
        if self.connection:
            try:
                self.connection.close()
                self.connection = None
                self._connected = False
                
                logger.info("Conexão DuckDB fechada")
                
            except Exception as e:
                logger.error(f"Erro ao fechar conexão DuckDB: {str(e)}")
                raise