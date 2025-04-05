# -*- coding: utf-8 -*-
"""
Módulo para conectar com arquivos CSV.
"""

import os
import re
import logging
import pandas as pd
from typing import Dict, Any, Optional, List, Union

# Configuração de logging
logger = logging.getLogger(__name__)


class CSVConnector:
    """
    Conector para arquivos CSV.
    Carrega dados de arquivos CSV e fornece métodos para consulta.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Inicializa o conector CSV.
        
        Args:
            config: Configuração do conector
                - path: Caminho para o arquivo ou diretório CSV
                - delimiter: Delimitador do CSV (padrão: ',')
                - encoding: Codificação do arquivo (padrão: 'utf-8')
                - header: Linha do cabeçalho (padrão: 0)
                - id_column: Coluna de ID (opcional)
                - pattern: Padrão para busca em diretórios (opcional)
        """
        self.config = config
        self.data = None
        self._connected = False
        self.is_directory = False
        self.csv_files = []
        self.dataframes = {}
        
        # Valida a configuração
        self._validate_config()
        
        logger.info(f"CSVConnector inicializado com path: {self.config.get('path')}")
    
    def _validate_config(self) -> None:
        """Valida a configuração do conector."""
        # Verifica se o path foi fornecido
        if "path" not in self.config:
            raise ValueError("O path do arquivo CSV é obrigatório")
            
        # Verifica se o arquivo existe
        path = self.config["path"]
        if not os.path.exists(path):
            raise FileNotFoundError(f"Arquivo ou diretório não encontrado: {path}")
    
    def connect(self) -> None:
        """
        Carrega o arquivo CSV em memória.
        """
        try:
            path = self.config["path"]
            delimiter = self.config.get("delimiter", ",")
            encoding = self.config.get("encoding", "utf-8")
            header = self.config.get("header", 0)
            
            # Verifica se é um diretório ou arquivo
            if os.path.isdir(path):
                self.is_directory = True
                pattern = self.config.get("pattern", "*.csv")
                self._load_directory(path, pattern, delimiter, encoding, header)
            else:
                self._load_file(path, delimiter, encoding, header)
                
            self._connected = True
            logger.info(f"Conexão estabelecida com sucesso: {path}")
            
        except Exception as e:
            logger.error(f"Erro ao conectar com CSV: {str(e)}")
            raise
    
    def _load_file(self, file_path: str, delimiter: str, encoding: str, header: int) -> None:
        """
        Carrega um único arquivo CSV.
        
        Args:
            file_path: Caminho para o arquivo
            delimiter: Delimitador do CSV
            encoding: Codificação do arquivo
            header: Linha do cabeçalho
        """
        try:
            # Carrega o arquivo CSV
            df = pd.read_csv(
                file_path,
                delimiter=delimiter,
                encoding=encoding,
                header=header
            )
            
            # Infere tipos de dados
            for col in df.select_dtypes(include=['object']).columns:
                try:
                    # Tenta converter para datetime sem usar errors='ignore' (deprecated)
                    # Captura exceções explicitamente conforme recomendado no warning
                    try:
                        df[col] = pd.to_datetime(df[col], format='infer')
                    except (ValueError, TypeError):
                        # Se falhar, mantém como está
                        pass
                except Exception as e:
                    logger.debug(f"Erro ao converter coluna {col} para datetime: {str(e)}")
            
            self.data = df
            self.dataframes[os.path.basename(file_path)] = df
            # Add 'dados' as an alias for compatibility with hardcoded references
            self.dataframes['dados'] = df
            
            logger.info(f"Arquivo CSV carregado: {file_path} ({len(df)} registros)")
            
        except Exception as e:
            logger.error(f"Erro ao carregar arquivo CSV {file_path}: {str(e)}")
            raise
    
    def _load_directory(self, dir_path: str, pattern: str, delimiter: str, encoding: str, header: int) -> None:
        """
        Carrega todos os arquivos CSV de um diretório.
        
        Args:
            dir_path: Caminho para o diretório
            pattern: Padrão para busca de arquivos
            delimiter: Delimitador do CSV
            encoding: Codificação do arquivo
            header: Linha do cabeçalho
        """
        import glob
        
        try:
            # Busca por arquivos CSV no diretório
            search_pattern = os.path.join(dir_path, pattern)
            self.csv_files = glob.glob(search_pattern)
            
            if not self.csv_files:
                raise FileNotFoundError(f"Nenhum arquivo CSV encontrado em: {dir_path} (padrão: {pattern})")
            
            # Carrega cada arquivo
            combined_df = None
            for file_path in self.csv_files:
                try:
                    df = pd.read_csv(
                        file_path,
                        delimiter=delimiter,
                        encoding=encoding,
                        header=header
                    )
                    
                    # Adiciona ao dicionário de dataframes
                    file_name = os.path.basename(file_path)
                    self.dataframes[file_name] = df
                    
                    # Combina os dataframes se a opção estiver ativa
                    if self.config.get("auto_concat", True):
                        if combined_df is None:
                            combined_df = df
                        else:
                            # Tenta concatenar apenas se as colunas forem compatíveis
                            if set(df.columns) == set(combined_df.columns):
                                combined_df = pd.concat([combined_df, df], ignore_index=True)
                    
                    logger.info(f"Arquivo CSV carregado: {file_path} ({len(df)} registros)")
                    
                except Exception as e:
                    logger.error(f"Erro ao carregar arquivo CSV {file_path}: {str(e)}")
                    
            # Define o dataframe combinado como o principal se existir
            if combined_df is not None:
                self.data = combined_df
                logger.info(f"Dados combinados: {len(combined_df)} registros")
            else:
                # Usa o primeiro dataframe como principal se não puder combinar
                first_file = self.csv_files[0]
                self.data = self.dataframes[os.path.basename(first_file)]
                logger.info(f"Usando o primeiro arquivo como principal: {first_file}")
            
        except Exception as e:
            logger.error(f"Erro ao carregar diretório CSV {dir_path}: {str(e)}")
            raise
    
    def read_data(self) -> pd.DataFrame:
        """
        Lê os dados carregados.
        
        Returns:
            DataFrame com os dados
            
        Raises:
            RuntimeError: Se o conector não estiver conectado
        """
        if not self._connected:
            self.connect()
            
        return self.data
    
    def execute_query(self, sql_query: str) -> Dict[str, Any]:
        """
        Executa uma consulta SQL nos dados.
        
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
            # Implementação básica para testes com capacidades expandidas
            # Suporta: SELECT, FROM, WHERE, GROUP BY, ORDER BY, LIMIT
            
            # Normaliza a consulta para facilitar o parsing
            sql_query = ' '.join(sql_query.split()).lower()
            logger.debug(f"Consulta SQL normalizada: {sql_query}")
            
            # Determina qual dataframe usar baseado na cláusula FROM
            from_matches = re.search(r'from\s+(\w+)', sql_query)
            if not from_matches:
                raise ValueError("Cláusula FROM não encontrada na consulta SQL")
                
            table_name = from_matches.group(1)
            
            # Mapeia nomes de tabela para dataframes registrados
            if table_name in self.dataframes:
                df = self.dataframes[table_name]
            else:
                # Tenta buscar pelo nome do arquivo sem extensão
                found = False
                for key in self.dataframes.keys():
                    if key.split('.')[0] == table_name:
                        df = self.dataframes[key]
                        found = True
                        break
                        
                if not found:
                    logger.warning(f"Tabela '{table_name}' não encontrada, usando dataframe principal")
                    df = self.data
            
            # Faz uma cópia para não modificar o dataframe original
            result_df = df.copy()
            
            # Extrai as colunas do SELECT
            select_cols = ['*']
            select_part = sql_query.split('from')[0].replace('select', '').strip()
            
            # Verifica se temos agregações (sum, count, etc.)
            has_aggregation = re.search(r'(sum|count|avg|min|max|group by)', sql_query.lower()) is not None
            aggregation_result = None
            
            # Processa as expressões de agregação
            if not select_part == '*':
                if ',' in select_part:
                    select_cols = [c.strip() for c in select_part.split(',')]
                else:
                    select_cols = [select_part.strip()]
                
                # Processa colunas de agregação (sum, count, etc.)
                select_mapping = {}
                for i, col in enumerate(select_cols):
                    # Detecta funções de agregação
                    if 'sum(' in col:
                        col_name = re.search(r'sum\(([^)]+)\)', col).group(1).strip()
                        as_name = re.search(r'as\s+(\w+)', col)
                        as_name = as_name.group(1) if as_name else f"sum_{col_name}"
                        select_mapping[col] = (col_name, 'sum', as_name)
                    elif 'count(' in col:
                        if '*' in col:
                            col_name = '*'
                        else:
                            col_name = re.search(r'count\(([^)]+)\)', col).group(1).strip()
                        as_name = re.search(r'as\s+(\w+)', col)
                        as_name = as_name.group(1) if as_name else "contagem"
                        select_mapping[col] = (col_name, 'count', as_name)
                    elif 'as' in col:
                        col_parts = col.split('as')
                        orig_col = col_parts[0].strip()
                        alias = col_parts[1].strip()
                        select_mapping[col] = (orig_col, None, alias)
                    else:
                        select_mapping[col] = (col, None, col)
            
            # Aplica filtros do WHERE
            if 'where' in sql_query:
                where_part = sql_query.split('where')[1]
                if 'group by' in where_part:
                    where_part = where_part.split('group by')[0]
                elif 'order by' in where_part:
                    where_part = where_part.split('order by')[0]
                elif 'limit' in where_part:
                    where_part = where_part.split('limit')[0]
                
                where_part = where_part.strip()
                logger.debug(f"Cláusula WHERE: {where_part}")
                
                # Processa condições simples (por enquanto apenas suporta AND)
                if 'and' in where_part:
                    conditions = [c.strip() for c in where_part.split('and')]
                else:
                    conditions = [where_part]
                
                for condition in conditions:
                    # Detecta operador na condição
                    if '=' in condition:
                        op = '='
                        col, val = [p.strip() for p in condition.split('=')]
                    elif '>' in condition:
                        op = '>'
                        col, val = [p.strip() for p in condition.split('>')]
                    elif '<' in condition:
                        op = '<'
                        col, val = [p.strip() for p in condition.split('<')]
                    elif '>=' in condition:
                        op = '>='
                        col, val = [p.strip() for p in condition.split('>=')]
                    elif '<=' in condition:
                        op = '<='
                        col, val = [p.strip() for p in condition.split('<=')]
                    elif 'like' in condition:
                        op = 'like'
                        col, val = [p.strip() for p in condition.split('like')]
                    else:
                        logger.warning(f"Operador não suportado em condição: {condition}")
                        continue
                    
                    # Remove aspas das strings
                    col = col.strip("'\"")
                    val = val.strip("'\"")
                    
                    if col not in result_df.columns:
                        logger.warning(f"Coluna '{col}' não encontrada no dataframe")
                        continue
                    
                    # Aplica o filtro baseado no operador
                    if op == '=':
                        # Normalize strings for case-insensitive comparison
                        # Also handle accented characters by normalizing 
                        # 'eletrônicos' should match 'eletronicos'
                        import unicodedata
                        
                        def normalize_str(s):
                            # Remove accents and convert to lowercase
                            if isinstance(s, str):
                                # NFD decomposes characters, then we filter out combining marks
                                return ''.join(c for c in unicodedata.normalize('NFD', s.lower())
                                              if not unicodedata.combining(c))
                            return str(s).lower()
                        
                        # Apply normalization to both sides for consistent comparison
                        normalized_val = normalize_str(val)
                        result_df = result_df[result_df[col].astype(str).apply(normalize_str) == normalized_val]
                        
                        logger.debug(f"Aplicando filtro: {col} = {val} (normalizado: {normalized_val})")
                    elif op == '>':
                        try:
                            # Tenta converter para numérico se possível
                            result_df = result_df[pd.to_numeric(result_df[col], errors='coerce') > float(val)]
                        except:
                            # Fallback para string
                            result_df = result_df[result_df[col].astype(str) > val]
                    elif op == '<':
                        try:
                            result_df = result_df[pd.to_numeric(result_df[col], errors='coerce') < float(val)]
                        except:
                            result_df = result_df[result_df[col].astype(str) < val]
                    elif op == '>=':
                        try:
                            result_df = result_df[pd.to_numeric(result_df[col], errors='coerce') >= float(val)]
                        except:
                            result_df = result_df[result_df[col].astype(str) >= val]
                    elif op == '<=':
                        try:
                            result_df = result_df[pd.to_numeric(result_df[col], errors='coerce') <= float(val)]
                        except:
                            result_df = result_df[result_df[col].astype(str) <= val]
                    elif op == 'like':
                        # Converte padrão SQL LIKE para regex
                        pattern = val.replace('%', '.*')
                        result_df = result_df[result_df[col].astype(str).str.contains(pattern, case=False, regex=True)]
            
            # Processa GROUP BY
            if 'group by' in sql_query:
                group_part = sql_query.split('group by')[1]
                if 'order by' in group_part:
                    group_part = group_part.split('order by')[0]
                elif 'limit' in group_part:
                    group_part = group_part.split('limit')[0]
                
                group_part = group_part.strip()
                if ',' in group_part:
                    group_cols = [c.strip() for c in group_part.split(',')]
                else:
                    group_cols = [group_part]
                
                # Verifica se as colunas existem
                for col in group_cols:
                    if col not in result_df.columns:
                        logger.warning(f"Coluna de agrupamento '{col}' não encontrada")
                
                # Se temos agregações no select, executa-as com o groupby
                if has_aggregation and select_mapping:
                    # Cria um novo DataFrame para o resultado da agregação
                    grouped = result_df.groupby(group_cols)
                    
                    # Inicializa um dicionário vazio para o resultado
                    agg_data = {col: result_df[col].iloc[0] for col in group_cols}
                    
                    # Aplica cada função de agregação
                    for col_expr, (col_name, agg_func, alias) in select_mapping.items():
                        if agg_func == 'sum':
                            agg_data[alias] = grouped[col_name].sum()
                        elif agg_func == 'count':
                            if col_name == '*':
                                agg_data[alias] = grouped.size()
                            else:
                                agg_data[alias] = grouped[col_name].count()
                    
                    # Converte para DataFrame
                    result_df = pd.DataFrame(agg_data).reset_index(drop=True)
                else:
                    # Se não temos agregações explícitas, agrupa e conta
                    result_df = result_df.groupby(group_cols).size().reset_index(name='contagem')
            
            # Processa ORDER BY
            if 'order by' in sql_query:
                order_part = sql_query.split('order by')[1]
                if 'limit' in order_part:
                    order_part = order_part.split('limit')[0]
                
                order_part = order_part.strip()
                
                # Processa múltiplas colunas ORDER BY
                if ',' in order_part:
                    order_cols = []
                    ascending = []
                    
                    for col_expr in order_part.split(','):
                        col_expr = col_expr.strip()
                        if 'desc' in col_expr:
                            col = col_expr.split('desc')[0].strip()
                            asc = False
                        elif 'asc' in col_expr:
                            col = col_expr.split('asc')[0].strip()
                            asc = True
                        else:
                            col = col_expr
                            asc = True
                        
                        order_cols.append(col)
                        ascending.append(asc)
                    
                    # Verifica se as colunas existem
                    valid_cols = []
                    valid_asc = []
                    for i, col in enumerate(order_cols):
                        if col in result_df.columns:
                            valid_cols.append(col)
                            valid_asc.append(ascending[i])
                        else:
                            logger.warning(f"Coluna de ordenação '{col}' não encontrada")
                    
                    if valid_cols:
                        result_df = result_df.sort_values(by=valid_cols, ascending=valid_asc)
                    
                else:
                    # Processa expressão única ORDER BY
                    col_expr = order_part
                    
                    # Detecta se é DESC ou ASC
                    if 'desc' in col_expr:
                        col = col_expr.split('desc')[0].strip()
                        ascending = False
                    elif 'asc' in col_expr:
                        col = col_expr.split('asc')[0].strip()
                        ascending = True
                    else:
                        col = col_expr
                        ascending = True
                    
                    if col in result_df.columns:
                        result_df = result_df.sort_values(by=col, ascending=ascending)
                    else:
                        logger.warning(f"Coluna de ordenação '{col}' não encontrada")
            
            # Aplica LIMIT
            if 'limit' in sql_query:
                limit_part = sql_query.split('limit')[1].strip()
                try:
                    limit = int(limit_part)
                    result_df = result_df.head(limit)
                except ValueError:
                    logger.warning(f"Valor inválido para LIMIT: {limit_part}")
            
            # Seleciona apenas as colunas especificadas no SELECT, se não for '*'
            if select_cols != ['*'] and not has_aggregation:
                valid_cols = [col for col in select_cols if col in result_df.columns]
                if valid_cols:
                    result_df = result_df[valid_cols]
            
            # Converte para o formato de saída
            result = {
                "data": result_df.to_dict(orient='records'),
                "columns": list(result_df.columns),
                "count": len(result_df)
            }
            
            logger.info(f"Consulta SQL executada com sucesso: {len(result_df)} registros retornados")
            return result
            
        except Exception as e:
            logger.error(f"Erro ao executar consulta SQL: {str(e)}")
            raise ValueError(f"Formato SQL não suportado no modo básico: {str(e)}")
    
    def get_schema(self) -> Dict[str, Dict[str, str]]:
        """
        Obtém o schema dos dados.
        
        Returns:
            Dicionário com o schema (tabela -> {coluna -> tipo})
            
        Raises:
            RuntimeError: Se o conector não estiver conectado
        """
        if not self._connected:
            self.connect()
            
        schema = {}
        
        # Mapeia tipos pandas para tipos SQL
        type_mapping = {
            'int64': 'INTEGER',
            'float64': 'FLOAT',
            'object': 'TEXT',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP'
        }
        
        # Adiciona o schema do dataframe principal
        main_table = self.config.get("table_name", "dados")
        columns = {}
        
        for col_name, dtype in self.data.dtypes.items():
            # Mapeia o tipo pandas para o tipo SQL
            sql_type = type_mapping.get(str(dtype), 'TEXT')
            columns[str(col_name)] = sql_type
            
        schema[main_table] = columns
        
        # Adiciona o schema de cada dataframe separado
        if self.is_directory and not self.config.get("auto_concat", True):
            for file_name, df in self.dataframes.items():
                table_name = os.path.splitext(file_name)[0]  # Remove a extensão
                columns = {}
                
                for col_name, dtype in df.dtypes.items():
                    sql_type = type_mapping.get(str(dtype), 'TEXT')
                    columns[str(col_name)] = sql_type
                    
                schema[table_name] = columns
        
        return schema
    
    def close(self) -> None:
        """Fecha a conexão e libera recursos."""
        # Limpa a memória
        self.data = None
        self.dataframes.clear()
        self._connected = False
        
        logger.info("Conexão com CSV fechada")