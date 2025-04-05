# -*- coding: utf-8 -*-
"""
Funções auxiliares e utilitárias para uso em todo o projeto.
"""

import os
import logging
import json
import pandas as pd
import re
from typing import Dict, Any, Optional, List, Union, Tuple

# Configuração de logging
logger = logging.getLogger(__name__)


def setup_logging(log_level: str = "info", log_file: Optional[str] = None) -> None:
    """
    Configura o sistema de logging.
    
    Args:
        log_level: Nível de log (debug, info, warning, error, critical)
        log_file: Caminho para o arquivo de log (opcional)
    """
    # Mapeia o nível de log
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL
    }
    
    log_level = level_map.get(log_level.lower(), logging.INFO)
    
    # Configuração do handler
    handlers = []
    
    # Handler de console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    handlers.append(console_handler)
    
    # Handler de arquivo se especificado
    if log_file:
        try:
            # Cria o diretório se não existir
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(log_level)
            file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(file_formatter)
            handlers.append(file_handler)
        except Exception as e:
            print(f"Erro ao configurar arquivo de log: {str(e)}")
    
    # Configura o logger raiz
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remove handlers existentes
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Adiciona os novos handlers
    for handler in handlers:
        root_logger.addHandler(handler)
    
    logger.info(f"Logging configurado com nível: {log_level}")


def load_json_file(file_path: str, default: Any = None) -> Any:
    """
    Carrega um arquivo JSON.
    
    Args:
        file_path: Caminho para o arquivo JSON
        default: Valor padrão se o arquivo não existir ou for inválido
        
    Returns:
        Conteúdo do arquivo JSON ou o valor padrão
    """
    try:
        if not os.path.exists(file_path):
            logger.warning(f"Arquivo não encontrado: {file_path}")
            return default
            
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
            
    except Exception as e:
        logger.error(f"Erro ao carregar arquivo JSON {file_path}: {str(e)}")
        return default


def save_json_file(file_path: str, data: Any, indent: int = 2) -> bool:
    """
    Salva dados em um arquivo JSON.
    
    Args:
        file_path: Caminho para o arquivo JSON
        data: Dados a serem salvos
        indent: Indentação do JSON
        
    Returns:
        True se salvou com sucesso, False caso contrário
    """
    try:
        # Cria o diretório se não existir
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)
            
        logger.info(f"Arquivo JSON salvo: {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao salvar arquivo JSON {file_path}: {str(e)}")
        return False


def infer_data_types(df: pd.DataFrame) -> Dict[str, str]:
    """
    Infere os tipos de dados de um DataFrame para uso em SQL.
    
    Args:
        df: DataFrame pandas
        
    Returns:
        Dicionário mapeando colunas para tipos SQL
    """
    # Mapeia tipos pandas para tipos SQL
    type_mapping = {
        'int64': 'INTEGER',
        'int32': 'INTEGER',
        'float64': 'FLOAT',
        'float32': 'FLOAT',
        'object': 'TEXT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
        'category': 'TEXT',
        'timedelta64[ns]': 'INTERVAL'
    }
    
    column_types = {}
    
    for col_name, dtype in df.dtypes.items():
        # Determina o tipo SQL
        sql_type = type_mapping.get(str(dtype), 'TEXT')
        
        # Verifica se colunas do tipo object podem ser convertidas para outros tipos
        if dtype == 'object':
            # Tenta converter para datetime
            try:
                if pd.to_datetime(df[col_name], errors='coerce').notna().all():
                    sql_type = 'TIMESTAMP'
            except:
                pass
                
            # Verifica se é um booleano
            if sql_type == 'TEXT':
                try:
                    unique_values = df[col_name].dropna().unique()
                    if set(unique_values).issubset({'true', 'false', 'True', 'False', '0', '1', 0, 1, True, False}):
                        sql_type = 'BOOLEAN'
                except:
                    pass
        
        column_types[str(col_name)] = sql_type
    
    return column_types


def detect_query_type(query: str) -> Tuple[str, Dict[str, Any]]:
    """
    Detecta o tipo de consulta a partir do texto em linguagem natural.
    
    Args:
        query: Texto da consulta em linguagem natural
        
    Returns:
        Tuple com (tipo_de_consulta, metadados)
    """
    query_lower = query.lower()
    
    # Palavras-chave para diferentes tipos de consultas
    visualization_keywords = ["gráfico", "visualização", "apresente", "mostre", "visualize", 
                             "chart", "plot", "exiba", "figura", "diagrama"]
    
    aggregation_keywords = ["total", "soma", "média", "contagem", "máximo", "mínimo", 
                           "average", "sum", "count", "max", "min", "quantos"]
    
    filtering_keywords = ["filtr", "onde", "quando", "qual", "quais", "filtro", 
                         "filter", "where", "cujo", "contendo", "com"]
    
    # Detecta o tipo primário da consulta
    if any(keyword in query_lower for keyword in visualization_keywords):
        query_type = "visualization"
        
        # Tenta detectar o tipo de visualização
        viz_metadata = {"chart_type": "bar"}  # padrão
        
        if "linha" in query_lower or "temporal" in query_lower or "line" in query_lower:
            viz_metadata["chart_type"] = "line"
        elif "pizza" in query_lower or "pie" in query_lower:
            viz_metadata["chart_type"] = "pie"
        elif "dispersão" in query_lower or "scatter" in query_lower:
            viz_metadata["chart_type"] = "scatter"
        
        return "visualization", viz_metadata
        
    elif any(keyword in query_lower for keyword in aggregation_keywords):
        return "aggregation", {}
        
    elif any(keyword in query_lower for keyword in filtering_keywords):
        return "filtering", {}
        
    else:
        # Consulta genérica
        return "generic", {}


def safe_eval(expression: str, context: Optional[Dict[str, Any]] = None) -> Any:
    """
    Avalia expressões Python de forma segura.
    
    Args:
        expression: Expressão Python a ser avaliada
        context: Contexto para avaliação (opcional)
        
    Returns:
        Resultado da avaliação
        
    Raises:
        ValueError: Se a expressão contiver código potencialmente inseguro
    """
    # Lista de palavras-chave proibidas para segurança
    blacklist = ['__', 'eval', 'exec', 'compile', 'import', 'open', 'file', 
                'os', 'sys', 'subprocess', 'shutil', 'requests']
    
    # Verifica palavras-chave proibidas
    for word in blacklist:
        if word in expression:
            raise ValueError(f"Expressão contém código potencialmente inseguro: {word}")
    
    # Define contexto seguro
    safe_context = {
        'abs': abs,
        'bool': bool,
        'float': float,
        'int': int,
        'len': len,
        'max': max,
        'min': min,
        'round': round,
        'str': str,
        'sum': sum
    }
    
    # Adiciona contexto personalizado se fornecido
    if context:
        safe_context.update(context)
    
    # Avalia a expressão de forma segura
    return eval(expression, {"__builtins__": {}}, safe_context)


def format_number(number: Union[int, float], precision: int = 2) -> str:
    """
    Formata um número para exibição.
    
    Args:
        number: Número a ser formatado
        precision: Número de casas decimais
        
    Returns:
        Número formatado como string
    """
    if isinstance(number, int):
        return f"{number:,}".replace(",", ".")
    else:
        return f"{number:,.{precision}f}".replace(",", "X").replace(".", ",").replace("X", ".")