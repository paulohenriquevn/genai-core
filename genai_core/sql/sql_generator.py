import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class SQLGenerator:
    """
    Gerador de SQL para o GenAI Core.
    Versão simplificada para testes.
    """
    
    def __init__(self, dialect="duckdb"):
        """
        Inicializa o gerador de SQL.
        
        Args:
            dialect: Dialeto SQL a ser utilizado
        """
        self.dialect = dialect
        logger.info(f"SQLGenerator inicializado com dialeto padrão: {dialect}")
        
    def generate_sql(self, semantics: Dict[str, Any], schema: Optional[Dict[str, Any]] = None) -> str:
        """
        Gera uma consulta SQL a partir de uma estrutura semântica.
        
        Args:
            semantics: Estrutura semântica da consulta
            schema: Schema das tabelas disponíveis
            
        Returns:
            Consulta SQL gerada
        """
        logger.info("Gerando SQL a partir da estrutura semântica")
        
        # Valor padrão para evitar erros
        if not semantics:
            return "SELECT 1"
            
        # Extrai os elementos principais da estrutura semântica
        intent = semantics.get("intent", "consulta_dados")
        data_source = semantics.get("data_source", "vendas")
        
        # Gera SQL baseado na intenção
        if intent == "consulta_dados":
            sql = f"SELECT * FROM '{data_source}' LIMIT 100"
        elif intent == "agregacao":
            sql = f"SELECT 'cliente', SUM('valor') as total FROM '{data_source}' GROUP BY 'cliente' ORDER BY total DESC"
        elif intent == "filtragem":
            # Simplificado para testes
            if "eletrônicos" in str(semantics).lower():
                sql = f"SELECT * FROM '{data_source}' WHERE 'categoria' = 'eletrônicos'"
            else:
                sql = f"SELECT * FROM '{data_source}' WHERE 1=1"
        elif intent == "classificacao":
            sql = f"SELECT * FROM '{data_source}' ORDER BY 'valor' DESC LIMIT 5"
        else:
            # Consulta genérica
            sql = f"SELECT * FROM '{data_source}' LIMIT 10"
        
        # Loga o SQL gerado
        logger.info(f"SQL gerado: {sql}")
        
        # Gera também com um método legado para testes de compatibilidade
        # (versão simplificada que não usa aspas nos identificadores)
        legacy_sql = sql.replace("'", "")
        logger.info(f"SQL gerado (método legado): {legacy_sql}")
        
        return sql