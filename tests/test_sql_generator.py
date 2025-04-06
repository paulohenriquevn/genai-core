#!/usr/bin/env python3
"""
Testes para o módulo SQLGenerator.

Este arquivo contém testes unitários para o gerador de SQL,
verificando se as consultas são geradas corretamente para diferentes
tipos de estruturas semânticas.
"""

import unittest
import logging
import sys
import os
from typing import Dict, Any

# Adiciona o diretório raiz ao PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importa as classes a serem testadas
from genai_core.sql.sql_generator import SQLGenerator

# Configura logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_sql_generator")


class TestSQLGenerator(unittest.TestCase):
    """Testes para o gerador de SQL."""
    
    def setUp(self):
        """Inicializa o ambiente de teste."""
        # Inicializa o gerador SQL sem configurações específicas
        self.sql_generator = SQLGenerator()
        
        # Define um schema de exemplo para testes
        self.sample_schema = {
            "data_sources": {
                "vendas": {
                    "tables": {
                        "vendas": {
                            "columns": {
                                "id": "INTEGER",
                                "data": "TIMESTAMP",
                                "cliente": "TEXT",
                                "produto": "TEXT",
                                "categoria": "TEXT",
                                "valor": "FLOAT",
                                "quantidade": "INTEGER"
                            }
                        }
                    }
                },
                "clientes": {
                    "tables": {
                        "clientes": {
                            "columns": {
                                "id": "INTEGER",
                                "nome": "TEXT",
                                "cidade": "TEXT",
                                "tipo": "TEXT",
                                "limite_credito": "FLOAT"
                            }
                        }
                    }
                }
            }
        }
    
    def test_simple_select_query(self):
        """Testa a geração de uma consulta SELECT simples."""
        # Define uma estrutura semântica para consulta simples
        semantics = {
            "intent": "consulta_dados",
            "parameters": {
                "table": "vendas",
                "limit": 10
            },
            "data_source": "vendas"
        }
        
        # Gera o SQL e verifica se está correto
        sql_query = self.sql_generator.generate_sql(semantics, self.sample_schema)
        
        # Valida a query gerada
        self.assertTrue(sql_query.startswith("SELECT"), "A consulta deve começar com SELECT")
        self.assertIn("FROM 'vendas'", sql_query, "A consulta deve conter a tabela correta")
        self.assertIn("LIMIT 10", sql_query, "A consulta deve conter o LIMIT")
    
    def test_aggregation_query(self):
        """Testa a geração de uma consulta de agregação."""
        # Define uma estrutura semântica para consulta de agregação
        semantics = {
            "intent": "agregacao",
            "parameters": {
                "table": "vendas",
                "group_col": "categoria",
                "value_col": "valor",
                "alias": "total"
            },
            "data_source": "vendas"
        }
        
        # Gera o SQL e verifica se está correto
        sql_query = self.sql_generator.generate_sql(semantics, self.sample_schema)
        
        # Valida a query gerada
        self.assertIn("SELECT 'categoria'", sql_query, "A consulta deve conter a coluna de agrupamento")
        self.assertIn("SUM('valor')", sql_query, "A consulta deve conter a função de agregação")
        self.assertIn("GROUP BY 'categoria'", sql_query, "A consulta deve conter a cláusula GROUP BY")
        self.assertIn("ORDER BY total", sql_query, "A consulta deve conter a cláusula ORDER BY")
    
    def test_filter_query(self):
        """Testa a geração de uma consulta com filtro."""
        # Define uma estrutura semântica para consulta com filtro
        semantics = {
            "intent": "filtragem",
            "parameters": {
                "table": "vendas",
                "filter_col": "categoria",
                "filter_value": "Eletrônicos"
            },
            "data_source": "vendas"
        }
        
        # Gera o SQL e verifica se está correto
        sql_query = self.sql_generator.generate_sql(semantics, self.sample_schema)
        
        # Valida a query gerada
        self.assertIn("WHERE 'categoria' = 'Eletrônicos'", sql_query, 
                     "A consulta deve conter a cláusula WHERE com o filtro correto")
    
    def test_order_query(self):
        """Testa a geração de uma consulta com ordenação."""
        # Define uma estrutura semântica para consulta com ordenação
        semantics = {
            "intent": "classificacao",
            "parameters": {
                "table": "vendas",
                "order_col": "valor",
                "limit": 5
            },
            "data_source": "vendas"
        }
        
        # Gera o SQL e verifica se está correto
        sql_query = self.sql_generator.generate_sql(semantics, self.sample_schema)
        
        # Valida a query gerada
        self.assertIn("ORDER BY 'valor' DESC", sql_query, 
                     "A consulta deve conter a cláusula ORDER BY com a coluna correta")
        self.assertIn("LIMIT 5", sql_query, "A consulta deve conter o LIMIT correto")
    
    def test_parameter_formatting(self):
        """Testa se os parâmetros são formatados corretamente."""
        # Testa alguns valores específicos
        self.assertEqual(self.sql_generator._format_parameter(None), "NULL", "None deve ser formatado como NULL")
        self.assertEqual(self.sql_generator._format_parameter(True), "TRUE", "True deve ser formatado como TRUE")
        self.assertEqual(self.sql_generator._format_parameter(42), "42", "Números devem ser formatados como strings")
        self.assertEqual(self.sql_generator._format_parameter("texto"), "'texto'", 
                        "Strings devem ser formatadas com aspas simples")
        self.assertEqual(self.sql_generator._format_parameter("O'Reilly"), "'O''Reilly'", 
                        "Strings com aspas simples devem ser escapadas")
        self.assertEqual(self.sql_generator._format_parameter([1, 2, 3]), "1, 2, 3", 
                        "Listas devem ser formatadas como valores separados por vírgula")
    
    def test_legacy_generate_method(self):
        """Testa se o método legado generate ainda funciona corretamente."""
        # Define uma estrutura semântica com template explícito
        semantics = {
            "sql_template": "SELECT * FROM {table} WHERE categoria = '{categoria}'",
            "parameters": {
                "table": "vendas",
                "categoria": "Eletrônicos"
            }
        }
        
        # Gera o SQL com o método legado
        sql_query = self.sql_generator.generate(semantics)
        
        # Valida a query gerada
        self.assertEqual(sql_query, "SELECT * FROM vendas WHERE categoria = 'Eletrônicos'", 
                        "O método legado deve gerar a consulta correta")


if __name__ == "__main__":
    unittest.main()