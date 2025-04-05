"""
Sistema de fluxo alternativo quando ocorrem falhas na LLM.
"""

import re
import os
import logging
from typing import List, Dict, Any, Optional, Set, Union

from core.engine.dataset import Dataset
from core.response.string import StringResponse
from core.user_query import UserQuery

# Configura o logger
logger = logging.getLogger("core_integration")


class AlternativeFlow:
    """
    Implementa o fluxo alternativo para falhas na geração ou execução de consultas.
    
    Fornece:
    - Detecção de entidades inexistentes
    - Reformulação automática de consultas
    - Geração de consultas alternativas
    - Tratamento amigável de erros
    """
    
    def __init__(self, datasets: Dict[str, Dataset], llm_generator=None, feedback_manager=None, sql_executor=None):
        """
        Inicializa o fluxo alternativo.
        
        Args:
            datasets: Dicionário de datasets disponíveis (nome -> objeto Dataset)
            llm_generator: Gerador de código LLM para reformulações
            feedback_manager: Opcional, gerenciador de feedback
            sql_executor: Opcional, executor SQL
        """
        self.datasets = datasets
        self.llm_generator = llm_generator
        self.feedback_manager = feedback_manager
        self.sql_executor = sql_executor
        self._setup_entity_keywords()
    
    def _setup_entity_keywords(self):
        """Configura as palavras-chave para detecção de entidades."""
        # Lista de palavras-chave que indicam consultas sobre entidades não existentes
        self.missing_entity_keywords = {
            'produtos': ['produtos', 'produto', 'estoque', 'inventário', 'item', 'itens', 'mercadoria'],
            'funcionários': ['funcionários', 'funcionário', 'funcionario', 'funcionarios', 'colaborador', 'colaboradores', 'empregado', 'empregados', 'staff', 'equipe'],
            'departamentos': ['departamento', 'departamentos', 'setor', 'setores', 'área', 'áreas', 'divisão', 'divisões'],
            'categorias': ['categoria', 'categorias', 'classe', 'classes', 'tipo de produto', 'tipos de produto']
        }
    
    def update_datasets(self, datasets: Dict[str, Dataset]):
        """
        Atualiza a lista de datasets disponíveis.
        
        Args:
            datasets: Novo dicionário de datasets
        """
        self.datasets = datasets
        
    def pre_query_check(self, user_query) -> Optional[StringResponse]:
        """
        Verifica se a consulta tem problemas antes de ser enviada para o LLM.
        
        Args:
            user_query: Objeto UserQuery com a consulta do usuário
            
        Returns:
            StringResponse se houver um problema detectado, None caso contrário
        """
        # Verifica se a consulta menciona entidades que não existem nos dados
        return self.check_missing_entities(user_query.query)
    
    def check_missing_entities(self, query: str) -> Optional[StringResponse]:
        """
        Verifica se a consulta menciona entidades que não existem.
        
        Args:
            query: Consulta em linguagem natural
            
        Returns:
            StringResponse se detectar entidades inexistentes, None caso contrário
        """
        # Verifica se a consulta menciona entidades não existentes
        for entity_type, keywords in self.missing_entity_keywords.items():
            entity_exists = any(entity_type in ds.name.lower() for ds in self.datasets.values())
            
            # Se a entidade não existe mas é mencionada na consulta
            if not entity_exists and any(keyword in query.lower() for keyword in keywords):
                # Gera sugestões de consultas alternativas baseadas nos dados disponíveis
                alternative_queries = self.generate_alternative_queries()
                datasets_desc = ", ".join([f"{name}" for name, _ in self.datasets.items()])
                
                return self.create_missing_entity_response(
                    entity_type, 
                    datasets_desc, 
                    alternative_queries
                )
                
        return None
    
    def create_missing_entity_response(self, entity_type: str, datasets_desc: str, alternative_queries: List[str]) -> StringResponse:
        """
        Cria uma resposta amigável para consultas sobre entidades não existentes.
        
        Args:
            entity_type: Tipo de entidade não encontrada
            datasets_desc: Descrição dos datasets disponíveis
            alternative_queries: Consultas alternativas sugeridas
            
        Returns:
            StringResponse com mensagem e sugestões
        """
        message = f"Não há dados sobre {entity_type} disponíveis. Os datasets disponíveis são: {datasets_desc}."
        
        if alternative_queries:
            message += "\n\nVocê pode tentar estas consultas alternativas:\n"
            for i, query in enumerate(alternative_queries[:3], 1):
                message += f"{i}. {query}\n"
            
        message += "\nPor favor, reformule sua consulta para usar os dados disponíveis."
        
        return StringResponse(message)
    
    def handle_error(self, query, error, code=None) -> Optional[StringResponse]:
        """
        Tenta gerar uma resposta útil quando ocorre um erro.
        
        Args:
            query: Objeto UserQuery com a consulta do usuário
            error: String com a mensagem de erro
            code: Opcional, código que gerou o erro
            
        Returns:
            StringResponse com informações úteis ou None
        """
        error_str = str(error).lower()
        
        # Tratamentos específicos para diferentes tipos de erros
        if "no such table" in error_str or "table not found" in error_str:
            return self.handle_missing_table_error(error_str)
        
        # Se chegamos aqui, não temos tratamento específico
        # Oferece sugestões predefinidas como último recurso
        return self.offer_predefined_options(query.query if hasattr(query, 'query') else str(query), str(error))
        
    def handle_missing_table_error(self, error_msg: str) -> StringResponse:
        """
        Gera uma resposta amigável para erros de tabela não encontrada.
        
        Args:
            error_msg: Mensagem de erro original
            
        Returns:
            StringResponse com informações úteis
        """
        # Extrai o nome da tabela da mensagem de erro, se possível
        import re
        table_match = re.search(r"tabela '(\w+)'", error_msg)
        missing_table = table_match.group(1) if table_match else "mencionada"
        
        # Lista de datasets disponíveis com suas colunas
        datasets_info = []
        for name, ds in self.datasets.items():
            cols = ", ".join(ds.dataframe.columns[:5]) + ("..." if len(ds.dataframe.columns) > 5 else "")
            datasets_info.append(f"• {name}: {cols}")
        
        datasets_desc = "\n".join(datasets_info)
        
        message = f"""Não foi possível encontrar a tabela '{missing_table}' nos dados disponíveis.

Os datasets disponíveis são:
{datasets_desc}

Por favor, reformule sua consulta para usar apenas os datasets listados acima."""
        
        return StringResponse(message)
    
    def rephrase_query(self, original_query: str, error_info: str) -> str:
        """
        Usa o LLM para reformular a consulta original baseado no erro encontrado.
        
        Args:
            original_query: Consulta original que falhou
            error_info: Informação sobre o erro
            
        Returns:
            Consulta reformulada
        """
        if not self.llm_generator:
            # Se não temos gerador LLM, usa simplificação básica
            return self.simplify_query(original_query)
            
        # Lista de datasets disponíveis
        available_datasets = ', '.join(self.datasets.keys())
        
        # Colunas disponíveis em cada dataset
        datasets_columns = {}
        for name, ds in self.datasets.items():
            datasets_columns[name] = list(ds.dataframe.columns)
            
        # Instrução para reformular a consulta
        prompt = f"""
A consulta original "{original_query}" falhou com o seguinte erro:
{error_info}

Por favor, reformule a consulta para evitar este erro.

Datasets disponíveis:
{available_datasets}

Colunas disponíveis:
{str(datasets_columns)}

Sua reformulação deve:
1. Usar apenas os datasets e colunas disponíveis
2. Ser mais simples que a consulta original
3. Capturar a intenção original do usuário

Reformulação:
"""
        
        try:
            # Usa o gerador LLM para reformular a consulta
            if hasattr(self.llm_generator, 'generate_rephrased_query'):
                return self.llm_generator.generate_rephrased_query(prompt)
            
            # Se não tem método específico mas tem generate_text
            if hasattr(self.llm_generator, 'generate_text'):
                return self.llm_generator.generate_text(prompt)
                
            # Se não tem generate_text, tenta generate_code (não ideal, mas pode funcionar)
            rephrase_code = self.llm_generator.generate_code(
                system_message="Você é um assistente que reformula consultas para evitar erros.",
                user_message=prompt
            )
            
            # Extrai apenas a reformulação da resposta
            import re
            rephrase_match = re.search(r'(?:Reformulação:|```)(.*?)(?:$|```)', rephrase_code, re.DOTALL)
            if rephrase_match:
                return rephrase_match.group(1).strip()
                
            # Se não conseguiu extrair, retorna o código completo (não ideal)
            return rephrase_code.strip()
            
        except Exception as e:
            logger.error(f"Erro ao reformular consulta: {str(e)}")
            # Em caso de erro, tenta uma simplificação básica
            return self.simplify_query(original_query)
            
    def simplify_query(self, query: str) -> str:
        """
        Simplifica a consulta original para torná-la mais genérica.
        
        Args:
            query: Consulta original
            
        Returns:
            Consulta simplificada
        """
        # Substitui termos específicos por termos mais genéricos
        simplifications = [
            (r'produto[s]?', 'dados'),
            (r'funcionario[s]?|colaborador[es]?|empregado[s]?', 'pessoas'),
            (r'departamento[s]?|setor[es]?', 'grupos'),
            (r'categorias?', 'tipos'),
            (r'estoque', 'quantidade'),
            (r'inventário', 'dados'),
        ]
        
        simplified = query
        for pattern, replacement in simplifications:
            simplified = re.sub(pattern, replacement, simplified, flags=re.IGNORECASE)
        
        # Se a consulta foi modificada, retorna a versão simplificada
        if simplified != query:
            return simplified
        
        # Se não conseguiu simplificar, retorna uma consulta ainda mais genérica
        keywords = ['mostre', 'liste', 'exiba', 'apresente', 'qual', 'quais', 'como', 'onde', 'quando']
        for keyword in keywords:
            if keyword in query.lower():
                # Extrai apenas a parte após a palavra-chave
                parts = re.split(rf'{keyword}\s+', query.lower(), flags=re.IGNORECASE, maxsplit=1)
                if len(parts) > 1:
                    return f"{keyword} os dados disponíveis sobre {parts[1]}"
        
        # Último recurso: consulta completamente genérica
        return "Mostre um resumo dos dados disponíveis"
    
    def generate_alternative_queries(self) -> List[str]:
        """
        Gera consultas alternativas baseadas nos datasets disponíveis.
        
        Returns:
            Lista de consultas alternativas sugeridas
        """
        alternatives = []
        
        # Para cada dataset
        for name, ds in self.datasets.items():
            # Sugestões baseadas no nome do dataset
            alternatives.append(f"Mostre os dados de {name}")
            alternatives.append(f"Quantos registros existem em {name}?")
            
            # Sugestões baseadas nas colunas numéricas
            numeric_cols = ds.dataframe.select_dtypes(include=['number']).columns.tolist()
            if numeric_cols:
                for col in numeric_cols[:2]:  # limita a 2 colunas
                    alternatives.append(f"Qual a média de {col} em {name}?")
                    alternatives.append(f"Quais são os valores máximo e mínimo de {col} em {name}?")
                    
            # Sugestões baseadas nas colunas de data
            date_cols = [col for col in ds.dataframe.columns if 'date' in col.lower() or 'data' in col.lower()]
            if date_cols:
                for col in date_cols[:1]:  # limita a 1 coluna
                    alternatives.append(f"Mostre os dados de {name} agrupados por {col}")
                    
            # Sugestões de agrupamento
            categorical_cols = ds.dataframe.select_dtypes(include=['object', 'category']).columns.tolist()
            if categorical_cols:
                for col in categorical_cols[:1]:  # limita a 1 coluna
                    if numeric_cols:
                        alternatives.append(f"Mostre a média de {numeric_cols[0]} por {col} em {name}")
                        
            # Sugestões para buscar tendências/padrões
            alternatives.append(f"Quais são os principais padrões em {name}?")
            
            # Sugestões para explorar relacionamentos entre datasets
            for target, _ in self.datasets.items():
                if target != name:
                    alternatives.append(f"Mostre dados de {name} relacionados com {target}")
        
        # Remove duplicatas e limita a 10 alternativas
        unique_alternatives = list(set(alternatives))
        return unique_alternatives[:10]
    
    def offer_predefined_options(self, query: str, error_msg: str) -> StringResponse:
        """
        Oferece opções predefinidas de consultas quando todas as tentativas falharam.
        
        Args:
            query: Consulta original
            error_msg: Mensagem de erro
            
        Returns:
            StringResponse com opções predefinidas
        """
        # Gera alternativas
        alternatives = self.generate_alternative_queries()
        
        # Prepara a mensagem de resposta
        message = f"""Não foi possível processar a consulta: "{query}"

Erro: {error_msg}

Aqui estão algumas consultas alternativas que você pode tentar:

"""
        # Adiciona as alternativas numeradas
        for i, alt in enumerate(alternatives[:5], 1):
            message += f"{i}. {alt}\n"
            
        message += """
Você também pode:
• Simplificar sua consulta
• Especificar exatamente quais datasets quer consultar
• Fornecer feedback para melhorarmos o sistema
"""
        
        return StringResponse(message)