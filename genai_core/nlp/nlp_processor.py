"""
Processador de linguagem natural para o GenAI Core.
Versão simplificada para testes.
"""

import logging

logger = logging.getLogger(__name__)

class NLPProcessor:
    """
    Processador de linguagem natural.
    Versão simplificada para testes.
    """
    
    def __init__(self, model="mock"):
        """
        Inicializa o processador.
        
        Args:
            model: Modelo de linguagem a ser utilizado (mock, openai, etc.)
        """
        self.model = model
        
        if model == "mock":
            logger.info("Usando modo mock (baseado em regras)")
        else:
            logger.info(f"Usando modelo {model}")
            
        logger.info(f"NLPProcessor inicializado com modelo: {model}")
        
    def parse_question(self, question, schema=None):
        """
        Analisa uma pergunta em linguagem natural.
        
        Args:
            question: Pergunta em linguagem natural
            schema: Schema das tabelas disponíveis
            
        Returns:
            Estrutura semântica da pergunta
        """
        logger.info(f"Processando consulta com regras: {question}")
        
        # Versão simplificada para testes
        # Detecta palavras-chave para classificar a consulta
        
        # Detecta a fonte de dados
        data_source = "vendas"  # Padrão
        if "vendas" in question.lower():
            data_source = "vendas"
            logger.debug(f"Fonte de dados encontrada na consulta: {data_source}")
        elif "clientes" in question.lower():
            data_source = "clientes"
            logger.debug(f"Fonte de dados encontrada na consulta: {data_source}")
        else:
            logger.debug(f"Usando primeira fonte disponível: {data_source}")
            
        # Detecta o tipo de consulta
        intent = "consulta_dados"  # Padrão
        
        if "total" in question.lower() and "por" in question.lower():
            intent = "agregacao"
        elif any(word in question.lower() for word in ["onde", "qual", "quem", "quando", "como"]):
            intent = "filtragem"
        elif any(word in question.lower() for word in ["maior", "menor", "mais", "menos", "melhor", "pior"]):
            intent = "classificacao"
            
        logger.info(f"Padrão encontrado: {intent}")
        
        # Retorna uma estrutura semântica simplificada
        return {
            "intent": intent,
            "data_source": data_source,
            "parameters": {}
        }