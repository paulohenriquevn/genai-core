"""
Módulo de funções auxiliares do GenAI Core.
"""

import logging
import sys

def setup_logging(log_level="info"):
    """
    Configura o sistema de logging.
    
    Args:
        log_level: Nível de logging (debug, info, warning, error, critical)
    """
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL
    }
    
    level = level_map.get(log_level.lower(), logging.INFO)
    
    # Configura o formato e o nível do logging
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )
    
    logger = logging.getLogger("genai_core.utils.helpers")
    logger.info(f"Logging configurado com nível: {level}")
    
    return level