#!/usr/bin/env python3
"""
Servidor API para o Sistema de Consulta em Linguagem Natural
"""

import os
import uvicorn
import argparse
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

def main():
    parser = argparse.ArgumentParser(description="Servidor API para Sistema de Consulta em Linguagem Natural")
    parser.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"), 
                        help="Endereço de host (padrão: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8000)), 
                        help="Porta (padrão: 8000)")
    parser.add_argument("--reload", action="store_true", 
                        help="Ativar recarga automática do código")
    parser.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "info").lower(), 
                        help="Nível de log (debug, info, warning, error, critical)")
    
    args = parser.parse_args()
    
    # Determina o ambiente
    environment = os.environ.get("ENVIRONMENT", "production").lower()
    is_dev = environment == "development" or args.reload
    
    print(f"Iniciando servidor API em {args.host}:{args.port}")
    print(f"Ambiente: {'Desenvolvimento' if is_dev else 'Produção'}")
    print(f"Nível de log: {args.log_level}")
    
    # Verifica se as chaves API necessárias estão configuradas
    api_key = os.environ.get("OPENAI_API_KEY") or os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("AVISO: Nenhuma chave API (OPENAI_API_KEY ou ANTHROPIC_API_KEY) configurada.")
        print("O sistema usará o modo simulado (mock) para LLM, que não produzirá resultados reais.")
    
    # Verificar diretório de uploads
    uploads_dir = os.environ.get("UPLOADS_DIR", "uploads")
    os.makedirs(uploads_dir, exist_ok=True)
    print(f"Diretório de uploads: {os.path.abspath(uploads_dir)}")
    
    # Inicia o servidor
    uvicorn.run(
        "api:app", 
        host=args.host, 
        port=args.port, 
        reload=is_dev,
        log_level=args.log_level
    )

if __name__ == "__main__":
    main()