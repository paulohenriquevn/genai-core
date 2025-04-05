#!/bin/bash

# Script para executar testes do GenAI Core
# Este script verifica a estrutura do projeto e executa os testes

echo "========================================"
echo "     TESTE DO GENAI CORE"
echo "========================================"

# Verifica a estrutura de diretórios
echo -e "\n[1] Verificando a estrutura de diretórios..."
if [ -d "genai_core" ] && [ -f "genai_core/core.py" ]; then
    echo "✅ Estrutura de diretórios principal OK"
    
    # Verifica cada subdiretório
    SUBDIRS=("nlp" "sql" "data/connectors" "config" "utils")
    for dir in "${SUBDIRS[@]}"; do
        if [ -d "genai_core/$dir" ]; then
            echo "✅ Diretório genai_core/$dir existe"
        else
            echo "❌ Diretório genai_core/$dir NÃO existe"
        fi
    done
else
    echo "❌ Estrutura de diretórios principal inválida"
    exit 1
fi

# Verifica arquivos principais
echo -e "\n[2] Verificando arquivos principais..."
MAIN_FILES=("genai_core/__init__.py" "genai_core/core.py" "genai_core/nlp/nlp_processor.py" "genai_core/sql/sql_generator.py")
for file in "${MAIN_FILES[@]}"; do
    if [ -f "$file" ]; then
        echo "✅ Arquivo $file existe"
    else
        echo "❌ Arquivo $file NÃO existe"
    fi
done

# Executa o script de teste
echo -e "\n[3] Executando script de teste..."
if [ -f "test_genai_core.py" ]; then
    # Torna o script executável
    chmod +x test_genai_core.py
    
    # Executa o script
    python test_genai_core.py
    
    if [ $? -eq 0 ]; then
        echo "✅ Teste executado com sucesso"
    else
        echo "❌ Teste falhou"
        exit 1
    fi
else
    echo "❌ Script de teste não encontrado"
    exit 1
fi

echo -e "\n========================================"
echo "     TESTE CONCLUÍDO COM SUCESSO!"
echo "========================================"