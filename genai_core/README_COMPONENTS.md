# GenAI Core

Sistema modular para análise de dados com geração de SQL a partir de linguagem natural.

## Estrutura do Projeto

```
genai_core/
│
├── __init__.py
├── core.py                # Orquestra o fluxo completo NLP → SQL → Execução
│
├── nlp/
│   ├── __init__.py
│   └── nlp_processor.py   # Lida com interpretação de linguagem natural
│
├── sql/
│   ├── __init__.py
│   └── sql_generator.py   # Constrói SQL com base em estrutura semântica
│
├── data/
│   ├── __init__.py
│   ├── connectors/
│   │   ├── __init__.py
│   │   ├── csv_connector.py
│   │   ├── excel_connector.py
│   │   ├── postgres_connector.py
│   │   └── duckdb_connector.py
│
├── config/
│   ├── __init__.py
│   └── settings.py        # Gerencia variáveis de ambiente
│
└── utils/
    ├── __init__.py
    └── helpers.py         # Funções auxiliares reutilizáveis
```

## Descrição dos Módulos

### Núcleo do Sistema (core.py)

Módulo principal que orquestra todo o fluxo de processamento. Recebe consultas em linguagem natural, gerencia o ciclo completo e entrega os resultados.

Principais funcionalidades:
- Integração entre componentes NLP e SQL
- Gerenciamento de fontes de dados
- Execução de consultas em diferentes conectores
- Formatação e entrega de resultados

### Processamento de Linguagem Natural (nlp/)

Módulo responsável por interpretar consultas em linguagem natural e extrair estruturas semânticas.

Principais funcionalidades:
- Detecção de intenção da consulta
- Extração de entidades e relações
- Geração de templates SQL
- Integração com LLMs (OpenAI, Anthropic, etc.)

### Geração de SQL (sql/)

Módulo responsável por traduzir estruturas semânticas em consultas SQL executáveis.

Principais funcionalidades:
- Geração de SQL a partir de templates
- Integração com modelos especializados (SQLCoder)
- Adaptação para diferentes dialetos SQL
- Validação e otimização de consultas

### Conectores de Dados (data/connectors/)

Módulos para interagir com diferentes fontes de dados, fornecendo uma interface unificada.

Implementações atuais:
- **CSVConnector**: Lê dados de arquivos CSV
- **ExcelConnector**: Lê dados de arquivos Excel (.xls, .xlsx)
- **PostgresConnector**: Conecta com bancos de dados PostgreSQL
- **DuckDBConnector**: Uso otimizado de DuckDB para processamento de dados locais

### Configurações (config/)

Gerenciamento de configurações e variáveis de ambiente.

Principais funcionalidades:
- Carregamento de configurações de arquivos e variáveis de ambiente
- Gestão de fontes de dados
- Configurações de LLMs e outros serviços

### Utilitários (utils/)

Funções auxiliares utilizadas por todo o sistema.

Principais funcionalidades:
- Gerenciamento de logging
- Manipulação de arquivos JSON
- Inferência de tipos de dados
- Formatação de resultados

## Uso Básico

```python
from genai_core import GenAICore
from genai_core.config import Settings

# Inicializa as configurações
settings = Settings()

# Inicializa o sistema
genai = GenAICore(settings)

# Carrega uma fonte de dados
genai.load_data_source({
    "id": "vendas",
    "type": "csv",
    "path": "data/vendas.csv"
})

# Processa uma consulta em linguagem natural
result = genai.process_query("Qual é o total de vendas por região?")

print(result)
```

## Extensibilidade

O sistema foi projetado para ser facilmente extensível:

1. **Novos Conectores de Dados**: Implemente novos conectores seguindo o padrão dos existentes.
2. **Novos Modelos LLM**: Adicione suporte para novos modelos no módulo NLP.
3. **Novos Dialetos SQL**: Expanda o módulo SQL para suportar mais dialetos.

## Requisitos

- Python 3.7+
- pandas
- duckdb (opcional, para processamento otimizado)
- psycopg2 (opcional, para PostgreSQL)
- openpyxl (opcional, para Excel)
- openai/anthropic (opcional, para LLMs)