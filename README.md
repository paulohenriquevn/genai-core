# GenAI Core

Sistema modular para análise de dados com geração de código Python/SQL a partir de linguagem natural.

## Visão Geral

O GenAI Core é um sistema que permite analisar dados através de consultas em linguagem natural, eliminando a necessidade de conhecimentos técnicos em SQL ou programação. O sistema utiliza modelos de linguagem para gerar código Python com SQL que é executado nas fontes de dados conectadas.

### Principais Características

- **Processamento de Linguagem Natural**: Converte consultas em linguagem natural em código Python/SQL executável
- **Suporte a Múltiplas Fontes de Dados**: Integração com CSV, PostgreSQL e outras fontes
- **Processamento Otimizado**: Uso de DuckDB para consultas rápidas em arquivos locais
- **Arquitetura Modular**: Fácil extensão e personalização de componentes
- **Integração com Modelos de IA**: Utiliza LLMs para geração de código
- **Visualização de Dados**: Geração automática de gráficos e tabelas
- **API REST**: Interface para integração com outros sistemas

## Estrutura do Projeto

O projeto segue uma arquitetura modular, com responsabilidades bem definidas:

```
/
├── api.py                          # API RESTful com FastAPI
├── natural_language_query_system.py # Interface principal para consultas em linguagem natural
├── run_api_server.py               # Script para iniciar o servidor API
|
├── genai_core/                     # Módulo principal do sistema
│   ├── __init__.py                 # Inicialização do módulo
│   ├── core.py                     # Classes principais: GenAICore e QueryEngine
│   ├── config/                     # Configurações do sistema
│   │   └── settings.py             # Configurações globais
│   |
│   ├── data/                       # Manipulação de dados
│   │   ├── connectors/             # Conectores para fontes de dados
│   │   │   ├── data_connector.py   # Interface base para conectores
│   │   │   ├── data_connector_factory.py # Fábrica de conectores
│   │   │   ├── duckdb_connector.py # Conector para CSV, Excel, etc. via DuckDB
│   │   │   ├── postgres_connector.py # Conector para PostgreSQL
│   │   │   └── test_data_provider.py # Provedor de dados para testes
│   │   └── ...
│   |
│   ├── nlp/                        # Processamento de linguagem natural
│   │   ├── nlp_processor.py        # Processador de linguagem natural
│   │   └── mock_processor.py       # Implementação simulada para testes
│   |
│   ├── sql/                        # Geração e execução de SQL
│   │   └── sql_generator.py        # Gerador de consultas SQL
│   |
│   └── utils/                      # Utilitários diversos
│       └── helpers.py              # Funções auxiliares
|
└── tests/                          # Testes automatizados
    └── ...                         # Diversos testes do sistema
```

## Componentes Principais

### 1. GenAICore e QueryEngine (genai_core/core.py)

- **GenAICore**: Classe principal que gerencia fontes de dados e orquestra o sistema
- **QueryEngine**: Motor de consultas que integra NLP, SQL e execução

### 2. Conectores de Dados (genai_core/data/connectors/)

- **DataConnector**: Interface base para todos os conectores
- **DuckDBConnector**: Conector unificado para diversos formatos de arquivo
- **PostgresConnector**: Conector para bancos PostgreSQL
- **DataConnectorFactory**: Fábrica para criação dinâmica de conectores

### 3. NLP e SQL (genai_core/nlp/ e genai_core/sql/)

- **NLPProcessor**: Responsável por processar linguagem natural
- **SQLGenerator**: Gera consultas SQL a partir da estrutura semântica

### 4. Interface Simplificada (natural_language_query_system.py)

- **NaturalLanguageQuerySystem**: Fachada para interação com o sistema

## Instalação

### Requisitos

- Python 3.8+
- pandas
- duckdb (para processamento otimizado de SQL)
- FastAPI e Uvicorn (para API REST)
- psycopg2 (opcional, para PostgreSQL)
- openai/anthropic (para integração com LLMs)

### Passos

1. Clone o repositório
```bash
git clone https://github.com/seu-usuario/genai-core.git
cd genai-core
```

2. Instale as dependências
```bash
pip install -r requirements.txt
```

3. Configure as variáveis de ambiente
```bash
# Defina suas chaves de API para LLMs
export OPENAI_API_KEY=sua-chave-api
# ou
export ANTHROPIC_API_KEY=sua-chave-api
```

## Uso Básico

### Como API REST

Inicie o servidor API:

```bash
python run_api_server.py
```

Acesse a API em `http://localhost:8000` e utilize os endpoints:

- `POST /query`: Processa uma consulta em linguagem natural
- `POST /upload`: Carrega um arquivo de dados
- `GET /datasets`: Lista os conjuntos de dados disponíveis

### Como Biblioteca

```python
from natural_language_query_system import NaturalLanguageQuerySystem

# Inicializa o sistema
nlqs = NaturalLanguageQuerySystem()

# Carrega um arquivo CSV
nlqs.load_data("data/vendas.csv", "vendas")

# Processa uma consulta em linguagem natural
result = nlqs.ask("Quais são os 5 produtos mais vendidos?")

# Acesse os resultados
if result["success"]:
    data = result["data"]["data"]
    print(data)
```

## Consultas Exemplo

O sistema suporta diversos tipos de consultas, incluindo:

- "Mostre todos os clientes de São Paulo"
- "Qual é o total de vendas por região?"
- "Quais são os 10 produtos mais vendidos?"
- "Vendas acima de R$1000 nos últimos 3 meses"
- "Quais clientes compraram os produtos da categoria 'Eletrônicos'?"
- "Mostre um gráfico de vendas por mês"

## Desenvolvimento

### Executando Testes

```bash
python test_query_engine.py
```

### Adicionando Novos Conectores

Para adicionar suporte a uma nova fonte de dados:

1. Implemente a interface `DataConnector` em um novo arquivo
2. Registre o conector na fábrica `DataConnectorFactory`

```python
# Registrar um novo conector
from genai_core.data.connectors import DataConnectorFactory
DataConnectorFactory.register_connector('meu_tipo', ('caminho.para.modulo', 'MinhaClasse'))
```

## Licença

Este projeto está licenciado sob a licença MIT - veja o arquivo LICENSE para mais detalhes.

---

**Desenvolvido por [Paulo Henrique Vieira]**