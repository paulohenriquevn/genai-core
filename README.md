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
├── llm_integration.py              # Integração com modelos de linguagem (OpenAI, Anthropic, Hugging Face)
├── core_integration.py             # Integração entre os componentes principais
|
├── core/
│   ├── engine/
│   │   ├── analysis_engine.py      # Motor central de análise e processamento
│   │   ├── sql_executor.py         # Execução de consultas SQL
│   │   ├── dataset.py              # Gerenciamento de conjuntos de dados
│   │   └── feedback_manager.py     # Gerenciamento de feedback do usuário
│   ├── prompts/                    # Templates para interação com LLMs
│   ├── response/                   # Handlers para diferentes tipos de respostas
│   └── code_executor.py            # Executor de código Python gerado
│
├── connector/
│   ├── data_connector.py           # Interface base para conectores
│   ├── postgres_connector.py       # Conector para PostgreSQL
│   ├── duckdb_connector.py         # Conector unificado para múltiplos formatos de arquivo (CSV, Excel, etc.)
│   └── semantic_layer_schema.py    # Esquema para camada semântica
│
├── query_builders/                 # Construtores de consultas estruturadas
│
└── utils/
    ├── chart_converters.py         # Conversão para formatos de gráficos (ApexCharts)
    └── dataset_analyzer.py         # Análise de conjuntos de dados
```

## Componentes Principais

### 1. Motor de Análise (core/engine/analysis_engine.py)

O componente central que orquestra todo o fluxo:
- Recebe consultas em linguagem natural
- Gerencia a geração de código via LLMs
- Coordena a execução do código gerado
- Processa e formata os resultados

### 2. Integração com LLMs (llm_integration.py)

Responsável pela interação com modelos de linguagem:
- Suporte para múltiplos provedores (OpenAI, Anthropic, Hugging Face)
- Gera código Python com SQL para responder consultas
- Adapta prompts conforme o contexto e necessidade
- Gerencia limitações e erros dos LLMs

### 3. Executor de Código (core/code_executor.py)

Executa o código Python gerado pelo LLM:
- Ambiente seguro para execução
- Acesso aos conjuntos de dados carregados
- Tratamento de erros de execução
- Conversão de resultados para formatos estruturados

### 4. Conectores de Dados (connector/)

Fornece acesso unificado a diferentes fontes:
- **DataConnector**: Interface base
- **PostgresConnector**: Bancos PostgreSQL
- **DuckDBConnector**: Processamento unificado de arquivos (CSV, Excel, Parquet, JSON)
- Camada semântica para descrição e transformação de dados

### 5. API RESTful (api.py)

Disponibiliza os recursos do sistema via HTTP:
- Upload de arquivos
- Processamento de consultas
- Gestão de sessões
- Visualização de resultados

## Instalação

### Requisitos

- Python 3.8+
- pandas
- duckdb (para processamento otimizado de SQL)
- FastAPI e Uvicorn (para API REST)
- psycopg2 (opcional, para PostgreSQL)
- openai/anthropic (para integração com LLMs)
- matplotlib e ApexCharts (para visualizações)

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
dataset_id = nlqs.add_dataset_from_csv("vendas", "data/vendas.csv")

# Processa uma consulta em linguagem natural
response = nlqs.process_query("Quais são os 5 produtos mais vendidos?")

# Acesse os resultados
if response.type == "dataframe":
    df = response.data
    print(df)
elif response.type == "chart":
    chart = response.data
    # O chart pode ser renderizado em frontends como JSON para ApexCharts
```

## Tipos de Consultas Suportadas

O sistema suporta diversos tipos de consultas, incluindo:

- **Consultas básicas**: "Mostre todos os clientes de São Paulo"
- **Agregações**: "Qual é o total de vendas por região?"
- **Classificações**: "Quais são os 10 produtos mais vendidos?"
- **Filtros**: "Vendas acima de R$1000 nos últimos 3 meses"
- **Combinações**: "Quais clientes compraram os produtos da categoria 'Eletrônicos'?"
- **Visualizações**: "Mostre um gráfico de vendas por mês no formato de barras"
- **Análises estatísticas**: "Qual é a correlação entre preço e quantidade vendida?"
- **Previsões simples**: "Projete as vendas para os próximos 3 meses baseado no histórico"

## Fluxo de Processamento

O sistema segue um fluxo bem definido para processar consultas:

1. **Recebimento da consulta**: O usuário envia uma pergunta em linguagem natural
2. **Geração de código**: O LLM gera código Python com SQL para responder à consulta
3. **Execução segura**: O código é executado em um ambiente controlado com acesso aos datasets
4. **Processamento de resultados**: Os resultados são convertidos para o formato adequado
5. **Resposta formatada**: Tabelas, gráficos ou texto são retornados ao usuário

## Extensibilidade

O projeto foi projetado para ser facilmente extensível:

1. **Novos Conectores**: Adicione suporte a novas fontes de dados implementando a interface `DataConnector`
2. **Novos Provedores LLM**: Expanda o suporte para modelos adicionais em `llm_integration.py`
3. **Novos Tipos de Visualização**: Adicione suporte a mais visualizações em `chart_converters.py`
4. **Respostas Personalizadas**: Implemente novos tipos de resposta na pasta `core/response/`

## Contribuição

Contribuições são bem-vindas! Por favor, siga estas etapas:

1. Faça um fork do repositório
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`)
3. Faça commit das suas mudanças (`git commit -am 'Adiciona nova feature'`)
4. Envie para a branch (`git push origin feature/nova-feature`)
5. Crie um Pull Request

## Segurança

O sistema implementa várias medidas de segurança:

- Execução de código em ambiente isolado
- Validação e sanitização de entradas
- Limitação de recursos durante a execução
- Proteção contra injeção de SQL
- Acesso restrito a APIs externas

## Licença

Este projeto está licenciado sob a licença MIT - veja o arquivo LICENSE para mais detalhes.

---

**Desenvolvido por [Paulo Henrique Vieira]**