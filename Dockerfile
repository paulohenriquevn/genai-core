FROM python:3.10-slim

WORKDIR /app

# Instala pacotes do sistema necessários
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copia os arquivos de requisitos primeiro para aproveitar o cache de camadas do Docker
COPY requirements.txt .

# Instala dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código da aplicação
COPY . .

# Cria diretório para uploads e garante permissões adequadas
RUN mkdir -p /app/uploads && chmod 777 /app/uploads

# Expõe a porta da aplicação
EXPOSE 8000

# Variáveis de ambiente (serão substituídas em runtime)
ENV OPENAI_API_KEY="sua_chave_aqui"

# Define comando para execução
CMD ["python", "run_api_server.py"]