from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uuid
import os
import sys
from typing import Optional, Dict, Any
import pandas as pd
import json
import logging

# Adiciona o diretório raiz ao PYTHONPATH para garantir importações corretas
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("api")

# Importações do sistema
from core.engine.analysis_engine import AnalysisEngine
from utils.file_manager import FileManager

app = FastAPI(title="Sistema de Consulta em Linguagem Natural API")

# Configuração de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Modificar para origens específicas em produção
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Inicialização do gerenciador de arquivos
file_manager = FileManager(base_dir="uploads")

# Dicionário para armazenar engines por ID de sessão
engines: Dict[str, AnalysisEngine] = {}

# Armazena informações da sessão do usuário
session_data: Dict[str, Dict[str, Any]] = {}

# Carrega os arquivos existentes ao iniciar a API
def initialize_engines():
    available_files = file_manager.list_available_files()
    for file_info in available_files:
        try:
            file_id = file_info["file_id"]
            if file_id not in engines:
                # Obtém o caminho do arquivo
                file_path = file_manager.get_file_path(file_id)
                if file_path and os.path.exists(file_path):
                    # Cria uma instância do motor de análise
                    engine = AnalysisEngine(
                        model_type="openai",
                        model_name="gpt-3.5-turbo",
                        api_key=os.environ.get("OPENAI_API_KEY")
                    )
                    # Carrega o arquivo no motor
                    engine.load_data(
                        data=file_path,
                        name="dataset",
                        description=file_info.get("description") or f"Dados carregados de {file_info.get('filename')}"
                    )
                    # Armazena o engine
                    engines[file_id] = engine
                    logger.info(f"Engine inicializado para arquivo {file_id}: {file_info.get('filename')}")
        except Exception as e:
            logger.error(f"Erro ao inicializar engine para arquivo {file_info.get('file_id')}: {str(e)}")

# Inicializa engines ao iniciar a aplicação
initialize_engines()

@app.post("/upload/")
async def upload_file(
    file: UploadFile = File(...),
    description: Optional[str] = Form(None)
):
    """
    Endpoint para upload de arquivo.
    Retorna um identificador único para o arquivo.
    """
    # Gera um ID único para o arquivo
    file_id = str(uuid.uuid4())
    
    # Salva o arquivo com o identificador único e descrição
    file_path = await file_manager.save_file(file, file_id, description)
    
    # Cria uma instância do motor de análise para este arquivo
    engine = AnalysisEngine(
        model_type="openai",  # Configurar com base nas variáveis de ambiente
        model_name="gpt-3.5-turbo",
        api_key=os.environ.get("OPENAI_API_KEY")
    )
    
    # Carrega o arquivo no motor de análise
    try:
        engine.load_data(
            data=file_path,
            name="dataset",
            description=description or f"Dados carregados de {file.filename}"
        )
        # Armazena o engine associado ao ID
        engines[file_id] = engine
        
        return {"file_id": file_id, "filename": file.filename, "status": "success"}
    except Exception as e:
        logger.error(f"Erro ao processar arquivo: {str(e)}")
        # Remove o arquivo em caso de erro
        await file_manager.delete_file(file_id)
        raise HTTPException(status_code=400, detail=f"Erro ao processar arquivo: {str(e)}")

@app.post("/query/")
async def process_query(
    file_id: str = Form(...),
    query: str = Form(...),
    background_tasks: BackgroundTasks = None
):
    """
    Processa uma consulta em linguagem natural sobre o arquivo.
    Retorna o resultado da consulta, uma análise e a consulta SQL executada.
    """
    # Verifica se o ID do arquivo existe
    if file_id not in engines:
        raise HTTPException(status_code=404, detail="Arquivo não encontrado")
    
    engine = engines[file_id]
    
    try:
        # Processa a consulta
        result = engine.process_query(query)
        
        # Inicializa dados da sessão se não existirem
        if file_id not in session_data:
            session_data[file_id] = {}
        
        # Armazena o último resultado e consulta na sessão
        session_data[file_id]["last_query"] = query
        session_data[file_id]["last_result"] = result
        
        # Extrai consulta SQL do código gerado
        sql_query = None
        if hasattr(engine, 'last_code_generated') and engine.last_code_generated:
            import re
            sql_matches = re.findall(r'execute_sql_query\([\'"](.+?)[\'"]\)', engine.last_code_generated)
            if sql_matches:
                sql_query = sql_matches[0]
        
        # Importa bibliotecas necessárias no escopo global
        import pandas as pd
        import numpy as np
        
        # Verifica se o usuário solicitou uma visualização
        visualization_requested = any(keyword in query.lower() for keyword 
                                    in ['gráfico', 'grafico', 'visualização', 'visualizacao', 
                                       'visualize', 'plot', 'chart', 'mostrar', 'exibir', 
                                       'desenhar', 'desenhe'])
        
        # Prepara resposta com base no tipo de resultado
        response = {
            "type": result.type,
            "query": query,
            "analysis": engine.generate_analysis(result, query),
            "sql_query": sql_query  # Adiciona a consulta SQL
        }
        
        # Se o resultado é um dataframe e o usuário pediu visualização, gera também um gráfico
        chart_result = None
        if result.type == "dataframe" and visualization_requested:
            try:
                # Tenta criar uma visualização automática
                df = result.value
                
                # Determina colunas x e y automaticamente
                x_column = None
                y_column = None
                
                # Procura a primeira coluna categórica ou de data para x
                for col in df.columns:
                    if (df[col].dtype == 'object' or 
                        pd.api.types.is_datetime64_any_dtype(df[col]) or 
                        col.lower() in ['data', 'date', 'mes', 'month', 'ano', 'year', 'period', 'período', 'categoria', 'category']):
                        x_column = col
                        break
                
                # Se ainda não encontrou, usa a primeira coluna
                if not x_column and not df.empty and df.columns.size > 0:
                    x_column = df.columns[0]
                
                # Procura a primeira coluna numérica para y
                for col in df.columns:
                    if (pd.api.types.is_numeric_dtype(df[col]) and 
                        col != x_column and 
                        col.lower() not in ['id', 'código', 'code']):
                        y_column = col
                        break
                
                # Seleciona o tipo de gráfico apropriado
                if x_column and y_column:
                    # Determina o tipo de gráfico baseado nos dados
                    unique_x_values = df[x_column].nunique()
                    is_time_series = pd.api.types.is_datetime64_any_dtype(df[x_column])
                    
                    # Decide o tipo de gráfico
                    chart_type = 'bar'  # padrão
                    if is_time_series or (unique_x_values > 10):
                        chart_type = 'line'
                    elif unique_x_values <= 10 and len(df) <= 10:
                        chart_type = 'bar'
                    
                    # Gera o título
                    chart_title = f"{y_column} por {x_column}"
                    
                    # Cria o gráfico
                    chart_result = engine.generate_chart(
                        data=df,
                        chart_type=chart_type,
                        x=x_column,
                        y=y_column,
                        title=chart_title,
                        chart_format="apex"
                    )
                    
                    # Adiciona à resposta
                    logger.info(f"Visualização automática gerada para a consulta")
            except Exception as chart_error:
                logger.warning(f"Erro ao gerar visualização automática: {str(chart_error)}")
                # Não interrompe o fluxo em caso de erro na visualização
        
        # Adiciona o valor específico baseado no tipo
        if result.type == "dataframe":
            # Limita a 25 registros para garantir desempenho
            df_limited = result.value.head(25) if len(result.value) > 25 else result.value
            # Converte DataFrame para JSON com manipulação de datas
            records = df_limited.to_dict(orient="records")
            # Converte Timestamp e outros tipos não serializáveis para strings
            for record in records:
                for key, value in record.items():
                    # Trata objetos pandas.Timestamp
                    if isinstance(value, pd.Timestamp):
                        record[key] = value.isoformat()
                    # Trata numpy.datetime64
                    elif isinstance(value, np.datetime64):
                        record[key] = pd.Timestamp(value).isoformat()
                    # Trata valores não finitos (NaN, Inf, -Inf)
                    elif isinstance(value, float) and not np.isfinite(value):
                        record[key] = None
                    # Trata valores numpy
                    elif isinstance(value, np.number):
                        record[key] = value.item()
                    # Trata outros tipos não serializáveis
                    elif not isinstance(value, (str, int, float, bool, type(None))):
                        record[key] = str(value)
            response["data"] = records
            # Indica o número total de registros na consulta original
            response["total_records"] = len(result.value)
            # Adiciona indicador de que uma visualização está disponível
            response["visualization_available"] = True
            # Adiciona indicador de que o resultado foi limitado
            response["results_limited"] = len(result.value) > 25
            
            # Adiciona visualização se foi gerada
            if chart_result:
                response["chart"] = chart_result.to_apex_json()
                response["chart_type"] = "auto_generated"
        elif result.type == "chart":
            # Retorna configuração de gráfico
            if hasattr(result, "chart_format") and result.chart_format == "apex":
                response["chart"] = result.to_apex_json()
            else:
                # Fallback para imagem ou outro formato
                response["data"] = str(result.value)
        elif result.type == "number" and visualization_requested:
            # Para números com pedido de visualização, tenta criar um gráfico simples
            try:
                # Cria um dataframe simples com o valor
                df = pd.DataFrame({"Valor": [result.value]})
                
                # Gera um gráfico de barras simples
                chart_result = engine.generate_chart(
                    data=df,
                    chart_type="bar",
                    x=df.index,
                    y="Valor",
                    title=f"Valor: {result.value}",
                    chart_format="apex"
                )
                
                # Mantém o valor original como dados primários
                response["data"] = result.value
                # Adiciona o gráfico
                response["chart"] = chart_result.to_apex_json()
                response["chart_type"] = "auto_generated"
            except Exception as chart_error:
                logger.warning(f"Erro ao gerar visualização para valor numérico: {str(chart_error)}")
                response["data"] = result.value
        else:
            # Para string e outros tipos
            response["data"] = result.value
        
        # Adiciona a consulta SQL como resultado mesmo quando não extraída do código
        if not sql_query and engine.sql_executor and sql_query is None:
            response["sql_execute_warning"] = "Consulta SQL não identificada no código gerado"
        
        return JSONResponse(content=response)
    except Exception as e:
        logger.error(f"Erro ao processar consulta: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao processar consulta: {str(e)}")

@app.post("/visualization/")
async def generate_visualization(
    file_id: str = Form(...),
    chart_type: Optional[str] = Form(None),
    x_column: Optional[str] = Form(None),
    y_column: Optional[str] = Form(None),
    title: Optional[str] = Form(None)
):
    """
    Gera uma visualização inteligente baseada na última consulta do usuário.
    Se parâmetros específicos não forem fornecidos, determina automaticamente o tipo de gráfico mais adequado.
    Retorna configuração JSON do gráfico (ApexCharts).
    """
    # Verifica se o ID do arquivo existe
    if file_id not in engines:
        raise HTTPException(status_code=404, detail="Arquivo não encontrado")
    
    # Verifica se existe uma consulta anterior
    if file_id not in session_data or "last_result" not in session_data[file_id]:
        raise HTTPException(status_code=400, detail="Nenhuma consulta anterior encontrada. Faça uma consulta primeiro.")
    
    engine = engines[file_id]
    last_result = session_data[file_id]["last_result"]
    last_query = session_data[file_id].get("last_query", "")
    
    try:
        # Se o último resultado já for um gráfico, retorna-o diretamente
        if last_result.type == "chart" and hasattr(last_result, "chart_format") and last_result.chart_format == "apex":
            return JSONResponse(content={
                "chart": last_result.to_apex_json(),
                "type": "chart",
                "query": last_query,
                "description": "Visualização da última consulta"
            })
        
        # Se o último resultado for um DataFrame, gera uma visualização
        if last_result.type == "dataframe":
            df = last_result.value
            
            # Obtém o conjunto de dados
            data = df
            
            # Determina colunas x e y automaticamente se não fornecidas
            if not x_column:
                # Tenta encontrar a primeira coluna categórica ou de data
                for col in df.columns:
                    if (df[col].dtype == 'object' or 
                        pd.api.types.is_datetime64_any_dtype(df[col]) or 
                        col.lower() in ['data', 'date', 'mes', 'month', 'ano', 'year', 'period', 'período', 'categoria', 'category']):
                        x_column = col
                        break
                
                # Se ainda não encontrou, usa a primeira coluna
                if not x_column and not df.empty and df.columns.size > 0:
                    x_column = df.columns[0]
            
            if not y_column:
                # Tenta encontrar a primeira coluna numérica
                for col in df.columns:
                    if (pd.api.types.is_numeric_dtype(df[col]) and 
                        col != x_column and 
                        col.lower() not in ['id', 'código', 'code']):
                        y_column = col
                        break
                
                # Se ainda não encontrou, usa a segunda coluna ou a primeira
                if not y_column and not df.empty:
                    if df.columns.size > 1 and df.columns[1] != x_column:
                        y_column = df.columns[1]
                    else:
                        # Fallback para a primeira coluna numérica
                        for col in df.columns:
                            if pd.api.types.is_numeric_dtype(df[col]):
                                y_column = col
                                break
            
            # Determina o tipo de gráfico mais adequado se não fornecido
            if not chart_type:
                # Analisa o conteúdo e a estrutura dos dados para sugerir um tipo de gráfico
                
                # Quantidade de valores únicos na coluna x
                unique_x_values = df[x_column].nunique() if x_column and x_column in df.columns else 0
                
                # Verifica se a coluna x é temporal
                is_time_series = (pd.api.types.is_datetime64_any_dtype(df[x_column]) if x_column and x_column in df.columns else False)
                is_date_like = (is_time_series or 
                                (x_column and x_column.lower() in ['data', 'date', 'mes', 'month', 'ano', 'year', 'dia', 'day']))
                
                # Número de colunas numéricas
                num_numeric_cols = len(df.select_dtypes(include=['number']).columns)
                
                # Análise do conteúdo da consulta para inferir intenção
                query_lower = last_query.lower()
                
                # Lógica para decidir o tipo de gráfico
                if 'pizza' in query_lower or 'pie' in query_lower or 'distribuição' in query_lower or 'distribution' in query_lower:
                    chart_type = 'pie'
                elif 'área' in query_lower or 'area' in query_lower:
                    chart_type = 'area'
                elif 'linha' in query_lower or 'line' in query_lower or 'evolução' in query_lower or 'tendência' in query_lower or 'trend' in query_lower or 'evolution' in query_lower:
                    chart_type = 'line'
                elif 'dispersão' in query_lower or 'scatter' in query_lower or 'correlação' in query_lower or 'correlation' in query_lower:
                    chart_type = 'scatter'
                elif 'calor' in query_lower or 'heat' in query_lower:
                    chart_type = 'heatmap'
                else:
                    # Decisão baseada na estrutura dos dados
                    if is_date_like and unique_x_values > 1:
                        chart_type = 'line'  # Séries temporais geralmente ficam boas em linhas
                    elif df.shape[0] <= 10:  # Poucos dados
                        if unique_x_values <= 12:
                            chart_type = 'bar'  # Para categorias pequenas, barras são mais legíveis
                        else:
                            chart_type = 'line'  # Para muitas categorias, linhas são mais compactas
                    else:  # Muitos dados
                        chart_type = 'bar'  # Padrão para exploração de dados
            
            # Define um título significativo
            if not title:
                if y_column and x_column:
                    title = f"{y_column} por {x_column}"
                    # Capitaliza a primeira letra
                    title = title[0].upper() + title[1:]
                else:
                    # Gera um título a partir da consulta
                    title = f"Visualização de {last_query[:30]}{'...' if len(last_query) > 30 else ''}"
            
            # Gera o gráfico no formato ApexCharts
            chart_response = engine.generate_chart(
                data=data,
                chart_type=chart_type,
                x=x_column,
                y=y_column,
                title=title,
                chart_format="apex"
            )
            
            # Gera uma descrição da visualização
            description = f"Visualização gerada a partir da consulta: '{last_query}'. "
            description += f"Gráfico do tipo {chart_type} mostrando {y_column} por {x_column}."
            
            # Retorna a configuração do gráfico com contexto
            return JSONResponse(content={
                "chart": chart_response.to_apex_json(),
                "type": "chart",
                "chart_type": chart_type,
                "x_column": x_column,
                "y_column": y_column,
                "query": last_query,
                "description": description
            })
        
        # Se o resultado não for um DataFrame nem um gráfico
        raise HTTPException(
            status_code=400, 
            detail="A última consulta não retornou dados que possam ser visualizados."
        )
        
    except Exception as e:
        logger.error(f"Erro ao gerar visualização: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao gerar visualização: {str(e)}")

def _generate_analysis(result, query: str) -> str:
    """
    Gera uma análise simplificada do resultado da consulta.
    """
    if result.type == "dataframe":
        df = result.value
        return f"A consulta retornou {len(df)} registros com {len(df.columns)} colunas."
    elif result.type == "chart":
        return f"Visualização gerada com base na consulta: '{query}'."
    elif result.type == "number":
        return f"O valor numérico obtido foi {result.value}."
    else:
        return f"Consulta processada com sucesso."

@app.get("/files/")
async def list_files():
    """
    Lista todos os arquivos disponíveis no sistema.
    """
    available_files = file_manager.list_available_files()
    return {"files": available_files}

@app.get("/files/{file_id}")
async def get_file_info(file_id: str):
    """
    Obtém informações detalhadas sobre um arquivo específico.
    """
    file_info = file_manager.get_file_info(file_id)
    if file_info:
        # Verifica se existe um engine carregado para este arquivo
        engine_loaded = file_id in engines
        return {**file_info, "engine_loaded": engine_loaded}
    
    raise HTTPException(status_code=404, detail="Arquivo não encontrado")

@app.post("/files/{file_id}/load")
async def load_file_engine(file_id: str):
    """
    Carrega ou recarrega um arquivo existente no engine de análise.
    """
    file_info = file_manager.get_file_info(file_id)
    if not file_info:
        raise HTTPException(status_code=404, detail="Arquivo não encontrado")
    
    try:
        # Obtém o caminho do arquivo
        file_path = file_manager.get_file_path(file_id)
        if not file_path or not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="Arquivo físico não encontrado")
        
        # Cria uma instância do motor de análise
        engine = AnalysisEngine(
            model_type="openai",
            model_name="gpt-3.5-turbo",
            api_key=os.environ.get("OPENAI_API_KEY")
        )
        
        # Carrega o arquivo no motor
        engine.load_data(
            data=file_path,
            name="dataset",
            description=file_info.get("description") or f"Dados carregados de {file_info.get('filename')}"
        )
        
        # Armazena o engine
        engines[file_id] = engine
        
        return {"status": "success", "message": "Arquivo carregado com sucesso", "file_id": file_id}
    except Exception as e:
        logger.error(f"Erro ao carregar arquivo {file_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao carregar arquivo: {str(e)}")

@app.delete("/session/{file_id}")
async def cleanup_session(file_id: str, delete_file: bool = False):
    """
    Remove os recursos associados a uma sessão.
    
    Args:
        file_id: Identificador único do arquivo
        delete_file: Se True, remove o arquivo físico. Se False, mantém o arquivo para uso futuro.
    """
    if file_id in engines:
        # Remove o engine
        del engines[file_id]
        
        # Remove dados da sessão
        if file_id in session_data:
            del session_data[file_id]
        
        # Remove o arquivo físico apenas se solicitado
        if delete_file:
            await file_manager.delete_file(file_id)
            return {"status": "success", "message": "Sessão encerrada e arquivo removido com sucesso"}
        else:
            return {"status": "success", "message": "Sessão encerrada com sucesso (arquivo mantido para uso futuro)"}
    else:
        # Verifica se o arquivo existe mesmo sem engine
        file_info = file_manager.get_file_info(file_id)
        if file_info and delete_file:
            await file_manager.delete_file(file_id)
            return {"status": "success", "message": "Arquivo removido com sucesso"}
        elif file_info:
            return {"status": "success", "message": "Engine já estava descarregado. Arquivo mantido."}
        else:
            raise HTTPException(status_code=404, detail="Sessão e arquivo não encontrados")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)