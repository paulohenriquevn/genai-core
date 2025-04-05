import os
import shutil
import json
from fastapi import UploadFile
import pandas as pd
from typing import Optional, List, Dict, Any, Tuple

class FileManager:
    """
    Gerencia o armazenamento e acesso a arquivos carregados.
    """
    
    def __init__(self, base_dir: str = "uploads"):
        """
        Inicializa o gerenciador de arquivos.
        
        Args:
            base_dir: Diretório base para armazenamento
        """
        self.base_dir = base_dir
        self.metadata_path = os.path.join(base_dir, "metadata.json")
        os.makedirs(self.base_dir, exist_ok=True)
        
        # Carrega os metadados dos arquivos se existirem
        self.metadata = self._load_metadata()
    
    def _load_metadata(self) -> Dict[str, Dict[str, Any]]:
        """
        Carrega os metadados dos arquivos se o arquivo existir.
        
        Returns:
            Dicionário com os metadados dos arquivos
        """
        if os.path.exists(self.metadata_path):
            try:
                with open(self.metadata_path, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                # Se o arquivo estiver corrompido, retorna um dicionário vazio
                return {}
        return {}
    
    def _save_metadata(self) -> None:
        """
        Salva os metadados dos arquivos no disco.
        """
        with open(self.metadata_path, "w") as f:
            json.dump(self.metadata, f, indent=2)
    
    async def save_file(self, file: UploadFile, file_id: str, description: Optional[str] = None) -> str:
        """
        Salva um arquivo carregado e seus metadados.
        
        Args:
            file: Arquivo carregado
            file_id: Identificador único para o arquivo
            description: Descrição opcional do arquivo
            
        Returns:
            Caminho completo do arquivo salvo
        """
        # Cria diretório específico para o arquivo
        file_dir = os.path.join(self.base_dir, file_id)
        os.makedirs(file_dir, exist_ok=True)
        
        # Define o caminho do arquivo
        file_path = os.path.join(file_dir, file.filename)
        
        # Salva o arquivo
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        # Armazena metadados do arquivo
        self.metadata[file_id] = {
            "filename": file.filename,
            "description": description or f"Arquivo {file.filename}",
            "path": file_path,
            "content_type": file.content_type,
            "upload_date": pd.Timestamp.now().isoformat()
        }
        
        # Salva os metadados atualizados
        self._save_metadata()
        
        # Retorna o caminho completo
        return file_path
    
    async def delete_file(self, file_id: str) -> bool:
        """
        Remove um arquivo, seu diretório e seus metadados.
        
        Args:
            file_id: Identificador do arquivo
            
        Returns:
            True se removido com sucesso, False caso contrário
        """
        file_dir = os.path.join(self.base_dir, file_id)
        
        if os.path.exists(file_dir):
            shutil.rmtree(file_dir)
            
            # Remove os metadados do arquivo
            if file_id in self.metadata:
                del self.metadata[file_id]
                self._save_metadata()
                
            return True
        return False
    
    def get_file_path(self, file_id: str, filename: Optional[str] = None) -> str:
        """
        Obtém o caminho completo para um arquivo.
        
        Args:
            file_id: Identificador do arquivo
            filename: Nome do arquivo (opcional)
            
        Returns:
            Caminho completo do arquivo
        """
        # Verifica primeiro nos metadados
        if file_id in self.metadata and not filename:
            return self.metadata[file_id]["path"]
        
        file_dir = os.path.join(self.base_dir, file_id)
        
        if filename:
            return os.path.join(file_dir, filename)
        
        # Se o nome do arquivo não for fornecido, retorna o primeiro arquivo no diretório
        if os.path.exists(file_dir):
            files = os.listdir(file_dir)
            if files:
                file_path = os.path.join(file_dir, files[0])
                
                # Atualiza os metadados se necessário
                if file_id in self.metadata and "path" not in self.metadata[file_id]:
                    self.metadata[file_id]["path"] = file_path
                    self.metadata[file_id]["filename"] = files[0]
                    self._save_metadata()
                
                return file_path
        
        return ""
    
    def list_files(self, file_id: str) -> List[str]:
        """
        Lista todos os arquivos em um diretório de ID.
        
        Args:
            file_id: Identificador do diretório
            
        Returns:
            Lista de nomes de arquivos
        """
        file_dir = os.path.join(self.base_dir, file_id)
        
        if os.path.exists(file_dir):
            return os.listdir(file_dir)
        return []
    
    def list_available_files(self) -> List[Dict[str, Any]]:
        """
        Lista todos os arquivos disponíveis com seus metadados.
        
        Returns:
            Lista de dicionários com informações dos arquivos
        """
        available_files = []
        
        # Verifica se os arquivos ainda existem fisicamente
        for file_id, metadata in list(self.metadata.items()):
            file_dir = os.path.join(self.base_dir, file_id)
            
            if os.path.exists(file_dir):
                # Verifica se o arquivo ainda existe
                path = metadata.get("path", "")
                if path and os.path.exists(path):
                    available_files.append({
                        "file_id": file_id,
                        "filename": metadata.get("filename", ""),
                        "description": metadata.get("description", ""),
                        "upload_date": metadata.get("upload_date", "")
                    })
                else:
                    # Tenta encontrar o arquivo no diretório
                    files = os.listdir(file_dir)
                    if files:
                        # Atualiza o caminho nos metadados
                        new_path = os.path.join(file_dir, files[0])
                        self.metadata[file_id]["path"] = new_path
                        self.metadata[file_id]["filename"] = files[0]
                        self._save_metadata()
                        
                        available_files.append({
                            "file_id": file_id,
                            "filename": files[0],
                            "description": metadata.get("description", ""),
                            "upload_date": metadata.get("upload_date", "")
                        })
            else:
                # Limpa metadados de arquivos que não existem mais
                del self.metadata[file_id]
                self._save_metadata()
        
        return available_files
    
    def get_file_info(self, file_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtém as informações de um arquivo específico.
        
        Args:
            file_id: Identificador do arquivo
            
        Returns:
            Dicionário com informações do arquivo ou None se não encontrado
        """
        if file_id in self.metadata:
            # Verifica se o arquivo ainda existe
            path = self.metadata[file_id].get("path", "")
            if path and os.path.exists(path):
                return {
                    "file_id": file_id,
                    **self.metadata[file_id]
                }
            
            # Tenta encontrar o arquivo no diretório
            file_dir = os.path.join(self.base_dir, file_id)
            if os.path.exists(file_dir):
                files = os.listdir(file_dir)
                if files:
                    # Atualiza o caminho nos metadados
                    new_path = os.path.join(file_dir, files[0])
                    self.metadata[file_id]["path"] = new_path
                    self.metadata[file_id]["filename"] = files[0]
                    self._save_metadata()
                    
                    return {
                        "file_id": file_id,
                        **self.metadata[file_id]
                    }
        
        return None