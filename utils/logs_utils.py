import pandas as pd
from datetime import datetime
import pytz
import os
from io import StringIO

from google.cloud import storage

def load_processed_log(log_path: str, bucket_name: str) -> set:
    """
    Carga la lista de archivos ya procesados desde un archivo de log en formato CSV
    ubicado en GCS.

    Args:
        log_path (str): La ruta/nombre del archivo de log dentro del bucket.
        bucket_name (str): El nombre del bucket de GCS donde se encuentra el log.

    Returns:
        set: Un conjunto de rutas de archivos ya procesados para una búsqueda eficiente.
             Retorna un conjunto vacío si el log no existe o está vacío.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(log_path)
        
        # Descargar el contenido del log como texto
        log_content = blob.download_as_text()
        
        # Leer el contenido en un DataFrame de pandas
        df = pd.read_csv(StringIO(log_content))
        
        if 'processed_file_path' in df.columns:
            # Retornar como un 'set' para búsquedas O(1)
            return set(df['processed_file_path'].tolist())
        else:
            # El archivo existe pero no tiene la columna esperada
            return set()
            
    except Exception:
        # Si el archivo no existe (primera ejecución) o hay otro error, retorna un set vacío
        return set()

def append_to_log(file_path: str, log_path: str, bucket_name: str):
    """
    Añade una nueva entrada al archivo de log CSV en GCS.
    Crea el archivo y el encabezado si no existen.

    Args:
        file_path (str): La ruta completa (gs://...) del archivo que fue procesado.
        log_path (str): La ruta/nombre del archivo de log dentro del bucket.
        bucket_name (str): El nombre del bucket de GCS.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(log_path)

    # Crear un DataFrame para la nueva entrada
    new_log_entry = {
        'processed_file_path': [file_path],
        'processing_timestamp_utc': [datetime.now(pytz.utc).isoformat()]
    }
    new_df = pd.DataFrame(new_log_entry)

    try:
        # Verificar si el archivo ya existe para decidir si añadir el header
        file_exists = blob.exists()
        
        if file_exists:
            # Si existe, descargar, añadir la nueva línea y volver a subir
            existing_content = blob.download_as_text()
            existing_df = pd.read_csv(StringIO(existing_content))
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            output_csv = combined_df.to_csv(index=False)
        else:
            # Si no existe, este es el primer registro, incluir header
            output_csv = new_df.to_csv(index=False)
        
        # Subir el contenido actualizado/nuevo al bucket
        blob.upload_from_string(output_csv, 'text/csv')

    except Exception as e:
        print(f"❌ Error al actualizar el archivo de log '{log_path}': {e}")
        raise