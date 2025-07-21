import os
from utils import gcp_utils, logs_utils
from utils.env_config import config
from utils.logger import get_logger

# Módulos de procesamiento para cada tabla de hecho
from src.processors import sales_processor, sales_orders_processor

logger = get_logger(__name__)

# --------------------------------------------------------------------------------
# 1. CENTRALIZACIÓN DE TAREAS DE HECHOS
# --------------------------------------------------------------------------------
FACT_PROCESSING_TASKS = [
    #{"name": "sales", "processor_func": sales_processor.process, "log_file": "logs/processed_sales_log.txt"},
    {"name": "sales_orders", "processor_func": sales_orders_processor.process, "log_file": "logs/processed_sales_orders_log.txt"}
]

# --------------------------------------------------------------------------------
# 2. LÓGICA REUTILIZABLE PARA PROCESAR UNA TABLA DE HECHOS
#    Esta función encapsula la lógica para procesar todos los archivos nuevos
#    de una tabla de hechos.
# --------------------------------------------------------------------------------
def run_fact_processing_task(fact_name: str, process_function, log_path: str):
    """
    Ejecuta el pipeline para un lote de archivos nuevos de una tabla de hechos.
    """
    logger.info(f"--- Iniciando procesamiento para la tabla de hechos: '{fact_name}' ---")
    
    try:
        raw_folder_prefix = f"raw/fact_{fact_name}/"
        all_files = gcp_utils.list_gcs_files(prefix=raw_folder_prefix)
        
        processed_files = logs_utils.load_processed_log(log_path, config.GCS_BUCKET_NAME)
        
        files_to_process = [
            f"gs://{config.GCS_BUCKET_NAME}/{f}" 
            for f in all_files 
            if f.endswith(".csv") and f"gs://{config.GCS_BUCKET_NAME}/{f}" not in processed_files
        ]

        if not files_to_process:
            logger.info(f"No se encontraron archivos nuevos para procesar en '{raw_folder_prefix}'.")
            return True, 0

        logger.info(f"Se encontraron {len(files_to_process)} archivos nuevos en total.")
        
        batch_size = config.PROCESSING_BATCH_SIZE
        files_for_this_run = files_to_process[:batch_size]
        
        logger.info(f"Se procesará un lote de hasta {len(files_for_this_run)} archivos (configuración de lote: {batch_size}).")

        processed_count = 0
        for file_path in files_for_this_run:
            try:
                logger.info(f"Procesando archivo: {file_path}")
                
                path_parts = file_path.split('/')
                date_partition = [part for part in path_parts if 'date=' in part][0]
                file_name = path_parts[-1]
                
                raw_df = gcp_utils.read_csv_from_gcs(file_path)
                logger.info(f"Aplicando la función de procesamiento: {process_function.__module__}")
                clean_df = process_function(raw_df)

                destination_path = f"gs://{config.GCS_BUCKET_NAME}/clean/fact_{fact_name}/{date_partition}/{file_name.replace('.csv', '.parquet')}"
                gcp_utils.write_parquet_to_gcs(clean_df, destination_path)
                
                logs_utils.append_to_log(file_path, log_path, config.GCS_BUCKET_NAME)
                
                logger.info(f"Archivo procesado y guardado exitosamente en {destination_path}.")
                processed_count += 1

            except Exception as e:
                logger.error(f"ERROR al procesar el archivo '{file_path}': {e}", exc_info=True)
                continue 

        logger.info(f"Finalizó el lote. Se procesaron {processed_count} de {len(files_for_this_run)} archivos para '{fact_name}'.")
        return True, processed_count

    except Exception as e:
        logger.error(f"ERROR CRÍTICO en la tarea de procesamiento de '{fact_name}': {e}", exc_info=True)
        return False, 0

# --------------------------------------------------------------------------------
# 3. ORQUESTADOR PRINCIPAL
#    Itera sobre la lista de tareas y ejecuta cada una.
# --------------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("--- INICIANDO PIPELINE DE PROCESAMIENTO DE TABLAS DE HECHOS ---")
    
    total_success_tasks = 0
    total_failed_tasks = 0

    for task in FACT_PROCESSING_TASKS:
        fact_name = task["name"]
        processor = task["processor_func"]
        log_file = task["log_file"]
        
        success, files_processed = run_fact_processing_task(fact_name, processor, log_file)
        
        if success:
            total_success_tasks += 1
        else:
            total_failed_tasks += 1

    logger.info("--- PIPELINE DE HECHOS FINALIZADO ---")
    logger.info(f"Resumen: {total_success_tasks} tareas de hechos exitosas, {total_failed_tasks} tareas fallidas.")