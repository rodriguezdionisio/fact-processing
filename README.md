# fact-processing

Este repositorio contiene un pipeline de procesamiento de datos para tablas de hechos (fact tables). El sistema procesa archivos CSV almacenados en Google Cloud Storage (GCS), aplica transformaciones específicas y guarda los datos procesados en formato Parquet, manteniendo un log de archivos procesados para evitar duplicados.

## Estructura del proyecto

```
.
├── .dockerignore
├── .gitignore
├── cloudbuild.yaml
├── Dockerfile
├── main.py
├── README.md
├── requirements.txt
├── config/
│   └── credentials.json
├── src/
│   └── processors/
│       ├── __init__.py
│       ├── sales_processor.py
│       └── sales_orders_processor.py
└── utils/
    ├── __init__.py
    ├── env_config.py
    ├── gcp_utils.py
    ├── logger.py
    └── logs_utils.py
```

## Uso

El script principal es [main.py](main.py), que ejecuta el pipeline de procesamiento de tablas de hechos.

```sh
python main.py
```

El sistema actualmente procesa las siguientes tablas de hechos:

- `fact_sales`
- `fact_sales_orders`

### Configuración por lotes

El sistema procesa archivos en lotes configurables mediante la variable `PROCESSING_BATCH_SIZE` para optimizar el rendimiento y gestión de memoria.

## Flujo de procesamiento

1. **Identificación de archivos**: Lista archivos CSV en la carpeta `raw/fact_{table}/` 
2. **Filtrado**: Excluye archivos ya procesados según el log de procesamiento
3. **Procesamiento por lotes**: Procesa un número configurable de archivos por ejecución
4. **Transformación**: Aplica las reglas de negocio específicas de cada procesador
5. **Almacenamiento**: Guarda los datos procesados en formato Parquet en `clean/fact_{table}/`
6. **Logging**: Registra los archivos procesados para evitar reprocesamiento

## Variables de entorno

Configura los siguientes valores como variables de entorno o en tu archivo `.env`:

- `GCS_BUCKET_NAME`: Nombre del bucket de Google Cloud Storage
- `GCP_PROJECT_NAME`: Nombre del proyecto de Google Cloud Platform
- `GCP_PROJECT_ID`: ID del proyecto de Google Cloud Platform
- `GOOGLE_APPLICATION_CREDENTIALS`: Ruta al archivo de credenciales de GCP
- `PROCESSING_BATCH_SIZE`: Número de archivos a procesar por lote (opcional)
- `LOG_LEVEL`: Nivel de logging (INFO, DEBUG, ERROR, etc.)

## Instalación

1. Clona el repositorio:
```bash
git clone https://github.com/rodriguezdionisio/fact-processing.git
cd fact-processing
```

2. Instala las dependencias:
```bash
pip install -r requirements.txt
```

3. Configura las variables de entorno en un archivo `.env`:
```env
GCS_BUCKET_NAME=tu-bucket-name
GCP_PROJECT_NAME=tu-proyecto-gcp
GCP_PROJECT_ID=tu-proyecto-id
GOOGLE_APPLICATION_CREDENTIALS=path/to/credentials.json
PROCESSING_BATCH_SIZE=10
LOG_LEVEL=INFO
```

## Componentes principales

- **[main.py](main.py):** Orquestador principal del pipeline de procesamiento de hechos.
- **[src/processors/sales_processor.py](src/processors/sales_processor.py):** Procesador específico para la tabla de hechos de ventas.
- **[src/processors/sales_orders_processor.py](src/processors/sales_orders_processor.py):** Procesador para órdenes de venta.
- **[utils/gcp_utils.py](utils/gcp_utils.py):** Utilidades para Google Cloud Storage (lectura/escritura de archivos).
- **[utils/logs_utils.py](utils/logs_utils.py):** Gestión de logs de archivos procesados.
- **[utils/env_config.py](utils/env_config.py):** Carga de configuración y variables de entorno.
- **[utils/logger.py](utils/logger.py):** Configuración de logging.

## Procesadores

### Sales Processor
- Limpia datos de ventas eliminando estados 'IN-COURSE' y 'PENDING'
- Renombra y elimina columnas según reglas de negocio
- Procesa fechas de creación y cierre con conversión a zona horaria Argentina
- Genera claves de fecha y tiempo para análisis temporal

### Extensibilidad
Para agregar nuevos procesadores:
1. Crear un nuevo archivo en `src/processors/`
2. Implementar la función `process(df: pd.DataFrame) -> pd.DataFrame`
3. Agregar el procesador a `FACT_PROCESSING_TASKS` en `main.py`

## Despliegue

El proyecto incluye archivos para despliegue con Docker y Google Cloud Build:

- `Dockerfile`: Configuración para crear la imagen de contenedor
- `cloudbuild.yaml`: Configuración para Google Cloud Build
- `.dockerignore`: Archivos excluidos del contexto Docker

## Notas

- Las credenciales de GCP pueden configurarse mediante variables de entorno o archivo de credenciales
- Los archivos `.env` y credenciales no se incluyen en el control de versiones por seguridad
- El proyecto está containerizado y listo para despliegue en Google Cloud
- Los logs de procesamiento se almacenan en GCS para persistencia y seguimiento
- El sistema está diseñado para ser idempotente: puede ejecutarse múltiples veces sin reprocesar archivos
