import os

# Carga .env solo en local
if os.getenv("ENV", "local") == "local":
    from dotenv import load_dotenv
    load_dotenv()

class Config:
    GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
    GCP_PROJECT_NAME = os.getenv("GCP_PROJECT_NAME")
    GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
    GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", os.getenv("GOOGLE_CREDENTIALS_PATH"))

    PROCESSING_BATCH_SIZE = int(os.getenv('PROCESSING_BATCH_SIZE', '3')) # Lee la variable de entorno 'PROCESSING_BATCH_SIZE', si no existe, usa el número elegido
config = Config()

print(f"GCP_PROJECT_ID: {config.GCP_PROJECT_ID}")
print(f"GOOGLE_APPLICATION_CREDENTIALS: {config.GOOGLE_APPLICATION_CREDENTIALS}")