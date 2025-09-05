import os
import time
import logging
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/load_historical_data.log", encoding="utf-8"),
        logging.StreamHandler()
    ],
)
logger = logging.getLogger(__name__)


load_dotenv()  # si no tienes .env, usa los defaults

SERVER   = os.getenv("DB_SERVER", r"DESKTOP-4MJ4A33\SQLEXPRESS")
DATABASE = os.getenv("DB_NAME", "Prueba_Sep")
USERNAME = os.getenv("DB_USER", "sa")
PASSWORD = os.getenv("DB_PASS", "123")
DRIVER   = os.getenv("DB_DRIVER", "ODBC+Driver+17+for+SQL+Server")

DATA_DIR   = os.getenv("DATA_DIR", "data")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "1000"))

ENCODED_PASSWORD = quote_plus(PASSWORD)
DATABASE_URL = (
    f"mssql+pyodbc://{USERNAME}:{ENCODED_PASSWORD}@{SERVER}/{DATABASE}"
    f"?driver={DRIVER}"
)

def get_engine():
    """Crea y verifica un engine de SQLAlchemy."""
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,         # chequea conexión antes de usarla
        fast_executemany=True,      # acelera inserts con pyodbc
        future=True,                # API 2.0
        connect_args={"timeout": 30}
    )
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    logger.info("✅ Conexión a SQL Server OK.")
    return engine


def load_data_from_csv(file_path: str, table_name: str, engine, chunk_size: int = CHUNK_SIZE) -> bool:
    """
    Carga datos de un CSV a una tabla de SQL Server por lotes.
    """
    logger.info(f"➡️  Iniciando carga: '{file_path}' → '{table_name}' (chunk={chunk_size})")

    if not os.path.exists(file_path):
        logger.error(f"❌ Archivo no encontrado: {file_path}")
        return False

    total_rows = 0
    start = time.time()

    try:

        for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):

            chunk.to_sql(
                table_name,
                con=engine,
                if_exists="append",
                index=False,
                method="multi"   
            )
            total_rows += len(chunk)
            logger.info(f"   Lote {i+1}: {len(chunk)} filas (acumulado={total_rows})")

        elapsed = time.time() - start
        logger.info(f"✅ Carga completada '{table_name}': {total_rows} filas en {elapsed:0.1f}s")
        return True

    except Exception as e:
        logger.exception(f"💥 Error cargando '{table_name}': {e}")
        return False

if __name__ == "__main__":
    try:
        engine = get_engine()
    except Exception as e:
        logger.error(f"❌ No se pudo conectar a SQL Server: {e}")
        raise SystemExit(1)

    csv_files = {
        "departments":     os.path.join(DATA_DIR, "departments.csv"),
        "jobs":            os.path.join(DATA_DIR, "jobs.csv"),
        "hired_employees": os.path.join(DATA_DIR, "hired_employees.csv"),
    }

    for name, path in csv_files.items():
        if os.path.exists(path):
            logger.info(f"📄 {name}: {path}")
        else:
            logger.warning(f"⚠️  No existe archivo para {name}: {path}")

    ok1 = load_data_from_csv(csv_files["departments"], "departments", engine)
    ok2 = load_data_from_csv(csv_files["jobs"], "jobs", engine)
    ok3 = load_data_from_csv(csv_files["hired_employees"], "hired_employees", engine)

    if all([ok1, ok2, ok3]):
        logger.info("🎉 Histórico cargado sin errores.")
    else:
        logger.warning("⚠️ Hubo errores en la carga. Revisa el log 'logs/load_historical_data.log'.")
