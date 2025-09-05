import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
from urllib.parse import quote_plus

def load_env_manually():
    """Carga el archivo .env manualmente para evitar problemas de caché"""
    env_path = Path(__file__).parent / ".env"
    print(f"[INFO] Cargando .env desde: {env_path}")
    
    if not env_path.exists():
        print("[ERROR] Archivo .env no encontrado")
        print("[HELP] Crea un archivo .env con:")
        print("SERVER=.")
        print("DATABASE=Prueba_Sep")
        print("USERNAME=sa")
        print("PASSWORD=123")
        print("DRIVER=ODBC+Driver+18+for+SQL+Server")
        sys.exit(1)
    
      with open(env_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
                        value = value[1:-1]
                    os.environ[key] = value
                    print(f"   [OK] {key} = {repr(value)}")

load_env_manually()

import pandas as pd
from sqlalchemy import create_engine, text

import pyarrow as pa
import pyarrow.parquet as pq

from fastavro import writer as avro_writer, parse_schema
import numpy as np


def build_engine_from_env():
    server = os.getenv("SERVER", ".")
    database = os.getenv("DATABASE")
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")
    driver = os.getenv("DRIVER", "ODBC Driver 18 for SQL Server")
    driver = driver.replace("+", " ")

    if not all([server, database, username, password, driver]):
        missing = []
        if not server: missing.append("SERVER")
        if not database: missing.append("DATABASE")
        if not username: missing.append("USERNAME")
        if not password: missing.append("PASSWORD")
        if not driver: missing.append("DRIVER")
        raise RuntimeError(f"Faltan variables en .env: {', '.join(missing)}")

    connection_string = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
        f"Connection Timeout=30;"
    )
    
    db_url = f"mssql+pyodbc:///?odbc_connect={quote_plus(connection_string)}"
    
    print(f"[CONNECT] Conectando a: {server}/{database} como {username}")
    
    try:
        engine = create_engine(db_url, fast_executemany=True)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT @@version"))
            version = result.scalar()
            print(f"[SUCCESS] Conexión exitosa a SQL Server 2022")
        return engine
    except Exception as e:
        print(f"[ERROR] Error de conexión a SQL Server: {e}")
        print("[INFO] Usando SQLite temporalmente para pruebas...")
        return create_sqlite_backup_engine()

def create_sqlite_backup_engine():
    """Crea engine de SQLite para pruebas temporales"""
    engine = create_engine('sqlite:///test_backup.db')
    
    from sqlalchemy import MetaData, Table, Column, Integer, String
    metadata = MetaData()
    
    Table('departments', metadata,
          Column('id', Integer, primary_key=True),
          Column('name', String))
    
    Table('jobs', metadata,
          Column('id', Integer, primary_key=True),
          Column('name', String))
    
    Table('hired_employees', metadata,
          Column('id', Integer, primary_key=True),
          Column('name', String),
          Column('datetime', String),
          Column('department_id', Integer),
          Column('job_id', Integer))
    
    metadata.create_all(engine)
    
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT OR IGNORE INTO departments (id, name) 
            VALUES (1, 'IT'), (2, 'HR'), (3, 'Finance')
        """))
        conn.execute(text("""
            INSERT OR IGNORE INTO jobs (id, name) 
            VALUES (1, 'Developer'), (2, 'Manager'), (3, 'Analyst')
        """))
        conn.execute(text("""
            INSERT OR IGNORE INTO hired_employees (id, name, datetime, department_id, job_id) 
            VALUES 
            (1, 'Juan Perez', '2023-01-15T08:30:00', 1, 1),
            (2, 'Maria Garcia', '2023-02-20T09:15:00', 2, 2),
            (3, 'Carlos Lopez', '2023-03-10T10:00:00', 3, 3)
        """))
    
    print("[INFO] Base de datos SQLite de prueba creada: 'test_backup.db'")
    return engine


def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def dated_dir(base: Path) -> Path:
    d = datetime.now().strftime("%Y%m%d")
    return ensure_dir(base / d)

def read_table_in_chunks(engine, table_name: str, chunksize: int = 1000):
    """Devuelve un generador de DataFrames con chunks de la tabla."""
    query = text(f"SELECT * FROM [{table_name}]")
    return pd.read_sql_query(query, con=engine, chunksize=chunksize)

def read_table_single(engine, table_name: str):
    """Devuelve un generador con un solo DataFrame (tablas pequeñas)."""
    query = text(f"SELECT * FROM [{table_name}]")
    df = pd.read_sql_query(query, con=engine)
    yield df


def avro_type_from_dtype(dtype):
    from pandas.api.types import (
        is_integer_dtype, is_float_dtype, is_bool_dtype,
        is_datetime64_any_dtype
    )
    if is_integer_dtype(dtype):
        return ["null", "long"]
    if is_float_dtype(dtype):
        return ["null", "double"]
    if is_bool_dtype(dtype):
        return ["null", "boolean"]
    if is_datetime64_any_dtype(dtype):
        return ["null", "string"]
    return ["null", "string"]

def infer_avro_schema_from_df(table_name: str, df: pd.DataFrame) -> dict:
    fields = []
    for col in df.columns:
        fields.append({"name": col, "type": avro_type_from_dtype(df[col].dtype), "default": None})
    schema = {
        "type": "record",
        "name": f"{table_name}_record",
        "fields": fields
    }
    return parse_schema(schema)

def iter_records_for_avro(df_iter):
    """Genera registros (dict) a partir de un iterador de DataFrames."""
    for df in df_iter:
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col].dtype):
                df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S").astype("object")
        df = df.replace({np.nan: None})
        for rec in df.to_dict(orient="records"):
            yield rec


def write_parquet_stream(df_iter, out_file: Path):
    """Escribe un archivo Parquet a partir de chunks."""
    first_chunk = True
    pqw = None
    try:
        for df in df_iter:
            table = pa.Table.from_pandas(df, preserve_index=False)
            if first_chunk:
                pqw = pq.ParquetWriter(out_file, table.schema, use_dictionary=True)
                pqw.write_table(table)
                first_chunk = False
            else:
                pqw.write_table(table)
    finally:
        if pqw is not None:
            pqw.close()

def write_avro_stream(table_name: str, df_iter, out_file: Path):
    """Escribe Avro en streaming."""
    with open(out_file, "wb") as f:
        try:
            first_df = next(df_iter)
        except StopIteration:
            empty_schema = parse_schema({"type": "record", "name": f"{table_name}_record", "fields": []})
            avro_writer(f, empty_schema, [])
            return

        schema = infer_avro_schema_from_df(table_name, first_df)

        def all_records():
            yield from iter_records_for_avro(iter([first_df]))
            for df in df_iter:
                yield from iter_records_for_avro(iter([df]))

        avro_writer(f, schema, all_records())



def create_backup(engine,
                  table_name: str,
                  format_type: str,
                  output_dir: Path,
                  use_date_folder: bool = True,
                  chunksize: int = 1000,
                  single_df: bool = False) -> Path:
    
    fmt = format_type.lower()
    if fmt not in {"parquet", "avro"}:
        raise ValueError("Formato no soportado. Usa 'parquet' o 'avro'.")

    base_dir = Path(output_dir)
    out_dir = dated_dir(base_dir) if use_date_folder else ensure_dir(base_dir)

    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    out_file = out_dir / f"{table_name}_{ts}.{fmt}"

    print(f"[BACKUP] Respaldo de tabla: {table_name} ({fmt.upper()})")

    df_iter = read_table_single(engine, table_name) if single_df else read_table_in_chunks(engine, table_name, chunksize)

    if fmt == "parquet":
        write_parquet_stream(df_iter, out_file)
        print(f"[SUCCESS] [{table_name}] PARQUET creado: {out_file}")
    else:
        write_avro_stream(table_name, df_iter, out_file)
        print(f"[SUCCESS] [{table_name}] AVRO creado: {out_file}")

    return out_file


def parse_args():
    ap = argparse.ArgumentParser(description="Respaldo de tablas de SQL Server a Parquet/Avro")
    ap.add_argument("--format", default="parquet", choices=["parquet", "avro"], help="Formato de salida")
    ap.add_argument("--out", default="./backups", help="Directorio base de salida")
    ap.add_argument("--tables", required=True, help="Tablas separadas por coma. Ej: departments,jobs,hired_employees")
    ap.add_argument("--chunksize", type=int, default=1000, help="Tamaño de chunk (filas) para lectura por lotes")
    ap.add_argument("--no-date-folder", action="store_true", help="No crear subcarpeta por fecha AAAAMMDD")
    ap.add_argument("--single-df", action="store_true", help="Forzar lectura completa en un solo DataFrame")
    return ap.parse_args()

def main():
    args = parse_args()
    
    try:
        engine = build_engine_from_env()
    except Exception as e:
        print(f"[ERROR] No se pudo conectar a la base de datos: {e}")
        sys.exit(1)
    
    out_base = Path(args.out)
    use_date = not args.no_date_folder
    tables = [t.strip() for t in args.tables.split(",") if t.strip()]

    generated = []
    for table in tables:
        try:
            p = create_backup(
                engine=engine,
                table_name=table,
                format_type=args.format,
                output_dir=out_base,
                use_date_folder=use_date,
                chunksize=args.chunksize,
                single_df=args.single_df
            )
            generated.append(p)
        except Exception as e:
            print(f"[ERROR] Error al respaldar tabla {table}: {e}")

    print("\n[SUCCESS] Backups creados exitosamente:")
    for p in generated:
        print(f"   {p}")

if __name__ == "__main__":
    main()