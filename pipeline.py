# pipeline.py (sin emojis para Windows)
import os
import sys
import argparse
import json
import logging
import subprocess
from datetime import datetime
from pathlib import Path

# === Rutas base ===
ROOT = Path(__file__).parent.resolve()
ENV_PATH = ROOT / ".env"

# Configurar logging sin emojis
LOGS_DIR = ROOT / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOGS_DIR / "pipeline.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)-8s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()]
)

# === Vars de entorno ===
SERVER = os.getenv("SERVER", ".")
DATABASE = os.getenv("DATABASE", "Prueba_Sep")
USERNAME = os.getenv("USERNAME", "sa")
PASSWORD = os.getenv("PASSWORD", "123")
DRIVER = os.getenv("DRIVER", "ODBC Driver 18 for SQL Server")

BACKUP_ROOT = Path(os.getenv("BACKUP_ROOT", "./backups"))
BACKUP_FMT = os.getenv("BACKUP_FORMAT", "parquet")
TABLES_ENV = [t.strip() for t in os.getenv("BACKUP_TABLES", "departments,jobs,hired_employees").split(",") if t.strip()]

# === Scripts ===
RESPALDO = ROOT / "respaldo.py"
RESTAURACION = ROOT / "restauracion.py"

# === Utilidades ===
def run_step(name: str, cmd: list[str]):
    logging.info("==> %s", name)
    logging.info("Comando: %s", " ".join([str(c) for c in cmd]))
    
    try:
        proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, encoding='utf-8')
        
        if proc.returncode != 0:
            logging.error("Paso fallo: %s (codigo=%s)", name, proc.returncode)
            if proc.stderr:
                logging.error("Error: %s", proc.stderr.strip())
            return False
        
        logging.info("Paso exitoso: %s", name)
        if proc.stdout:
            logging.info("Salida: %s", proc.stdout.strip())
        return True
        
    except Exception as e:
        logging.error("Excepcion en %s: %s", name, e)
        return False

def today_str():
    return datetime.now().strftime("%Y%m%d")

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)
    return p

def find_latest_backup_file(table: str, fmt: str) -> Path | None:
    folder = BACKUP_ROOT / fmt
    pattern = f"{table}_*.{fmt}"
    candidates = list(folder.rglob(pattern))
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]

def write_manifest(data: dict):
    manifest_dir = BACKUP_ROOT / "manifest"
    ensure_dir(manifest_dir)
    path = manifest_dir / f"manifest_{today_str()}.json"
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    logging.info("Manifest escrito: %s", path)

# === Pipeline ===
def main():
    ap = argparse.ArgumentParser(description="Pipeline: backup -> restore-check")
    ap.add_argument("--format", choices=["parquet", "avro"], default=BACKUP_FMT)
    ap.add_argument("--tables", help="Tablas separadas por coma")
    ap.add_argument("--with-restore-check", action="store_true", help="Hacer restore de prueba")
    ap.add_argument("--chunksize", type=int, default=1000)
    
    args = ap.parse_args()

    if args.tables:
        tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    else:
        tables = TABLES_ENV
        
    backup_format = args.format
    date_str = today_str()

    logging.info("=== PIPELINE INICIADO ===")
    logging.info("Tablas: %s | Formato: %s", tables, backup_format)

    # 1) Backups
    ensure_dir(BACKUP_ROOT / backup_format)

    success = run_step(
        f"Respaldo {backup_format.upper()}",
        [
            sys.executable, str(RESPALDO),
            "--format", backup_format,
            "--out", str(BACKUP_ROOT / backup_format),
            "--tables", ",".join(tables),
            "--chunksize", str(args.chunksize),
        ]
    )

    if not success:
        logging.error("Pipeline fallo en respaldo")
        sys.exit(1)

    # 2) Restauración opcional
    restore_info = None
    if args.with_restore_check:
        test_table = tables[0]
        latest_file = find_latest_backup_file(test_table, backup_format)

        if latest_file:
            restored_dir = ROOT / "restored_data"
            ensure_dir(restored_dir)
            
            restore_success = run_step(
                f"Restore de {test_table}",
                [
                    sys.executable, str(RESTAURACION),
                    "--input", str(latest_file),
                    "--format", backup_format,
                    "--output", str(restored_dir)
                ]
            )
            
            if restore_success:
                restored_files = list(restored_dir.glob("*.csv"))
                restore_info = {
                    "table": test_table,
                    "backup_file": str(latest_file),
                    "restored_files": [str(f) for f in restored_files]
                }

    # 3) Manifest
    manifest = {
        "date": date_str,
        "format": backup_format,
        "tables": tables,
        "backup_dir": str(BACKUP_ROOT / backup_format),
        "restore_check": restore_info,
        "timestamp": datetime.now().isoformat()
    }
    write_manifest(manifest)

    logging.info("=== PIPELINE COMPLETADO ===")

if __name__ == "__main__":
    main()