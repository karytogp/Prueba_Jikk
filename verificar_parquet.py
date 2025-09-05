import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

def check_parquet_files():
    backup_dir = Path("./backups")
    parquet_files = list(backup_dir.rglob("*.parquet"))
    
    print("📊 Archivos Parquet encontrados:")
    for file in parquet_files:
        print(f"\n🔍 Verificando: {file}")
        try:
            table = pq.read_table(file)
            df = table.to_pandas()
            
            print(f"   ✅ Formato válido")
            print(f"   📋 Filas: {len(df)}")
            print(f"   🏷️  Columnas: {list(df.columns)}")
            print(f"   📝 Primeras filas:")
            print(df.head(3).to_string(index=False))
            
        except Exception as e:
            print(f"   ❌ Error leyendo archivo: {e}")

if __name__ == "__main__":
    check_parquet_files()