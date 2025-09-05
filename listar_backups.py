# listar_backups.py
from pathlib import Path
from datetime import datetime

def list_recent_backups():
    backup_dir = Path("./backups")
    if not backup_dir.exists():
        print("❌ No se encontró el directorio de backups")
        return
    
    print("📦 BACKUPS RECIENTES:")
    print("=" * 50)
    
    # Encontrar todos los archivos Parquet y Avro
    backup_files = list(backup_dir.rglob("*.parquet")) + list(backup_dir.rglob("*.avro"))
    
    if not backup_files:
        print("❌ No se encontraron archivos de backup")
        return
    
    # Ordenar por fecha de modificación (más recientes primero)
    backup_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    
    for file in backup_files[:10]:  # Mostrar solo los 10 más recientes
        file_time = datetime.fromtimestamp(file.stat().st_mtime)
        file_size = file.stat().st_size / 1024  # Tamaño en KB
        
        print(f"📁 {file.name}")
        print(f"   📍 Ubicación: {file}")
        print(f"   ⏰ Fecha: {file_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   📊 Tamaño: {file_size:.2f} KB")
        print(f"   🗂️  Formato: {file.suffix.upper()}")
        print("-" * 50)

if __name__ == "__main__":
    list_recent_backups()