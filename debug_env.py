# debug_env.py
import os
from dotenv import load_dotenv
from pathlib import Path

# Cargar .env explícitamente
ENV_PATH = Path(__file__).parent / ".env"
print(f"📁 Ruta del .env: {ENV_PATH}")

if ENV_PATH.exists():
    print("✅ Archivo .env encontrado")
    load_dotenv(dotenv_path=ENV_PATH, override=True)  # Forzar recarga
else:
    print("❌ Archivo .env NO encontrado")
    exit(1)

# Verificar todas las variables
variables = ['SERVER', 'DATABASE', 'USERNAME', 'PASSWORD', 'DRIVER']
print("\n🔍 Valores cargados desde .env:")
for var in variables:
    value = os.getenv(var)
    print(f"   {var}: {repr(value)}")