from pathlib import Path
from dotenv import load_dotenv
import os

ENV_PATH = Path(__file__).parent / ".env"
ok = load_dotenv(dotenv_path=ENV_PATH)
print("DOTENV_LOADED:", ok, "from", ENV_PATH)

for k in ["SERVER", "DATABASE", "USERNAME", "PASSWORD", "DRIVER"]:
    print(f"{k} -> {os.getenv(k)}")
