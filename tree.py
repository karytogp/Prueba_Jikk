# tree.py  — imprime la estructura del proyecto con fallback ASCII
import os
import sys

# Excluye carpetas ruidosas
EXCLUDE = {'.venv', '__pycache__', '.git', 'backups', 'logs', '.idea', '.vscode'}

def supports_unicode() -> bool:
    enc = sys.stdout.encoding or 'utf-8'
    try:
        "│└├".encode(enc)
        return True
    except Exception:
        return False

if supports_unicode():
    BR = {'mid': '├── ', 'end': '└── ', 'pipe': '│   ', 'space': '    '}
else:
    # Fallback 100% ASCII
    BR = {'mid': '+-- ', 'end': '`-- ', 'pipe': '|   ', 'space': '    '}

def print_tree(path: str, prefix: str = ""):
    try:
        entries = [e for e in os.scandir(path) if e.name not in EXCLUDE]
    except PermissionError:
        print(prefix + BR['end'] + '[PERMISSION DENIED]')
        return
    entries.sort(key=lambda e: (not e.is_dir(), e.name.lower()))
    for i, e in enumerate(entries):
        connector = BR['end'] if i == len(entries) - 1 else BR['mid']
        print(prefix + connector + e.name)
        if e.is_dir():
            new_prefix = prefix + (BR['space'] if i == len(entries) - 1 else BR['pipe'])
            print_tree(e.path, new_prefix)

if __name__ == "__main__":
    root = os.path.abspath(".")
    print(os.path.basename(root) or root)
    print_tree(".")
