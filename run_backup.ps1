$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

New-Item -ItemType Directory -Force -Path "$root\logs" | Out-Null
New-Item -ItemType Directory -Force -Path "$root\backups\parquet" | Out-Null

# Usamos el venv si existe; si no, usamos "python" del sistema
$venvPy = Join-Path $root ".venv\Scripts\python.exe"
if (Test-Path $venvPy) {
  cmd /c ".venv\Scripts\activate.bat && python respaldo.py --format parquet --out `"$root\backups\parquet`" --tables departments,jobs,hired_employees >> `"$root\logs\respaldo_task.log`" 2>&1"
} else {
  python respaldo.py --format parquet --out "$root\backups\parquet" --tables departments,jobs,hired_employees >> "$root\logs\respaldo_task.log" 2>&1
}
