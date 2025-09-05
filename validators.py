# validators.py
from datetime import datetime

def validate_employee_data(employee_data: dict) -> list:
    """
    Valida reglas de calidad para empleados
    Returns: Lista de errores
    """
    errors = []
    
    # 1. Todos los campos obligatorios
    required_fields = ['id', 'name', 'datetime', 'department_id', 'job_id']
    for field in required_fields:
        if field not in employee_data:
            errors.append(f"Campo requerido faltante: {field}")
    
    # 2. Validar formato de fecha
    if 'datetime' in employee_data:
        try:
            datetime.fromisoformat(employee_data['datetime'].replace('Z', '+00:00'))
        except ValueError:
            errors.append("Formato de fecha inválido. Use ISO-8601")
    
    # 3. Validar que department_id exista
    # 4. Validar que job_id exista
    # ... agregar más reglas según necesidad
    
    return errors