import pyodbc
import subprocess
import time

def test_sql_connection(server, port, instance_name):
    """Prueba conexión a SQL Server"""
    print(f"\n🔌 Probando {instance_name}...")
    print(f"   Servidor: {server}")
    print(f"   Puerto: {port}")
    
    connection_strings = [
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},{port};DATABASE=master;UID=sa;PWD=123;",
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server}\\{instance_name};DATABASE=master;UID=sa;PWD=123;",
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=localhost\\{instance_name};DATABASE=master;UID=sa;PWD=123;"
    ]
    
    for i, conn_str in enumerate(connection_strings, 1):
        print(f"   Prueba {i}: {conn_str[:80]}...")
        try:
            conn = pyodbc.connect(conn_str, timeout=10)
            cursor = conn.cursor()
            cursor.execute("SELECT @@version, @@SERVERNAME")
            result = cursor.fetchone()
            print(f"   ✅ CONEXIÓN EXITOSA!")
            print(f"   📋 Servidor: {result[1]}")
            print(f"   📋 Versión: {result[0][:60]}...")
            conn.close()
            return True
        except pyodbc.Error as e:
            print(f"   ❌ Error: {str(e)[:80]}...")
        except Exception as e:
            print(f"   ❌ Error general: {str(e)[:80]}...")
    
    return False

def check_ports():
    """Verifica qué puertos están abiertos"""
    print("🔍 Verificando puertos...")
    
    try:
        result = subprocess.run(['netstat', '-an'], capture_output=True, text=True, timeout=10)
        sql_ports = []
        
        for line in result.stdout.split('\n'):
            if any(port in line for port in ['1433', '1434', '1435']) and 'LISTENING' in line:
                sql_ports.append(line.strip())
        
        if sql_ports:
            print("✅ Puertos SQL detectados:")
            for port in sql_ports:
                print(f"   📍 {port}")
            return True
        else:
            print("❌ No se detectaron puertos SQL (1433, 1434, 1435)")
            return False
            
    except Exception as e:
        print(f"⚠️  Error verificando puertos: {e}")
        return False

def main():
    print("=" * 70)
    print("PRUEBA DE CONEXIÓN - AMBAS INSTANCIAS SQL SERVER")
    print("=" * 70)
    
    # Verificar puertos primero
    ports_ok = check_ports()
    
    if not ports_ok:
        print("\n❌ Los puertos SQL no están abiertos.")
        print("Ejecuta primero: .\\configure_both_instances.ps1 como Administrador")
        return
    
    # Probar ambas instancias
    instances = [
        {"name": "MSSQLSERVER", "server": "DESKTOP-4MJ4A33", "port": 1433, "display": "SQL Server (Default)"},
        {"name": "SQLEXPRESS", "server": "DESKTOP-4MJ4A33", "port": 1435, "display": "SQL Server Express"}
    ]
    
    print("\n" + "=" * 70)
    print("INICIANDO PRUEBAS DE CONEXIÓN")
    print("=" * 70)
    
    success_count = 0
    for instance in instances:
        if test_sql_connection(instance["server"], instance["port"], instance["name"]):
            success_count += 1
    
    print("\n" + "=" * 70)
    print("RESULTADO FINAL")
    print("=" * 70)
    
    if success_count > 0:
        print(f"✅ {success_count} de {len(instances)} instancias conectadas exitosamente!")
        print("\n💡 Para tu aplicación, usa:")
        print("   - Instancia Default: DESKTOP-4MJ4A33,1433")
        print("   - Instancia Express: DESKTOP-4MJ4A33\\SQLEXPRESS o DESKTOP-4MJ4A33,1435")
    else:
        print("❌ No se pudo conectar a ninguna instancia")
        print("\n🔧 Soluciones:")
        print(r"1. Ejecuta .\configure_both_instances.ps1 como Administrador")
        print("2. Verifica que las contraseñas sean correctas")
        print("3. Abre SQL Server Configuration Manager manualmente")

if __name__ == "__main__":
    main()