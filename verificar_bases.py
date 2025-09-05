# verificar_bases.py
import pyodbc

def list_databases():
    try:
        # Primero conectar a master sin autocommit=False
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=.;"
            "DATABASE=master;"
            "UID=sa;"
            "PWD=123;"
            "TrustServerCertificate=yes;"
        )
        
        conn = pyodbc.connect(conn_str, autocommit=True)  # ← AUTCOMMIT IMPORTANTE
        cursor = conn.cursor()
        
        # Listar bases de datos
        print("📊 Bases de datos encontradas:")
        cursor.execute("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')")
        databases = cursor.fetchall()
        
        for db in databases:
            print(f"   ✅ {db[0]}")
            
        # Verificar si Prueba_Sep existe
        cursor.execute("SELECT name FROM sys.databases WHERE name = 'Prueba_Sep'")
        if cursor.fetchone():
            print(f"🎉 La base de datos 'Prueba_Sep' EXISTE")
        else:
            print(f"❌ La base de datos 'Prueba_Sep' NO existe")
            print("💡 Creando base de datos...")
            
            # Crear base de datos con autocommit
            cursor.execute("CREATE DATABASE Prueba_Sep")
            print("✅ Base de datos 'Prueba_Sep' creada")
            
            # Ahora crear las tablas en la nueva base de datos
            print("💡 Creando tablas en Prueba_Sep...")
            conn.close()
            
            # Reconectar a la nueva base de datos
            conn_str_prueba = (
                "DRIVER={ODBC Driver 18 for SQL Server};"
                "SERVER=.;"
                "DATABASE=Prueba_Sep;"
                "UID=sa;"
                "PWD=123;"
                "TrustServerCertificate=yes;"
            )
            
            conn_prueba = pyodbc.connect(conn_str_prueba, autocommit=True)
            cursor_prueba = conn_prueba.cursor()
            
            # Crear tabla departments
            cursor_prueba.execute("""
            CREATE TABLE departments (
                id INT PRIMARY KEY,
                name NVARCHAR(255) NOT NULL
            )
            """)
            
            # Crear tabla jobs
            cursor_prueba.execute("""
            CREATE TABLE jobs (
                id INT PRIMARY KEY,
                name NVARCHAR(255) NOT NULL
            )
            """)
            
            # Crear tabla hired_employees
            cursor_prueba.execute("""
            CREATE TABLE hired_employees (
                id INT PRIMARY KEY,
                name NVARCHAR(255) NOT NULL,
                datetime NVARCHAR(50) NOT NULL,
                department_id INT,
                job_id INT,
                FOREIGN KEY (department_id) REFERENCES departments(id),
                FOREIGN KEY (job_id) REFERENCES jobs(id)
            )
            """)
            
            print("✅ Tablas creadas:")
            print("   - departments")
            print("   - jobs")
            print("   - hired_employees")
            
            # Insertar datos de ejemplo
            cursor_prueba.execute("INSERT INTO departments (id, name) VALUES (1, 'IT'), (2, 'HR'), (3, 'Finance')")
            cursor_prueba.execute("INSERT INTO jobs (id, name) VALUES (1, 'Developer'), (2, 'Manager'), (3, 'Analyst')")
            cursor_prueba.execute("""
            INSERT INTO hired_employees (id, name, datetime, department_id, job_id) 
            VALUES 
            (1, 'Juan Perez', '2023-01-15T08:30:00', 1, 1),
            (2, 'Maria Garcia', '2023-02-20T09:15:00', 2, 2),
            (3, 'Carlos Lopez', '2023-03-10T10:00:00', 3, 3)
            """)
            
            print("✅ Datos de ejemplo insertados")
            conn_prueba.close()
            
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    list_databases()