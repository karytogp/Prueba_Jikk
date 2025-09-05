# test_connection.py
import pyodbc
from urllib.parse import quote_plus

def test_sql_connection():
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=.;"
            f"DATABASE=Prueba_Sep;"
            f"UID=sa;"
            f"PWD=123;"
            f"TrustServerCertificate=yes;"
        )
        
        print("Probando conexión con:", conn_str)
        
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()
        print("✅ Conexión exitosa!")
        print("Versión:", version[0])
        conn.close()
        
    except Exception as e:
        print("❌ Error:", e)

if __name__ == "__main__":
    test_sql_connection()