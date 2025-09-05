# debug_connection.py
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import pyodbc

def test_all_connections():
    print("=== PRUEBA DE CONEXIÓN DIRECTA CON PYODBC ===")
    try:
        conn_str = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=.;DATABASE=Prueba_Sep;UID=sa;PWD=123;TrustServerCertificate=yes;"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        print("✅ PyODBC directo: FUNCIONA")
        conn.close()
    except Exception as e:
        print(f"❌ PyODBC directo: ERROR - {e}")

    print("\n=== PRUEBA DE SQLALCHEMY CON DIFERENTES FORMATOS ===")

    try:
        engine = create_engine("mssql+pyodbc://sa:123@./Prueba_Sep?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes")
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ SQLAlchemy Formato 1: FUNCIONA")
    except Exception as e:
        print(f"❌ SQLAlchemy Formato 1: ERROR - {e}")

    try:
        odbc_str = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=.;DATABASE=Prueba_Sep;UID=sa;PWD=123;TrustServerCertificate=yes;"
        db_url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"
        engine = create_engine(db_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ SQLAlchemy Formato 2: FUNCIONA")
    except Exception as e:
        print(f"❌ SQLAlchemy Formato 2: ERROR - {e}")

if __name__ == "__main__":
    test_all_connections()