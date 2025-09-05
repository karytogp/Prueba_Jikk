# app.py
# FastAPI para ingesta/consulta en línea con SQL Server + JWT (versión final estable)

import os
import logging
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt, JWTError
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv
from urllib.parse import quote_plus
from passlib.context import CryptContext


ENV_PATH = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=True)

SERVER   = os.getenv("SERVER", ".")
DATABASE = os.getenv("DATABASE", "Prueba_Sep")
USERNAME = os.getenv("USERNAME", "sa")
PASSWORD = os.getenv("PASSWORD", "123")
DRIVER   = (os.getenv("DRIVER") or "ODBC Driver 18 for SQL Server").replace("+", " ")
ENCRYPT  = os.getenv("ENCRYPT", "no")
TRUST    = os.getenv("TRUST_SERVER_CERT", "yes")
TIMEOUT  = os.getenv("CONNECTION_TIMEOUT", "30")

SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-change-me-in-production")
ALGORITHM  = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))

API_USER = os.getenv("API_USER", "admin")
API_PASS = os.getenv("API_PASS", "admin123")
pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")
API_PASS_HASH = pwd_context.hash(API_PASS) if API_PASS else pwd_context.hash("admin123")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
logger.info(
    "DB target => server='%s', db='%s', user='%s', driver='%s', encrypt='%s', trust='%s'",
    SERVER, DATABASE, USERNAME, DRIVER, ENCRYPT, TRUST
)


def build_engine() -> Engine:
    odbc = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        f"Encrypt={ENCRYPT};"
        f"TrustServerCertificate={TRUST};"
        f"Connection Timeout={TIMEOUT};"
    )
    url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc)}"
    eng = create_engine(url, pool_pre_ping=True, future=True)

    @event.listens_for(eng, "before_cursor_execute")
    def _fast_execmany(conn, cursor, statement, parameters, context, executemany):
        if executemany and hasattr(cursor, "fast_executemany"):
            cursor.fast_executemany = True

    return eng

engine: Engine = build_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

def get_db():
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class Department(BaseModel):
    id: int = Field(..., gt=0)
    name: str = Field(..., min_length=1, max_length=255)

class Job(BaseModel):
    id: int = Field(..., gt=0)
    name: str = Field(..., min_length=1, max_length=255)

class HiredEmployeeCreate(BaseModel):
    id: int
    name: Optional[str] = None
    datetime: Optional[str] = None  # entrará string; validamos en BD al insertar
    department_id: Optional[int] = None
    job_id: Optional[int] = None

    @field_validator("name")
    @classmethod
    def clean_name(cls, v):
        if v is None:
            return v
        return v.strip()

class HiredEmployeeResponse(BaseModel):
    id: int
    name: Optional[str] = None
    datetime: Optional[str] = None  # devolvemos ISO o None
    department_id: Optional[int] = None
    job_id: Optional[int] = None

class Token(BaseModel):
    access_token: str
    token_type: str

class BatchResponse(BaseModel):
    inserted: int
    duplicates: int = 0
    errors: List[str] = Field(default_factory=list)


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)

def authenticate_user(username: str, password: str) -> bool:
    if username != API_USER:
        return False
    return verify_password(password, API_PASS_HASH)

def create_access_token(subject: str, expires_minutes: int = ACCESS_TOKEN_EXPIRE_MINUTES) -> str:
    expire = datetime.utcnow() + timedelta(minutes=expires_minutes)
    to_encode = {"sub": subject, "exp": expire}
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(token: str = Depends(oauth2_scheme)) -> str:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Token inválido o expirado",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        sub: str = payload.get("sub")
        if sub is None or sub != API_USER:
            raise credentials_exception
        return sub
    except JWTError:
        raise credentials_exception


app = FastAPI(
    title="API PoC - Migración de Datos",
    version="1.0.0",
    description="API para ingesta y consulta de datos históricos"
)

@app.get("/")
async def root():
    return {"message": "API de Migración de Datos", "version": "1.0.0"}

@app.get("/health")
async def health():
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Error de conexión a la base de datos: {str(e)}"
        )

@app.post("/login", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    if not authenticate_user(form_data.username, form_data.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales inválidas",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = create_access_token(subject=API_USER)
    return {"access_token": token, "token_type": "bearer"}


def _to_iso_safely(val) -> Optional[str]:
    """Devuelve ISO-8601 o None para cualquier valor de fecha extraño."""
    try:
        if isinstance(val, datetime):
            return val.isoformat()
        if isinstance(val, str):
            try:
                return datetime.fromisoformat(val.replace('Z', '+00:00')).isoformat()
            except Exception:
                pass
            try:
                import pandas as pd  # opcional; solo si está instalado
                ts = pd.to_datetime(val, errors="coerce", utc=False)
                if pd.isna(ts):
                    return None
                return ts.to_pydatetime().isoformat()
            except Exception:
                return None
        return None
    except Exception:
        return None


@app.get("/departments", response_model=List[Department])
async def list_departments(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user)
):
    rows = db.execute(
        text("""SELECT id, name FROM dbo.[departments]
                ORDER BY id OFFSET :skip ROWS FETCH NEXT :limit ROWS ONLY"""),
        {"skip": skip, "limit": limit}
    ).mappings().all()
    return list(rows)

@app.get("/jobs", response_model=List[Job])
async def list_jobs(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user)
):
    rows = db.execute(
        text("""SELECT id, name FROM dbo.[jobs]
                ORDER BY id OFFSET :skip ROWS FETCH NEXT :limit ROWS ONLY"""),
        {"skip": skip, "limit": limit}
    ).mappings().all()
    return list(rows)

@app.get("/employees", response_model=List[HiredEmployeeResponse])
async def list_employees(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user)
):
    try:
        rows = db.execute(
            text("""
                SELECT id, name, [datetime], department_id, job_id
                FROM dbo.[hired_employees]
                ORDER BY id OFFSET :skip ROWS FETCH NEXT :limit ROWS ONLY
            """),
            {"skip": skip, "limit": limit}
        ).mappings().all()

        out = []
        for r in rows:
            out.append({
                "id": r.get("id"),
                "name": r.get("name"),
                "datetime": _to_iso_safely(r.get("datetime")),
                "department_id": r.get("department_id"),
                "job_id": r.get("job_id"),
            })
        return out

    except Exception as e:
        logger.error("Error en /employees: %s", e)
        logger.debug("Traceback:\n%s", traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"/employees error: {str(e)}")

@app.get("/employees/_diag")
async def employees_diag(
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user)
):
    try:
        rows = db.execute(
            text("SELECT TOP 3 id, name, [datetime], department_id, job_id FROM dbo.[hired_employees] ORDER BY id")
        ).mappings().all()
        sample = []
        for r in rows:
            dt = r.get("datetime")
            sample.append({
                "id": r.get("id"),
                "name": r.get("name"),
                "raw_datetime": str(dt),
                "python_type": type(dt).__name__,
                "iso_parsed": _to_iso_safely(dt),
                "department_id": r.get("department_id"),
                "job_id": r.get("job_id"),
            })
        return {"sample": sample}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"/employees/_diag error: {str(e)}")


@app.post("/ingest/employees", response_model=BatchResponse)
async def ingest_employees(
    employees: List[HiredEmployeeCreate],
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user)
):
    if not employees:
        raise HTTPException(status_code=400, detail="Lista de empleados vacía")
    if len(employees) > 1000:
        raise HTTPException(status_code=400, detail="Máximo 1000 registros por lote")

    stmt = text("""
        INSERT INTO dbo.[hired_employees]
        (id, name, [datetime], department_id, job_id)
        VALUES (:id, :name, :datetime, :department_id, :job_id)
    """)

    inserted = 0
    duplicates = 0
    errors: List[str] = []

    try:
        db.execute(stmt, [
            {
                "id": e.id,
                "name": e.name,
                "datetime": e.datetime,
                "department_id": e.department_id,
                "job_id": e.job_id
            } for e in employees
        ])
        db.commit()
        inserted = len(employees)
    except Exception:
        db.rollback()
        # Fallback fila a fila para clasificar errores
        for e in employees:
            try:
                db.execute(stmt, {
                    "id": e.id,
                    "name": e.name,
                    "datetime": e.datetime,
                    "department_id": e.department_id,
                    "job_id": e.job_id
                })
                db.commit()
                inserted += 1
            except Exception as ee:
                db.rollback()
                msg = str(ee)
                if "PRIMARY KEY" in msg or "duplicate" in msg.lower():
                    duplicates += 1
                else:
                    errors.append(f"ID {e.id}: {msg}")

    return BatchResponse(inserted=inserted, duplicates=duplicates, errors=errors)

@app.post("/ingest/departments", response_model=BatchResponse)
async def ingest_departments(
    departments: List[Department],
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user)
):
    if not departments:
        raise HTTPException(status_code=400, detail="Lista de departamentos vacía")

    stmt = text("INSERT INTO dbo.[departments] (id, name) VALUES (:id, :name)")
    inserted = 0
    duplicates = 0
    errors: List[str] = []

    for d in departments:
        try:
            db.execute(stmt, {"id": d.id, "name": d.name})
            db.commit()
            inserted += 1
        except Exception as e:
            db.rollback()
            msg = str(e)
            if "PRIMARY KEY" in msg or "duplicate" in msg.lower():
                duplicates += 1
            else:
                errors.append(f"ID {d.id}: {msg}")

    return BatchResponse(inserted=inserted, duplicates=duplicates, errors=errors)

@app.post("/ingest/jobs", response_model=BatchResponse)
async def ingest_jobs(
    jobs: List[Job],
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user)
):
    if not jobs:
        raise HTTPException(status_code=400, detail="Lista de cargos vacía")

    stmt = text("INSERT INTO dbo.[jobs] (id, name) VALUES (:id, :name)")
    inserted = 0
    duplicates = 0
    errors: List[str] = []

    for j in jobs:
        try:
            db.execute(stmt, {"id": j.id, "name": j.name})
            db.commit()
            inserted += 1
        except Exception as e:
            db.rollback()
            msg = str(e)
            if "PRIMARY KEY" in msg or "duplicate" in msg.lower():
                duplicates += 1
            else:
                errors.append(f"ID {j.id}: {msg}")

    return BatchResponse(inserted=inserted, duplicates=duplicates, errors=errors)


try:
    from analytics import router as analytics_router
    app.include_router(analytics_router)
    logger.info("Analytics router registrado")
except Exception as e:
    logger.warning("No se pudo registrar analytics router: %s", e)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8001, reload=True)

# Export para analytics.py
__all__ = ["engine", "get_db", "get_current_user"]
