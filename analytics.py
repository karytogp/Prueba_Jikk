# analytics.py
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import Dict, Any

# Usar SIEMPRE la Session del app.py
from app import get_db, get_current_user

router = APIRouter(prefix="/analytics", tags=["Analytics"])

@router.get("/test")
async def test_analytics(
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user)
):
    """Ping simple para verificar conexión y auth"""
    try:
        c = db.execute(text("SELECT COUNT(*) FROM dbo.[hired_employees]")).scalar() or 0
        return {"message": "Analytics OK", "employee_count": int(c)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error de prueba: {str(e)}")


@router.get("/hires-by-quarter")
async def hires_by_quarter(
    year: int = Query(2025, description="Año de análisis"),
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user)
):
    """
    Contrataciones por trimestre por (departamento, cargo).
    """
    try:
        rows = db.execute(text("""
            SELECT 
                d.name AS department,
                j.name AS job,
                DATEPART(QUARTER, he.[datetime]) AS quarter,
                COUNT(1) AS cnt
            FROM dbo.[hired_employees] he
            JOIN dbo.[departments] d ON he.department_id = d.id
            JOIN dbo.[jobs] j ON he.job_id = j.id
            WHERE YEAR(he.[datetime]) = :year
            GROUP BY d.name, j.name, DATEPART(QUARTER, he.[datetime])
            ORDER BY d.name, j.name, quarter
        """), {"year": year}).mappings().all()

        acc: Dict[tuple, Dict[str, Any]] = {}
        for r in rows:
            k = (r["department"], r["job"])
            if k not in acc:
                acc[k] = {"department": r["department"], "job": r["job"], "q1": 0, "q2": 0, "q3": 0, "q4": 0}
            q = int(r["quarter"]); c = int(r["cnt"])
            if   q == 1: acc[k]["q1"] = c
            elif q == 2: acc[k]["q2"] = c
            elif q == 3: acc[k]["q3"] = c
            elif q == 4: acc[k]["q4"] = c

        return list(acc.values())

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")


@router.get("/departments-above-average")
async def departments_above_average(
    year: int = Query(2025, description="Año de análisis"),
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user)
):
    """
    Departamentos que contrataron por encima del promedio anual.
    """
    try:
        rows = db.execute(text("""
            WITH DepartmentHires AS (
                SELECT 
                    d.id,
                    d.name AS department,
                    COUNT(he.id) AS hires
                FROM dbo.[hired_employees] he
                JOIN dbo.[departments] d ON he.department_id = d.id
                WHERE YEAR(he.[datetime]) = :year
                GROUP BY d.id, d.name
            )
            SELECT id, department, hires
            FROM DepartmentHires
            WHERE hires > (SELECT AVG(hires) FROM DepartmentHires)
            ORDER BY hires DESC
        """), {"year": year}).mappings().all()

        return [dict(r) for r in rows]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")


@router.get("/hires-summary")
async def hires_summary(
    year: int = Query(2025, description="Año de análisis"),
    db: Session = Depends(get_db),
    user: str = Depends(get_current_user)
):
    """
    Resumen anual: hires por Q1..Q4 + total.
    """
    try:
        rows = db.execute(text("""
            SELECT DATEPART(QUARTER, he.[datetime]) AS quarter, COUNT(1) AS cnt
            FROM dbo.[hired_employees] he
            WHERE YEAR(he.[datetime]) = :year
            GROUP BY DATEPART(QUARTER, he.[datetime])
        """), {"year": year}).mappings().all()

        s = {"year": year, "q1": 0, "q2": 0, "q3": 0, "q4": 0, "total": 0}
        for r in rows:
            q = int(r["quarter"]); c = int(r["cnt"])
            if   q == 1: s["q1"] = c
            elif q == 2: s["q2"] = c
            elif q == 3: s["q3"] = c
            elif q == 4: s["q4"] = c
        s["total"] = s["q1"] + s["q2"] + s["q3"] + s["q4"]
        return s

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

