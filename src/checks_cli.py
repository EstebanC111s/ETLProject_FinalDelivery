# src/checks_cli.py
# -*- coding: utf-8 -*-
import os
import sys
from contextlib import contextmanager
from typing import Dict, List, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine

import os
from contextlib import contextmanager
from typing import Any, Callable, Optional, cast

from sqlalchemy.engine import Engine
from typing import Any


# (Opcional) usa tu helper si existe
# `_get_engine` es una función opcional que debe devolver algo "Engine-like".
_get_engine: Optional[Callable[[], Any]]

try:
    from .util_db import get_engine as _get_engine  # type: ignore[assignment]
except Exception:
    _get_engine = None


# ===========================
# Config
# ===========================
# Umbral de % "DESCONOCIDO" (configurable por env)
MAX_DESCO_PCT = float(os.getenv("MAX_DESCO_PCT", "5.0"))  # 5% por defecto


# ---------------------------
# helpers de entorno / engine
# ---------------------------

def _is_airflow() -> bool:
    """Detecta si corre dentro de un task de Airflow."""
    return bool(os.getenv("AIRFLOW_CTX_DAG_ID"))


def _maybe_local_db_url(url: str) -> str:
    """
    Si el script corre fuera de Docker, 'warehouse:5432' no existe.
    Fallback a 'localhost:5434' (mapeo del compose).
    """
    if not url:
        return url
    if "warehouse:5432" in url:
        return url.replace("warehouse:5432", "localhost:5434")
    return url


def _build_engine() -> Any:
    """
    Obtiene un Engine de SQLAlchemy.
    - Si existe util_db.get_engine() lo usa (ideal dentro del contenedor).
    - Si no, construye desde DB_URL (y hace fallback a localhost:5434).
    """
    if _get_engine is not None:
        return _get_engine()

    from sqlalchemy import create_engine

    url = os.getenv("DB_URL", "")
    url = _maybe_local_db_url(url)
    if not url:
        raise RuntimeError("DB_URL no está definido en el entorno.")

    return create_engine(url, future=True)
@contextmanager

def connect():
    eng = _build_engine()
    with eng.begin() as conn:
        yield conn

def _count(conn, sql: str) -> int:
    return int(conn.execute(text(sql)).scalar() or 0)


# ---------------------------
# lógica de chequeos
# ---------------------------

def _collect(conn) -> Tuple[List[str], Dict[str, int]]:
    """
    Ejecuta todos los chequeos y devuelve:
      - problems: lista de strings con problemas (vacía si todo ok)
      - metrics:  dict con métricas para logging/XCom
    """
    problems: List[str] = []
    warns: List[str] = []     # avisos no bloqueantes
    m: Dict[str, int] = {}

    # 1) existencia y conteos
    try:
        m["rows_clean_staging"] = _count(conn, "SELECT COUNT(*) FROM clean_staging;")
        print(f"[OK] clean_staging filas: {m['rows_clean_staging']}")
        if m["rows_clean_staging"] == 0:
            problems.append("clean_staging está vacío.")
    except Exception as e:
        problems.append(f"clean_staging no existe o error de lectura: {e}")

    try:
        m["rows_clean_calidad"] = _count(conn, "SELECT COUNT(*) FROM clean_calidad;")
        print(f"[OK] clean_calidad filas: {m['rows_clean_calidad']}")
        if m["rows_clean_calidad"] == 0:
            problems.append("clean_calidad está vacío.")
    except Exception as e:
        problems.append(f"clean_calidad no existe o error de lectura: {e}")

    if problems:
        return problems, m  # si faltan tablas, cortamos aquí

    # 2) nulos prohibidos (claves)
    m["nulls_clean_staging"] = _count(conn, """
        SELECT COUNT(*) FROM clean_staging
        WHERE provider_id IS NULL OR nombre IS NULL OR
              departamento IS NULL OR municipio IS NULL OR servicio IS NULL;
    """)
    print(f"[CHK] nulos en claves (clean_staging): {m['nulls_clean_staging']}")
    if m["nulls_clean_staging"] > 0:
        problems.append(f"clean_staging tiene {m['nulls_clean_staging']} filas con nulos en claves.")

    m["nulls_clean_calidad"] = _count(conn, """
        SELECT COUNT(*) FROM clean_calidad
        WHERE departamento IS NULL OR municipio IS NULL OR
              fecha_muestra IS NULL OR parametro IS NULL;
    """)
    print(f"[CHK] nulos en claves (clean_calidad): {m['nulls_clean_calidad']}")
    if m["nulls_clean_calidad"] > 0:
        problems.append(f"clean_calidad tiene {m['nulls_clean_calidad']} filas con nulos en claves.")

    # 3) duplicados por llaves lógicas (prestadores)
    m["dups_clean_staging"] = _count(conn, """
        SELECT COUNT(*) FROM (
          SELECT provider_id, servicio, departamento, municipio, COUNT(*) c
          FROM clean_staging
          GROUP BY 1,2,3,4
          HAVING COUNT(*) > 1
        ) t;
    """)
    print(f"[CHK] duplicados clave (clean_staging): {m['dups_clean_staging']}")
    if m["dups_clean_staging"] > 0:
        problems.append(f"clean_staging tiene {m['dups_clean_staging']} combinaciones clave duplicadas.")

    # 3b) duplicados en calidad por punto
    m["dups_calidad_punto"] = _count(conn, """
        SELECT COUNT(*) FROM (
          SELECT departamento, municipio, parametro, fecha_muestra, COALESCE(nombre_punto,'') AS p, COUNT(*) c
          FROM clean_calidad
          GROUP BY 1,2,3,4,5
          HAVING COUNT(*) > 1
        ) t;
    """)
    print(f"[CHK] duplicados clave (clean_calidad por punto): {m['dups_calidad_punto']}")
    if m["dups_calidad_punto"] > 0:
        problems.append(
            f"clean_calidad tiene {m['dups_calidad_punto']} duplicados a nivel (dep,muni,param,fecha,punto)."
        )

    # (info) colisiones municipio/día/parámetro
    m["colisiones_calidad_muni_dia"] = _count(conn, """
        SELECT COUNT(*) FROM (
          SELECT departamento, municipio, parametro, fecha_muestra, COUNT(*) c
          FROM clean_calidad
          GROUP BY 1,2,3,4
          HAVING COUNT(*) > 1
        ) t;
    """)
    print(f"[INFO] colisiones municipio/día/parámetro: {m['colisiones_calidad_muni_dia']}")

    # 4) rango de fechas
    m["fechas_fuera_rango"] = _count(conn, """
        SELECT COUNT(*) FROM clean_calidad
        WHERE fecha_muestra < DATE '2000-01-01' OR fecha_muestra > CURRENT_DATE;
    """)
    print(f"[CHK] fechas fuera de rango (clean_calidad): {m['fechas_fuera_rango']}")
    if m["fechas_fuera_rango"] > 0:
        problems.append(f"clean_calidad tiene {m['fechas_fuera_rango']} fechas fuera de rango.")

    # 5) coordenadas
    m["latlon_despareados"] = _count(conn, """
        SELECT COUNT(*) FROM clean_calidad
        WHERE (latitud IS NULL) <> (longitud IS NULL);
    """)
    print(f"[CHK] lat/lon despareados: {m['latlon_despareados']}")
    if m["latlon_despareados"] > 0:
        problems.append(f"clean_calidad tiene {m['latlon_despareados']} filas con lat/lon despareados.")

    m["coords_fuera_col"] = _count(conn, """
        SELECT COUNT(*) FROM clean_calidad
        WHERE (latitud  IS NOT NULL AND (latitud  < -5  OR latitud  > 15))
           OR (longitud IS NOT NULL AND (longitud < -82 OR longitud > -66));
    """)
    print(f"[CHK] coordenadas fuera de rango COL: {m['coords_fuera_col']}")
    if m["coords_fuera_col"] > 0:
        problems.append(f"clean_calidad tiene {m['coords_fuera_col']} coordenadas fuera de rango COL.")

    # 6) reglas por parámetro
    m["ph_fuera_0_14"] = _count(conn, """
        SELECT COUNT(*) FROM clean_calidad
        WHERE parametro = 'PH' AND valor IS NOT NULL AND (valor < 0 OR valor > 14);
    """)
    print(f"[CHK] pH fuera de 0..14: {m['ph_fuera_0_14']}")
    if m["ph_fuera_0_14"] > 0:
        problems.append("pH fuera de 0..14 (deberían haber quedado en NULL para imputar).")

    m["cloro_fuera_0_5"] = _count(conn, """
        SELECT COUNT(*) FROM clean_calidad
        WHERE parametro LIKE 'CLORO%' AND valor IS NOT NULL AND (valor < 0 OR valor > 5);
    """)
    print(f"[CHK] CLORO fuera de 0..5: {m['cloro_fuera_0_5']}")
    if m["cloro_fuera_0_5"] > 0:
        problems.append("CLORO fuera de 0..5 (deberían haber quedado en NULL para imputar).")

    # 6c) parámetros no negativos (turbidez/conductividad/dureza/alcalinidad)
    m["valores_negativos"] = _count(conn, """
        SELECT COUNT(*) FROM clean_calidad
        WHERE valor < 0
          AND (
            parametro ILIKE '%TURBIDEZ%' OR
            parametro ILIKE '%CONDUCT%'  OR
            parametro ILIKE '%DUREZA%'   OR
            parametro ILIKE '%ALCALIN%'
          );
    """)
    print(f"[CHK] valores negativos en parámetros no-negativos: {m['valores_negativos']}")
    if m["valores_negativos"] > 0:
        problems.append(f"clean_calidad tiene {m['valores_negativos']} valores negativos en parámetros no-negativos.")

    # 7) Contacto (avisos blandos)
    m["emails_malos"] = _count(conn, r"""
        SELECT COUNT(*) FROM clean_staging
        WHERE email IS NOT NULL AND email !~* '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$';
    """)
    print(f"[CHK] emails con formato inválido: {m['emails_malos']}")
    if m["emails_malos"] > 0:
        warns.append(f"{m['emails_malos']} emails con formato inválido (ej. 'a@b.c').")

    m["telefonos_malos"] = _count(conn, r"""
        SELECT COUNT(*) FROM clean_staging
        WHERE telefono IS NOT NULL
          AND REGEXP_REPLACE(telefono, '[^0-9+]', '', 'g') !~ '^\+?[0-9]{7,15}$';
    """)
    print(f"[CHK] teléfonos con formato inválido: {m['telefonos_malos']}")
    if m["telefonos_malos"] > 0:
        warns.append(f"{m['telefonos_malos']} teléfonos con posible formato inválido (esperado 7–15 dígitos, opcional '+').")

    # 3c) servicio dentro del catálogo permitido (SOLO para servicios relevantes agua/aseo)
    m["serv_fuera_catalogo"] = _count(conn, """
        WITH relevantes AS (
          SELECT servicio
          FROM clean_staging
          WHERE servicio ILIKE ANY (ARRAY[
            '%ACUED%',      -- acueducto
            '%ALCANT%',     -- alcantarillado
            '%ASEO%', '%RESIDU%', '%BASUR%', -- aseo / residuos
            '%AGUA%'        -- agua potable / variaciones
          ])
        )
        SELECT COUNT(*) FROM relevantes
        WHERE servicio NOT IN ('ACUEDUCTO','ALCANTARILLADO','ASEO','DESCONOCIDO');
    """)
    print(f"[CHK] servicio fuera de catálogo (solo relevantes agua/aseo): {m['serv_fuera_catalogo']}")
    if m["serv_fuera_catalogo"] > 0:
        problems.append(f"clean_staging tiene {m['serv_fuera_catalogo']} filas con servicio fuera de catálogo (en los relevantes de agua/aseo).")

    # 3d) porcentaje de 'DESCONOCIDO' en claves (aviso si supera umbral)
    row = conn.execute(text("""
        WITH t AS (
          SELECT COUNT(*) AS n,
                 SUM((departamento='DESCONOCIDO')::int) AS dep,
                 SUM((municipio   ='DESCONOCIDO')::int) AS mun,
                 SUM((servicio    ='DESCONOCIDO')::int) AS srv
          FROM clean_staging
        )
        SELECT
          CASE WHEN n=0 THEN 0 ELSE 100.0*dep/n END AS pct_dep,
          CASE WHEN n=0 THEN 0 ELSE 100.0*mun/n END AS pct_mun,
          CASE WHEN n=0 THEN 0 ELSE 100.0*srv/n END AS pct_srv
        FROM t;
    """)).one()
    pct_dep, pct_mun, pct_srv = float(row[0]), float(row[1]), float(row[2])
    print(f"[INFO] DESCONOCIDO% dep={pct_dep:.2f} mun={pct_mun:.2f} serv={pct_srv:.2f}")
    if any(p > MAX_DESCO_PCT for p in (pct_dep, pct_mun, pct_srv)):
        warns.append(f"Porcentaje de 'DESCONOCIDO' alto: dep={pct_dep:.2f}%, mun={pct_mun:.2f}%, serv={pct_srv:.2f}% (umbral {MAX_DESCO_PCT}%).")

    # 8) Dimensiones: existencia y filas
    try:
        m["rows_dim_calidad_geo"]    = _count(conn, "SELECT COUNT(*) FROM dim_calidad_geo;")
        m["rows_dim_prestacion_geo"] = _count(conn, "SELECT COUNT(*) FROM dim_prestacion_geo;")
        m["rows_dim_prestadores"]    = _count(conn, "SELECT COUNT(*) FROM dim_prestadores;")
        print(f"[OK] dim_calidad_geo filas: {m['rows_dim_calidad_geo']}")
        print(f"[OK] dim_prestacion_geo filas: {m['rows_dim_prestacion_geo']}")
        print(f"[OK] dim_prestadores filas: {m['rows_dim_prestadores']}")
    except Exception as e:
        problems.append(f"Una dimensión no existe o falló la lectura: {e}")
        # retornamos ya; las demás validaciones de dims dependen de su existencia
        for w in warns:
            print(f"[WARN] {w}")
        m["warn_count"] = len(warns)
        return problems, m

    # 8a) PKs únicas
    m["dups_dim_calidad_geo"] = _count(conn, """
        SELECT COUNT(*) FROM (
          SELECT departamento, municipio, COUNT(*) c
          FROM dim_calidad_geo
          GROUP BY 1,2 HAVING COUNT(*)>1
        ) t;
    """)
    print(f"[CHK] dups PK dim_calidad_geo: {m['dups_dim_calidad_geo']}")
    if m["dups_dim_calidad_geo"] > 0:
        problems.append(f"dim_calidad_geo tiene {m['dups_dim_calidad_geo']} duplicados en (departamento, municipio).")

    m["dups_dim_prestacion_geo"] = _count(conn, """
        SELECT COUNT(*) FROM (
          SELECT departamento, municipio, COUNT(*) c
          FROM dim_prestacion_geo
          GROUP BY 1,2 HAVING COUNT(*)>1
        ) t;
    """)
    print(f"[CHK] dups PK dim_prestacion_geo: {m['dups_dim_prestacion_geo']}")
    if m["dups_dim_prestacion_geo"] > 0:
        problems.append(f"dim_prestacion_geo tiene {m['dups_dim_prestacion_geo']} duplicados en (departamento, municipio).")

    m["dups_dim_prestadores"] = _count(conn, """
        SELECT COUNT(*) FROM (
          SELECT departamento, municipio, provider_id, COUNT(*) c
          FROM dim_prestadores
          GROUP BY 1,2,3 HAVING COUNT(*)>1
        ) t;
    """)
    print(f"[CHK] dups PK dim_prestadores: {m['dups_dim_prestadores']}")
    if m["dups_dim_prestadores"] > 0:
        problems.append(f"dim_prestadores tiene {m['dups_dim_prestadores']} duplicados en (departamento, municipio, provider_id).")

    # 8b) No nulos en llaves de dimensiones
    m["nulls_dim_calidad_geo"] = _count(conn, """
        SELECT COUNT(*) FROM dim_calidad_geo
        WHERE departamento IS NULL OR municipio IS NULL;
    """)
    m["nulls_dim_prestacion_geo"] = _count(conn, """
        SELECT COUNT(*) FROM dim_prestacion_geo
        WHERE departamento IS NULL OR municipio IS NULL;
    """)
    m["nulls_dim_prestadores"] = _count(conn, """
        SELECT COUNT(*) FROM dim_prestadores
        WHERE departamento IS NULL OR municipio IS NULL OR provider_id IS NULL;
    """)
    print(f"[CHK] nulos claves dim_calidad_geo: {m['nulls_dim_calidad_geo']}")
    print(f"[CHK] nulos claves dim_prestacion_geo: {m['nulls_dim_prestacion_geo']}")
    print(f"[CHK] nulos claves dim_prestadores: {m['nulls_dim_prestadores']}")
    if m["nulls_dim_calidad_geo"] > 0: problems.append("dim_calidad_geo tiene nulos en llaves.")
    if m["nulls_dim_prestacion_geo"] > 0: problems.append("dim_prestacion_geo tiene nulos en llaves.")
    if m["nulls_dim_prestadores"] > 0: problems.append("dim_prestadores tiene nulos en llaves.")

    # 8c) Referencial suave: dim_prestadores ⊆ clean_staging
    m["prestadores_sin_clean"] = _count(conn, """
        SELECT COUNT(*) FROM dim_prestadores d
        LEFT JOIN clean_staging c
          ON c.provider_id=d.provider_id
         AND c.departamento=d.departamento
         AND c.municipio=d.municipio
        WHERE c.provider_id IS NULL;
    """)
    print(f"[CHK] prestadores sin match en clean_staging: {m['prestadores_sin_clean']}")
    if m["prestadores_sin_clean"] > 0:
        warns.append(f"{m['prestadores_sin_clean']} registros en dim_prestadores no encontraron match exacto en clean_staging (posible normalización distinta).")

    # --- imprimir avisos no bloqueantes ---
    for w in warns:
        print(f"[WARN] {w}")
    m["warn_count"] = len(warns)
    return problems, m


# ---------------------------
# wrappers CLI / Airflow
# ---------------------------

def _fail(problems: List[str]):
    msg = "\n".join(f" - {p}" for p in problems)
    if _is_airflow():
        # En Airflow, fallamos con excepción (para que el task quede FAILED)
        raise RuntimeError("DQ QUICKCHECK FALLÓ:\n" + msg)
    else:
        print("\n❌ [DQ QUICKCHECK] FALLÓ:")
        print(msg)
        print("")
        sys.exit(1)

def run(**kwargs):
    """
    Wrapper para Airflow (PythonOperator).
    - Imprime el detalle en logs del task.
    - Devuelve un dict con métricas a XCom si todo ok.
    - Lanza RuntimeError si hay problemas.
    """
    print("\n=== DQ QUICKCHECK (Airflow Task) ===")
    with connect() as conn:
        problems, metrics = _collect(conn)

    if problems:
        _fail(problems)

    print("\n✅ [DQ QUICKCHECK] OK — datos básicos y dimensiones consistentes.\n")
    return {"status": "ok", **metrics}

def main():
    """
    Modo CLI (python -m src.checks_cli).
    Guarda comportamiento clásico con sys.exit para devolver códigos de salida.
    """
    print("\n=== DQ QUICKCHECK (terminal) ===")
    with connect() as conn:
        problems, metrics = _collect(conn)

    if problems:
        _fail(problems)

    print("\n✅ [DQ QUICKCHECK] OK — datos básicos y dimensiones consistentes.\n")
    sys.exit(0)

if __name__ == "__main__":
    main()
