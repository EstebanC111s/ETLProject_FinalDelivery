import os
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.create import create_engine
from sqlalchemy.inspection import inspect as sqla_inspect  # lo dejo por si lo usas en otro lado

def _choose_url() -> str:
    """
    Selecciona la URL de conexión dependiendo de dónde se está corriendo el código.

    - Dentro de contenedores de Airflow: usa DB_URL (host 'warehouse').
    - Desde la máquina host (Windows): usa DB_URL_HOST si existe; si no, DB_URL.
    """
    db_url = os.getenv("DB_URL")
    db_url_host = os.getenv("DB_URL_HOST")

    # 1) Si estamos dentro de Airflow (variable típica del core) y hay DB_URL -> usa esa
    if os.getenv("AIRFLOW__CORE__EXECUTOR") and db_url:
        return db_url

    # 2) Si estamos fuera de Airflow (scripts en host) y existe DB_URL_HOST -> úsala
    if db_url_host:
        return db_url_host

    # 3) Fallback: al menos DB_URL
    if db_url:
        return db_url

    # 4) Nada definido
    raise RuntimeError("No se encontró ninguna URL de base de datos (DB_URL ni DB_URL_HOST).")


def get_engine() -> Engine:
    url = _choose_url()
    return create_engine(url, pool_pre_ping=True, future=False)
