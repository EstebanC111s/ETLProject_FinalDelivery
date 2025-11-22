# dags/etl.py
from datetime import datetime
import os, sys, pathlib, logging

# Asegura que /opt/airflow/src esté importable
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import great_expectations as gx
from great_expectations.data_context import DataContext  # <-- NUEVO

# Funciones en src/
from src.extract_old import run as extract_old
from src.extract_new import run as extract_new
from src.extract_api import run as extract_api
from src.transform   import run as transform_run

SCHEDULE = os.getenv("SCHEDULE", "@daily")
PSQL = "psql postgresql://etl:etl@warehouse:5432/etl -v ON_ERROR_STOP=1 -f"

# Ruta real del proyecto GX dentro del contenedor
BASE_DIR = pathlib.Path(__file__).resolve().parents[1]       # /opt/airflow
GE_ROOT = BASE_DIR / "great_expectations"                    # /opt/airflow/great_expectations


def _run_ge_checkpoint(checkpoint_name: str):
    """
    Ejecuta un checkpoint de Great Expectations 0.17.x
    usando el proyecto basado en archivos de /opt/airflow/great_expectations.
    """
    logger = logging.getLogger("airflow.task")
    logger.info("GX ROOT: %s", GE_ROOT)

    # Debug: listar lo que ve el contenedor
    try:
        logger.info("Contenido de GE_ROOT: %s", os.listdir(GE_ROOT))
        logger.info(
            "Contenido de checkpoints/: %s",
            os.listdir(GE_ROOT / "checkpoints")
        )
    except Exception as e:
        logger.error("Error listando carpetas GX: %s", e)

    # Usar DataContext clásico, NO get_context()
    context = DataContext(context_root_dir=str(GE_ROOT))

    # Ejecutar el checkpoint por nombre
    result = context.run_checkpoint(checkpoint_name=checkpoint_name)

    success = result.get("success", False)
    logger.info("GX checkpoint '%s' success = %s", checkpoint_name, success)

    if not success:
        raise ValueError(
            f"GX checkpoint '{checkpoint_name}' FAILED. Revisa Data Docs."
        )

    return {"checkpoint": checkpoint_name, "success": success}

with DAG(
    dag_id="etl",
    description="ETL: extracts -> transform -> merge -> GE clean_staging -> build dims (+snowflake geo)",
    start_date=datetime(2025, 10, 17),
    schedule_interval=SCHEDULE,
    catchup=False,
    default_args={"owner": "team", "retries": 1},
    tags=["etl", "acueductos", "dw"],
) as dag:

    t_extract_old = PythonOperator(
        task_id="extract_old",
        python_callable=extract_old,
    )
    t_extract_new = PythonOperator(
        task_id="extract_new",
        python_callable=extract_new,
    )
    t_extract_api = PythonOperator(
        task_id="extract_api",
        python_callable=extract_api,
    )

    t_transform = PythonOperator(
        task_id="transform",
        python_callable=transform_run,
    )

    t_merge_sql = BashOperator(
        task_id="merge_clean_sql",
        bash_command=f"{PSQL} /opt/airflow/sql/merge_pipeline.sql",
    )

    # GE SOBRE clean_staging usando chk_clean_calidadstag
    ge_chk_clean_staging = PythonOperator(
        task_id="ge_chk_clean_staging",
        python_callable=_run_ge_checkpoint,
        op_args=["chk_clean_calidadstag"],
    )

    build_dim_geo = BashOperator(
        task_id="build_dim_geo",
        bash_command=f"{PSQL} /opt/airflow/sql/build_dim_geo.sql",
    )

    t_build_dim_calidad = BashOperator(
        task_id="build_dim_calidad",
        bash_command=f"{PSQL} /opt/airflow/sql/build_dim_calidad.sql",
    )
    t_build_dim_prestacion = BashOperator(
        task_id="build_dim_prestacion",
        bash_command=f"{PSQL} /opt/airflow/sql/build_dim_prestacion.sql",
    )
    t_build_dim_prestadores = BashOperator(
        task_id="build_dim_prestadores",
        bash_command=f"{PSQL} /opt/airflow/sql/build_dim_prestadores.sql",
    )

    add_geo_fks = BashOperator(
        task_id="add_geo_fks",
        bash_command=f"{PSQL} /opt/airflow/sql/add_geo_fks.sql",
    )

    # Orquestación
    [t_extract_old, t_extract_new, t_extract_api] >> t_transform >> t_merge_sql
    t_merge_sql >> ge_chk_clean_staging
    ge_chk_clean_staging >> build_dim_geo
    build_dim_geo >> [t_build_dim_calidad, t_build_dim_prestacion, t_build_dim_prestadores] >> add_geo_fks
