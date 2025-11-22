# src/kafka_producer_metrics.py
from __future__ import annotations
import os
import json
from typing import Any, Dict

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer
from sqlalchemy import text

try:
    from .util_db import get_engine
except ImportError:
    from util_db import get_engine

load_dotenv()

BROKERS = os.getenv("KAFKA_BROKERS_HOST", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_RUPS", "etl_p3.rups.metrics")

print(f"[producer] Usando brokers Kafka en: {BROKERS}")



def fetch_metrics() -> pd.DataFrame:
    """Lee las métricas nivel prestador + contexto geo del DW."""
    eng = get_engine()
    sql = text("""
        SELECT
          dp.departamento,
          dp.municipio,
          dp.provider_id,
          dp.nombre,
          dp.servicio,
          dp.estado,
          dp.clasificacion,
          dp.es_publica,
          pg.total_prestadores,
          pg.acueducto_total,
          pg.alcantarillado_total,
          pg.aseo_total,
          pg.operativos_total,
          pg.suspendidos_total,
          cg.fecha_ult_muestra,
          cg.estado_ph,
          cg.estado_cloro,
          cg.puntos_monitoreo,
          cg.mediciones,
          cg.parametros_distintos
        FROM dim_prestadores dp
        LEFT JOIN dim_prestacion_geo pg
          ON pg.departamento = dp.departamento
         AND pg.municipio    = dp.municipio
        LEFT JOIN dim_calidad_geo cg
          ON cg.departamento = dp.departamento
         AND cg.municipio    = dp.municipio
        ORDER BY dp.departamento, dp.municipio, dp.provider_id;
    """)
    return pd.read_sql(sql, eng)


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BROKERS.split(","),
        # default=str convierte date, datetime, Decimal, etc. a string
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8") if v is not None else None,
    )



def main() -> None:
    df = fetch_metrics()

    if df.empty:
        print("⚠️  No hay métricas para enviar (DataFrame vacío).")
        return

    producer = build_producer()
    print(f"🔄 Enviando {len(df)} mensajes a '{TOPIC}' usando brokers {BROKERS}...")

    for _, row in df.iterrows():
        payload: Dict[str, Any] = {
            col: (None if pd.isna(val) else val)
            for col, val in row.items()
        }
        key = f"{row['departamento']}|{row['municipio']}|{row['provider_id']}"
        producer.send(TOPIC, key=key, value=payload)
        print("✅ Enviado:", key)

    producer.flush()
    producer.close()
    print("🏁 Producer terminado.")


if __name__ == "__main__":
    main()
