# src/kafka_consumer_metrics.py
from __future__ import annotations
import os
import json
from typing import Any, Dict

from dotenv import load_dotenv
from kafka import KafkaConsumer
from sqlalchemy import text

try:
    from .util_db import get_engine
except ImportError:
    from util_db import get_engine

load_dotenv()

# Siempre que corremos los .py en el host, queremos hablar con localhost:9092
BROKERS = os.getenv("KAFKA_BROKERS_HOST", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_RUPS", "etl_p3.rups.metrics")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "etl_p3_rt_dashboard")

print(f"[consumer] Usando brokers Kafka en: {BROKERS}")


TABLE_RT = "rt_rups_metrics"


def ensure_rt_table() -> None:
    """Crea la tabla de métricas en tiempo real si no existe."""
    eng = get_engine()
    dialect = eng.dialect.name

    id_col = "SERIAL" if dialect != "sqlite" else "INTEGER"

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_RT} (
        id                {id_col} PRIMARY KEY,
        ts_ingestion      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        departamento      TEXT NOT NULL,
        municipio         TEXT NOT NULL,
        provider_id       TEXT NOT NULL,
        nombre            TEXT,
        servicio          TEXT,
        estado            TEXT,
        clasificacion     TEXT,
        es_publica        BOOLEAN,
        total_prestadores INT,
        acueducto_total   INT,
        alcantarillado_total INT,
        aseo_total        INT,
        operativos_total  INT,
        suspendidos_total INT,
        fecha_ult_muestra DATE,
        estado_ph         TEXT,
        estado_cloro      TEXT,
        puntos_monitoreo  INT,
        mediciones        INT,
        parametros_distintos INT
    );
    """
    with eng.begin() as conn:
        conn.execute(text(ddl))


def save_message(payload: Dict[str, Any]) -> None:
    """Inserta un mensaje JSON del topic en la tabla rt_rups_metrics."""
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {TABLE_RT} (
                    departamento, municipio, provider_id,
                    nombre, servicio, estado, clasificacion, es_publica,
                    total_prestadores, acueducto_total, alcantarillado_total,
                    aseo_total, operativos_total, suspendidos_total,
                    fecha_ult_muestra, estado_ph, estado_cloro,
                    puntos_monitoreo, mediciones, parametros_distintos
                )
                VALUES (
                    :departamento, :municipio, :provider_id,
                    :nombre, :servicio, :estado, :clasificacion, :es_publica,
                    :total_prestadores, :acueducto_total, :alcantarillado_total,
                    :aseo_total, :operativos_total, :suspendidos_total,
                    :fecha_ult_muestra, :estado_ph, :estado_cloro,
                    :puntos_monitoreo, :mediciones, :parametros_distintos
                );
            """),
            {
                "departamento":       payload.get("departamento"),
                "municipio":          payload.get("municipio"),
                "provider_id":        payload.get("provider_id"),
                "nombre":             payload.get("nombre"),
                "servicio":           payload.get("servicio"),
                "estado":             payload.get("estado"),
                "clasificacion":      payload.get("clasificacion"),
                "es_publica":         payload.get("es_publica"),
                "total_prestadores":  payload.get("total_prestadores"),
                "acueducto_total":    payload.get("acueducto_total"),
                "alcantarillado_total": payload.get("alcantarillado_total"),
                "aseo_total":         payload.get("aseo_total"),
                "operativos_total":   payload.get("operativos_total"),
                "suspendidos_total":  payload.get("suspendidos_total"),
                "fecha_ult_muestra":  payload.get("fecha_ult_muestra"),
                "estado_ph":          payload.get("estado_ph"),
                "estado_cloro":       payload.get("estado_cloro"),
                "puntos_monitoreo":   payload.get("puntos_monitoreo"),
                "mediciones":         payload.get("mediciones"),
                "parametros_distintos": payload.get("parametros_distintos"),
            },
        )


def main() -> None:
    ensure_rt_table()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKERS.split(","),
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda m: m.decode("utf-8") if m else None,
    )

    print(f"👂 Escuchando topic '{TOPIC}' en {BROKERS}...")

    for msg in consumer:
        payload = msg.value
        key = msg.key
        print(f"📥 Mensaje recibido key={key}: {payload}")
        try:
            save_message(payload)
            print("💾 Guardado en rt_rups_metrics.")
        except Exception as e:
            print("❌ Error guardando mensaje:", e)


if __name__ == "__main__":
    main()
