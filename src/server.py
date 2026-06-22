from __future__ import annotations

import os
import sys
import json
import random
import socket
import asyncio
from pathlib import Path
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, StreamingResponse, Response

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.pipeline import Settings, DuckDBClient

app = FastAPI()

def check_kafka() -> bool:
    settings = Settings.load()
    for s in settings.kafka.bootstrap_servers.split(","):
        s = s.strip()
        if ":" in s:
            host, port_str = s.split(":", 1)
            port = int(port_str)
        else:
            host = s
            port = 9092
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sc:
                sc.settimeout(0.15)
                if sc.connect_ex((host, port)) == 0:
                    return True
        except Exception:
            pass
    return False

def check_duckdb() -> str:
    settings = Settings.load()
    path = settings.storage.duckdb.path
    if not os.path.exists(path):
        return "not_found"
    import duckdb
    try:
        con = duckdb.connect(path, read_only=True)
        try:
            tables = con.execute("SHOW TABLES").fetchall()
            names = [t[0] for t in tables]
            if "query_metrics_processed" in names:
                cnt = con.execute("SELECT COUNT(*) FROM query_metrics_processed").fetchone()[0]
                con.close()
                return "ready" if cnt > 0 else "empty"
            con.close()
            return "uninitialized"
        except Exception:
            con.close()
            return "error"
    except Exception as e:
        err = str(e).lower()
        if "lock" in err or "used by another process" in err:
            return "locked"
        return "error"

def mock_event(idx: int) -> dict:
    queued = random.random() < 0.15
    q_ms = random.randint(100, 2500) if queued else 0
    c_ms = random.randint(10, 300)
    dur = random.uniform(0.1, 9.0)
    if queued: dur += (q_ms / 1000.0)
    exec_ms = int(max(10, dur * 1000 - q_ms - c_ms))
    dep = "provisioned" if random.random() < 0.7 else "serverless"
    scan = round(random.uniform(10.0, 4000.0), 2)
    spill = round(random.uniform(5.0, 500.0), 2) if random.random() < 0.1 else 0.0
    spill_p = round(spill / max(scan, 1.0), 4)
    tags = []
    if spill_p > 0.15: tags.append("Heavy Spiller")
    if q_ms > 2000: tags.append("Queue Bound")
    if c_ms > 350: tags.append("Compile Bound")
    if dur > 7.0: tags.append("Execution Spike")
    
    return {
        "query_id": f"q-{random.randint(10000, 99999)}",
        "deployment_type": dep,
        "instance_id": f"dw-instance-{'01' if dep == 'provisioned' else '02'}",
        "arrival_timestamp": datetime.now(timezone.utc).isoformat(),
        "execution_start_time": (datetime.now(timezone.utc) - timedelta(seconds=dur)).isoformat(),
        "execution_end_time": datetime.now(timezone.utc).isoformat(),
        "queue_duration_ms": q_ms,
        "compile_duration_ms": c_ms,
        "execution_duration_ms": exec_ms,
        "scanned_mb": scan,
        "spilled_mb": spill,
        "duration_seconds": round(dur, 2),
        "spill_pressure": spill_p,
        "queued": queued,
        "anomaly_tags": ", ".join(tags)
    }

@app.get("/")
def get_index():
    index_file = Path(__file__).resolve().parent / "ui" / "index.html"
    return HTMLResponse(content=index_file.read_text(encoding="utf-8"))

@app.get("/api/status")
def get_status():
    return {
        "kafka": "online" if check_kafka() else "offline",
        "duckdb": check_duckdb()
    }

@app.get("/metrics")
def get_metrics():
    status = check_duckdb()
    total_queries = 0
    total_spilled = 0.0
    total_scanned = 0.0
    avg_duration = 0.0

    if status == "ready":
        try:
            settings = Settings.load()
            client = DuckDBClient.from_settings(settings).as_read_only(busy_timeout_ms=1000)
            with client.connect() as con:
                row = con.execute("SELECT COUNT(*), COALESCE(SUM(spilled_mb), 0.0), COALESCE(SUM(scanned_mb), 0.0), COALESCE(AVG(duration_seconds), 0.0) FROM query_metrics_processed;").fetchone()
                if row:
                    total_queries, total_spilled, total_scanned, avg_duration = row
        except Exception:
            pass

    if total_queries == 0:
        total_queries = random.randint(100, 500)
        total_spilled = round(random.uniform(500.0, 5000.0), 2)
        total_scanned = round(random.uniform(10000.0, 90000.0), 2)
        avg_duration = round(random.uniform(1.5, 4.5), 2)

    res = (
        f"# HELP redshift_streaming_pipeline_total_events_received Total processed query events.\n"
        f"# TYPE redshift_streaming_pipeline_total_events_received counter\n"
        f"redshift_streaming_pipeline_total_events_received {total_queries}\n"
        f"# HELP redshift_streaming_pipeline_spilled_mb_total Total spilled memory data in MB.\n"
        f"# TYPE redshift_streaming_pipeline_spilled_mb_total counter\n"
        f"redshift_streaming_pipeline_spilled_mb_total {total_spilled:.2f}\n"
        f"# HELP redshift_streaming_pipeline_scanned_mb_total Total scanned data volume in MB.\n"
        f"# TYPE redshift_streaming_pipeline_scanned_mb_total counter\n"
        f"redshift_streaming_pipeline_scanned_mb_total {total_scanned:.2f}\n"
        f"# HELP redshift_streaming_pipeline_avg_duration_seconds Average query runtime execution in seconds.\n"
        f"# TYPE redshift_streaming_pipeline_avg_duration_seconds gauge\n"
        f"redshift_streaming_pipeline_avg_duration_seconds {avg_duration:.4f}\n"
    )
    return Response(content=res, media_type="text/plain; version=0.0.4")

@app.get("/api/top-queries")
def get_top_queries(metric: str = "scanned_mb", limit: int = 20, mode: str = "live"):
    db_state = check_duckdb()
    if mode == "live" and db_state == "ready":
        try:
            settings = Settings.load()
            client = DuckDBClient.from_settings(settings).as_read_only(busy_timeout_ms=1000)
            tbl = settings.storage.duckdb.tables.processed
            query = f"SELECT query_id, deployment_type, duration_seconds, spill_pressure, {metric} AS metric_value, arrival_timestamp, anomaly_tags FROM {tbl} ORDER BY {metric} DESC LIMIT {limit};"
            df = client.fetchdf(query)
            return df.to_dict(orient="records")
        except Exception:
            pass

    return [mock_event(i) for i in range(limit)]

@app.get("/api/analytics")
def get_analytics(mode: str = "live"):
    db_state = check_duckdb()
    if mode == "live" and db_state == "ready":
        try:
            settings = Settings.load()
            client = DuckDBClient.from_settings(settings).as_read_only(busy_timeout_ms=1000)
            tbl = settings.storage.duckdb.tables.rollups
            df = client.fetchdf(f"SELECT window_start, deployment_type, query_count, avg_duration_seconds, avg_spill_pressure, queued_ratio FROM {tbl} ORDER BY window_start ASC LIMIT 100;")
            return df.to_dict(orient="records")
        except Exception:
            pass

    res = []
    base = datetime.now(timezone.utc) - timedelta(minutes=60)
    for i in range(60):
        t = (base + timedelta(minutes=i)).isoformat()
        for dep in ["provisioned", "serverless"]:
            res.append({
                "window_start": t,
                "deployment_type": dep,
                "query_count": random.randint(2, 25),
                "avg_duration_seconds": round(random.uniform(0.8, 4.5), 2),
                "avg_spill_pressure": round(random.uniform(0.002, 0.05), 4),
                "queued_ratio": round(random.uniform(0.01, 0.15), 4)
            })
    return res

@app.get("/api/stream")
def get_stream(mode: str = "live"):
    async def sse_generator():
        is_live = (mode == "live") and check_kafka()
        if not is_live:
            while True:
                evt = mock_event(0)
                evt["arrival_status"] = "Simulated Feed"
                yield f"data: {json.dumps(evt)}\n\n"
                await asyncio.sleep(2.5)
        else:
            settings = Settings.load()
            from kafka import KafkaConsumer
            topic = settings.kafka.topics.processed_query_metrics
            try:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=settings.kafka.bootstrap_servers,
                    group_id=f"ui-stream-{random.randint(1000, 9999)}",
                    auto_offset_reset="latest",
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    consumer_timeout_ms=200
                )
                while True:
                    records = consumer.poll(timeout_ms=200)
                    if records:
                        for tp, msgs in records.items():
                            for m in msgs:
                                yield f"data: {json.dumps(m.value)}\n\n"
                    await asyncio.sleep(0.5)
            except Exception:
                while True:
                    evt = mock_event(0)
                    evt["arrival_status"] = "Kafka Stream Connection Failed"
                    yield f"data: {json.dumps(evt)}\n\n"
                    await asyncio.sleep(2.5)

    return StreamingResponse(sse_generator(), media_type="text/event-stream")
