import base64
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any

import redis
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from pydantic import BaseModel, Field


app = FastAPI(title="RADAR Event Gateway")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id="radar-admin",
)


redis_client = redis.Redis(
    host="localhost",
    port=6380,
    db=0,
    decode_responses=True,
)


DEFAULT_LIMIT_PER_MINUTE = 10000
MAX_LIMIT_PER_MINUTE = 10000
MIN_LIMIT_PER_MINUTE = 50
STATIC_LIMIT_PER_MINUTE = 1000
EXPERIMENT_MODE_KEY = "radar:experiment:mode"
VALID_EXPERIMENT_MODES = {"none", "static", "storage_only", "radar"}
EXPERIMENT_RUNS_KEY = "radar:experiment:runs"
CLICKHOUSE_URL = "http://localhost:8123/"
CLICKHOUSE_USER = "radar_user"
CLICKHOUSE_PASSWORD = "radar_pass"


class Event(BaseModel):
    client_id: str = Field(..., example="seller_12")
    event_type: str = Field(..., example="order_created")
    entity_id: str = Field(..., example="ORD101")
    event_time: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    use_case_id: str | None = None
    source_topic: str | None = None


class UseCaseIngestRequest(BaseModel):
    use_case_name: str = Field(..., example="Replay Attack Demo")
    events: list[Event]
    experiment_mode: str | None = Field(default=None, example="radar")


class ExperimentModeRequest(BaseModel):
    mode: str = Field(..., example="radar")



def sanitize_use_case_topic(name: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_-]+", "_", name.strip().lower()).strip("_")
    if not normalized:
        normalized = "upload"
    return f"usecase_{normalized[:80]}"


def ensure_topic(topic: str) -> None:
    try:
        admin_client.create_topics([
            NewTopic(name=topic, num_partitions=4, replication_factor=1)
        ], validate_only=False)
    except TopicAlreadyExistsError:
        return
    except Exception as exc:
        if "TopicAlreadyExists" not in str(exc):
            raise


def prepare_event_data(event: Event, topic: str | None = None, use_case_id: str | None = None) -> dict[str, Any]:
    event_data = event.model_dump()
    if event_data["event_time"] is None:
        event_data["event_time"] = datetime.now(timezone.utc).isoformat()
    if use_case_id is not None:
        event_data["use_case_id"] = use_case_id
    if topic is not None:
        event_data["source_topic"] = topic
    return event_data

def normalize_experiment_mode(mode: str | None) -> str:
    normalized = (mode or "radar").strip().lower()
    if normalized not in VALID_EXPERIMENT_MODES:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_experiment_mode",
                "valid_modes": sorted(VALID_EXPERIMENT_MODES),
            },
        )
    return normalized


def get_experiment_mode() -> str:
    return normalize_experiment_mode(redis_client.get(EXPERIMENT_MODE_KEY) or "radar")


def set_experiment_mode(mode: str) -> str:
    normalized = normalize_experiment_mode(mode)
    redis_client.set(EXPERIMENT_MODE_KEY, normalized)
    return normalized

def limit_key(client_id: str) -> str:
    return f"radar:v2:limit:{client_id}"


def rate_key(client_id: str, mode: str = "radar") -> str:
    return f"rate:v2:{mode}:{client_id}"


def get_client_limit(client_id: str, mode: str | None = None) -> int:
    active_mode = normalize_experiment_mode(mode or get_experiment_mode())

    if active_mode in {"none", "storage_only"}:
        return DEFAULT_LIMIT_PER_MINUTE
    if active_mode == "static":
        return STATIC_LIMIT_PER_MINUTE

    limit = redis_client.get(limit_key(client_id))
    if limit is None:
        return DEFAULT_LIMIT_PER_MINUTE
    return max(MIN_LIMIT_PER_MINUTE, int(float(limit)))


def allow_request(client_id: str, mode: str | None = None) -> tuple[bool, dict[str, Any]]:
    active_mode = normalize_experiment_mode(mode or get_experiment_mode())

    if active_mode in {"none", "storage_only"}:
        return True, {
            "mode": active_mode,
            "strategy": "bypass_rate_limit",
            "limit_per_minute": "unlimited",
            "bucket_capacity": "unlimited",
            "tokens_remaining": "unlimited",
        }

    now = time.time()
    key = rate_key(client_id, active_mode)
    limit_per_minute = get_client_limit(client_id, active_mode)
    bucket_capacity = max(1, limit_per_minute)

    bucket = redis_client.hgetall(key)

    if not bucket:
        tokens = bucket_capacity
        last_refill = now
    else:
        tokens = min(float(bucket.get("tokens", bucket_capacity)), bucket_capacity)
        last_refill = float(bucket.get("last_refill", now))

    refill_rate_per_second = limit_per_minute / 60.0
    elapsed = now - last_refill
    tokens = min(bucket_capacity, tokens + elapsed * refill_rate_per_second)

    allowed = tokens >= 1

    if allowed:
        tokens -= 1

    redis_client.hset(key, mapping={
        "mode": active_mode,
        "tokens": tokens,
        "last_refill": now,
        "limit_per_minute": limit_per_minute,
        "bucket_capacity": bucket_capacity,
    })
    redis_client.expire(key, 3600)

    return allowed, {
        "mode": active_mode,
        "strategy": "static_token_bucket" if active_mode == "static" else "radar_adaptive_token_bucket",
        "tokens_remaining": round(tokens, 2),
        "limit_per_minute": limit_per_minute,
        "bucket_capacity": bucket_capacity,
    }


def run_key(topic: str) -> str:
    return f"radar:experiment:run:{topic}"


def save_experiment_run(summary: dict[str, Any]) -> None:
    topic = str(summary["topic"])
    redis_client.hset(run_key(topic), mapping={
        key: json.dumps(value) if isinstance(value, (dict, list)) else value
        for key, value in summary.items()
    })
    redis_client.sadd(EXPERIMENT_RUNS_KEY, topic)
    redis_client.expire(run_key(topic), 86400)


def get_experiment_runs() -> list[dict[str, Any]]:
    runs = []
    for topic in sorted(redis_client.smembers(EXPERIMENT_RUNS_KEY)):
        row = redis_client.hgetall(run_key(topic))
        if not row:
            continue
        for numeric_key in ["total_events", "accepted", "rejected"]:
            if numeric_key in row:
                row[numeric_key] = int(float(row[numeric_key]))
        runs.append(row)
    return runs

def clickhouse_query(query: str) -> list[dict[str, Any]]:
    params = urllib.parse.urlencode({"database": "radar"})
    auth = base64.b64encode(f"{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}".encode("utf-8")).decode("ascii")
    request = urllib.request.Request(
        f"{CLICKHOUSE_URL}?{params}",
        data=query.encode("utf-8"),
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "text/plain; charset=utf-8",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            body = response.read().decode("utf-8").strip()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise HTTPException(status_code=502, detail={"error": "clickhouse_query_failed", "message": detail}) from exc
    except urllib.error.URLError as exc:
        raise HTTPException(status_code=502, detail={"error": "clickhouse_unavailable", "message": str(exc)}) from exc

    if not body:
        return []

    return [json.loads(line) for line in body.splitlines() if line.strip()]


def get_client_metrics() -> list[dict[str, Any]]:
    query = """
    SELECT
        use_case_topic,
        client_id,
        raw_total AS raw_events,
        unique_total AS unique_events,
        duplicate_total AS duplicate_events,
        if(raw_total = 0, 0, duplicate_total / raw_total) AS duplicate_ratio
    FROM
    (
        SELECT
            use_case_topic,
            client_id,
            sum(raw_events) AS raw_total,
            sum(unique_events) AS unique_total,
            sum(duplicate_events) AS duplicate_total
        FROM client_event_metrics
        WHERE batch_time >= now() - INTERVAL 30 MINUTE
        GROUP BY use_case_topic, client_id
    )
    ORDER BY use_case_topic, client_id
    FORMAT JSONEachRow
    """
    return clickhouse_query(query)


def radar_limit_from_duplicate_ratio(duplicate_ratio: float) -> int:
    risk = max(0.0, min(1.0, duplicate_ratio))

    if risk < 0.05:
        return DEFAULT_LIMIT_PER_MINUTE
    if risk < 0.30:
        return 3000
    if risk < 0.70:
        return 500
    return MIN_LIMIT_PER_MINUTE


@app.get("/")
def health():
    return {
        "status": "ok",
        "service": "radar-event-gateway",
        "experiment_mode": get_experiment_mode(),
    }


@app.post("/events")
def publish_event(event: Event):
    mode = get_experiment_mode()
    allowed, rate_info = allow_request(event.client_id, mode)

    if not allowed:
        raise HTTPException(
            status_code=429,
            detail={
                "error": "rate_limit_exceeded",
                "client_id": event.client_id,
                "rate_info": rate_info,
            },
        )

    event_data = prepare_event_data(event, topic="raw_events", use_case_id=event.use_case_id)
    producer.send("raw_events", event_data)
    producer.flush()

    return {
        "status": "accepted",
        "topic": "raw_events",
        "experiment_mode": mode,
        "rate_info": rate_info,
        "event": event_data,
    }


@app.post("/usecases/ingest")
def ingest_use_case(payload: UseCaseIngestRequest):
    mode = normalize_experiment_mode(payload.experiment_mode or get_experiment_mode())
    topic = sanitize_use_case_topic(payload.use_case_name)
    ensure_topic(topic)

    accepted = 0
    rejected = 0
    rejections = []

    for event in payload.events:
        allowed, rate_info = allow_request(event.client_id, mode)
        if not allowed:
            rejected += 1
            rejections.append({
                "client_id": event.client_id,
                "entity_id": event.entity_id,
                "rate_info": rate_info,
            })
            continue

        event_data = prepare_event_data(event, topic=topic, use_case_id=payload.use_case_name)
        event_data["experiment_mode"] = mode
        producer.send(topic, event_data)
        accepted += 1

    producer.flush()

    return {
        "status": "ingested",
        "use_case_name": payload.use_case_name,
        "topic": topic,
        "experiment_mode": mode,
        "protection_strategy": "bypass" if mode in {"none", "storage_only"} else ("static_token_bucket" if mode == "static" else "radar_adaptive_token_bucket"),
        "total_events": len(payload.events),
        "accepted": accepted,
        "rejected": rejected,
        "rejections": rejections[:20],
    }


@app.get("/ratelimit/{client_id}")
def get_rate_limit(client_id: str):
    return {
        "client_id": client_id,
        "experiment_mode": get_experiment_mode(),
        "configured_limit_per_minute": get_client_limit(client_id),
        "radar_limit_per_minute": get_client_limit(client_id, "radar"),
        "static_limit_per_minute": STATIC_LIMIT_PER_MINUTE,
        "radar_bucket": redis_client.hgetall(rate_key(client_id, "radar")),
        "static_bucket": redis_client.hgetall(rate_key(client_id, "static")),
    }


@app.get("/experiment/mode")
def experiment_mode():
    return {
        "mode": get_experiment_mode(),
        "valid_modes": sorted(VALID_EXPERIMENT_MODES),
        "static_limit_per_minute": STATIC_LIMIT_PER_MINUTE,
        "default_limit_per_minute": DEFAULT_LIMIT_PER_MINUTE,
    }


@app.post("/experiment/mode")
def update_experiment_mode(payload: ExperimentModeRequest):
    mode = set_experiment_mode(payload.mode)
    return {
        "mode": mode,
        "valid_modes": sorted(VALID_EXPERIMENT_MODES),
        "static_limit_per_minute": STATIC_LIMIT_PER_MINUTE,
        "default_limit_per_minute": DEFAULT_LIMIT_PER_MINUTE,
    }

@app.get("/experiment/runs")
def experiment_runs():
    return {"runs": get_experiment_runs()}

@app.get("/radar/metrics")
def radar_metrics():
    return {"metrics": get_client_metrics()}


@app.post("/radar/update-limits")
def radar_update_limits():
    metrics = get_client_metrics()
    decisions = []

    for row in metrics:
        client_id = row["client_id"]
        duplicate_ratio = float(row["duplicate_ratio"])
        new_limit = radar_limit_from_duplicate_ratio(duplicate_ratio)

        redis_client.set(limit_key(client_id), new_limit, ex=3600)

        decisions.append({
            "client_id": client_id,
            "raw_events": int(row["raw_events"]),
            "unique_events": int(row["unique_events"]),
            "duplicate_events": int(row["duplicate_events"]),
            "duplicate_ratio": round(duplicate_ratio, 4),
            "new_limit_per_minute": new_limit,
            "use_case_topic": row.get("use_case_topic", "unknown"),
        })

    return {"updated_clients": len(decisions), "decisions": decisions}















