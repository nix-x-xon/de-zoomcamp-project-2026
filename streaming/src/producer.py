"""Producer: polls PSE real-time demand and publishes to Kafka with Avro serialization.

PSE publishes KSE demand on 15-minute cadence. This producer polls every POLL_INTERVAL
seconds (default 60) and emits only new intervals it hasn't seen.

Env:
    KAFKA_BOOTSTRAP        default localhost:9092
    SCHEMA_REGISTRY_URL    default http://localhost:8081
    TOPIC                  default pse.demand.v1
    POLL_INTERVAL          default 60 (seconds)
"""
from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path

import httpx
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

PSE_URL = "https://api.raporty.pse.pl/api/his-wlk-cal"
SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "pse_demand.avsc"


def fetch_today() -> list[dict]:
    today = date.today().isoformat()
    resp = httpx.get(PSE_URL, params={"$filter": f"business_date eq '{today}'"}, timeout=30)
    resp.raise_for_status()
    return resp.json().get("value", [])


def _interval_id(dtime_str: str) -> int:
    t = datetime.fromisoformat(dtime_str)
    return (t.hour * 60 + t.minute) // 15 or 96


def to_event(record: dict) -> dict:
    ts = datetime.fromisoformat(record["dtime"]).replace(tzinfo=timezone.utc)
    return {
        "event_ts": int(ts.timestamp() * 1000),
        "demand_date": ts.date(),
        "interval_id": _interval_id(record["dtime"]),
        "demand_mw": float(record["demand"]),
        "source": "PSE",
        "ingested_at": int(datetime.now(timezone.utc).timestamp() * 1000),
    }


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    sr_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    topic = os.getenv("TOPIC", "pse.demand.v1")
    poll_interval = int(os.getenv("POLL_INTERVAL", "60"))

    schema_str = SCHEMA_PATH.read_text()
    sr_client = SchemaRegistryClient({"url": sr_url})
    avro_ser = AvroSerializer(sr_client, schema_str)
    key_ser = StringSerializer("utf_8")

    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 100})

    seen: set[int] = set()

    def on_delivery(err, msg):
        if err:
            print(f"[producer] delivery failed: {err}")
        else:
            print(f"[producer] {msg.topic()}[{msg.partition()}]@{msg.offset()}")

    print(f"[producer] starting → {bootstrap} topic={topic}")
    while True:
        try:
            for raw in fetch_today():
                interval = _interval_id(raw["dtime"])
                if interval in seen:
                    continue
                event = to_event(raw)
                key = f"{event['demand_date']}-{event['interval_id']}"
                producer.produce(
                    topic=topic,
                    key=key_ser(key, SerializationContext(topic, MessageField.KEY)),
                    value=avro_ser(event, SerializationContext(topic, MessageField.VALUE)),
                    on_delivery=on_delivery,
                )
                seen.add(interval)
            producer.poll(0)
        except Exception as exc:
            print(f"[producer] poll error: {exc}")
        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
