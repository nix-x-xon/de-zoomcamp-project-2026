"""Consumer: reads pse.demand.v1 from Kafka and streams rows into BigQuery.

Sink table:  {GCP_PROJECT}.energy_raw.pse_demand_stream
             (partitioned by DATE(event_ts), clustered by interval_id)

The consumer batches messages (default 200) before flushing to BQ to reduce per-row cost.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from google.cloud import bigquery

SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "pse_demand.avsc"


def ensure_table(client: bigquery.Client, table_id: str) -> None:
    schema = [
        bigquery.SchemaField("event_ts", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("demand_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("interval_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("demand_mw", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="event_ts"
    )
    table.clustering_fields = ["interval_id"]
    client.create_table(table, exists_ok=True)


def to_bq_row(event: dict) -> dict:
    return {
        "event_ts": datetime.fromtimestamp(event["event_ts"] / 1000, tz=timezone.utc).isoformat(),
        "demand_date": event["demand_date"].isoformat()
            if hasattr(event["demand_date"], "isoformat") else event["demand_date"],
        "interval_id": event["interval_id"],
        "demand_mw": event["demand_mw"],
        "source": event.get("source"),
        "ingested_at": datetime.fromtimestamp(
            event["ingested_at"] / 1000, tz=timezone.utc
        ).isoformat(),
    }


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    sr_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    topic = os.getenv("TOPIC", "pse.demand.v1")
    group = os.getenv("GROUP_ID", "bq-sink-v1")
    batch_size = int(os.getenv("BATCH_SIZE", "200"))
    project = os.environ["GCP_PROJECT"]
    table_id = f"{project}.energy_raw.pse_demand_stream"

    sr_client = SchemaRegistryClient({"url": sr_url})
    avro_de = AvroDeserializer(sr_client, SCHEMA_PATH.read_text())

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])

    bq = bigquery.Client(project=project)
    ensure_table(bq, table_id)

    buffer: list[dict] = []
    print(f"[consumer] subscribed to {topic}, sinking to {table_id}")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[consumer] error: {msg.error()}")
                continue
            event = avro_de(msg.value(), SerializationContext(topic, MessageField.VALUE))
            buffer.append(to_bq_row(event))
            if len(buffer) >= batch_size:
                errors = bq.insert_rows_json(table_id, buffer)
                if errors:
                    print(f"[consumer] BQ errors: {errors}")
                else:
                    print(f"[consumer] flushed {len(buffer)} rows → {table_id}")
                    consumer.commit(asynchronous=False)
                buffer.clear()
    except KeyboardInterrupt:
        print("[consumer] shutting down")
    finally:
        if buffer:
            bq.insert_rows_json(table_id, buffer)
            consumer.commit(asynchronous=False)
        consumer.close()


if __name__ == "__main__":
    main()
