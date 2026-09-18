import logging
from pathlib import Path

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dlq-monitor")

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
DLQ_TOPIC = "orders.dlq"


def load_schema() -> str:
    schema_path = Path(__file__).resolve().parent.parent / "schemas" / "order.avsc"
    return schema_path.read_text()


def main():
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client, load_schema())

    consumer = DeserializingConsumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": avro_deserializer,
            "group.id": "dlq-monitor-group",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([DLQ_TOPIC])

    log.info(f"Watching '{DLQ_TOPIC}' for permanently failed messages...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error(f"Consumer error: {msg.error()}")
                continue

            reason = "unknown"
            for key, value in msg.headers() or []:
                if key == "failure-reason":
                    reason = value.decode("utf-8")

            log.warning(f"DLQ message: order={msg.value()} reason='{reason}'")
            consumer.commit(msg)
    except KeyboardInterrupt:
        log.info("Shutting down DLQ monitor...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
