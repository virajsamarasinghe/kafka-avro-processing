import argparse
import logging
import random
import time
from pathlib import Path

from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("consumer")

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders"
DLQ_TOPIC = "orders.dlq"
GROUP_ID = "order-processing-group"
MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 0.2
TRANSIENT_FAILURE_RATE = 0.15  # simulates an occasional flaky downstream dependency


def load_schema() -> str:
    schema_path = Path(__file__).resolve().parent.parent / "schemas" / "order.avsc"
    return schema_path.read_text()


class RunningAverage:
    """Tracks a real-time running average of order prices."""

    def __init__(self):
        self.count = 0
        self.total = 0.0

    def update(self, price: float) -> float:
        self.count += 1
        self.total += price
        return self.total / self.count


class TransientProcessingError(Exception):
    """Simulated transient failure - safe to retry."""


class PermanentProcessingError(Exception):
    """Non-retryable failure - goes straight to the DLQ."""


def process_order(order: dict) -> float:
    price = order["price"]
    if price < 0:
        raise PermanentProcessingError(f"invalid price {price}")
    if random.random() < TRANSIENT_FAILURE_RATE:
        raise TransientProcessingError("simulated transient downstream failure")
    return price


def build_dlq_producer() -> SerializingProducer:
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry_client, load_schema())
    return SerializingProducer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
        }
    )


def send_to_dlq(dlq_producer: SerializingProducer, order: dict, reason: str):
    dlq_producer.produce(
        topic=DLQ_TOPIC,
        key=order["orderId"],
        value=order,
        headers=[("failure-reason", reason.encode("utf-8"))],
    )
    dlq_producer.poll(0)
    log.error(f"Order {order['orderId']} sent to DLQ: {reason}")


def handle_order(order: dict, running_avg: RunningAverage, dlq_producer: SerializingProducer):
    attempt = 0
    while True:
        try:
            price = process_order(order)
            avg = running_avg.update(price)
            log.info(
                f"Processed order {order['orderId']} product={order['product']} "
                f"price={price:.2f} running_avg={avg:.2f} (n={running_avg.count})"
            )
            return
        except TransientProcessingError as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                send_to_dlq(dlq_producer, order, f"exhausted {MAX_RETRIES} retries: {e}")
                return
            backoff = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
            log.warning(
                f"Transient failure on order {order['orderId']} "
                f"(attempt {attempt}/{MAX_RETRIES}): {e}. Retrying in {backoff:.2f}s"
            )
            time.sleep(backoff)
        except PermanentProcessingError as e:
            send_to_dlq(dlq_producer, order, str(e))
            return


def main():
    parser = argparse.ArgumentParser(description="Order consumer")
    parser.parse_args()

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client, load_schema())

    consumer = DeserializingConsumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": avro_deserializer,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    dlq_producer = build_dlq_producer()
    running_avg = RunningAverage()

    log.info(f"Consuming from '{TOPIC}', DLQ is '{DLQ_TOPIC}'...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error(f"Consumer error: {msg.error()}")
                continue

            order = msg.value()
            if order is None:
                consumer.commit(msg)
                continue

            handle_order(order, running_avg, dlq_producer)
            consumer.commit(msg)
    except KeyboardInterrupt:
        log.info("Shutting down consumer...")
    finally:
        dlq_producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()
