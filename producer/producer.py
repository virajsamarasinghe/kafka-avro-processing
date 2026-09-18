import argparse
import logging
import random
import time
from pathlib import Path

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders"
PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def load_schema() -> str:
    schema_path = Path(__file__).resolve().parent.parent / "schemas" / "order.avsc"
    return schema_path.read_text()


def make_order(order_id: str, poison: bool = False) -> dict:
    if poison:
        # Intentionally invalid price - the consumer treats this as a
        # permanent failure and routes it straight to the DLQ.
        return {"orderId": order_id, "product": random.choice(PRODUCTS), "price": -1.0}
    return {
        "orderId": order_id,
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5.0, 500.0), 2),
    }


def delivery_report(err, msg):
    if err is not None:
        log.error(f"Delivery failed for order {msg.key()}: {err}")
    else:
        log.info(f"Delivered order {msg.key()} to {msg.topic()} [partition {msg.partition()}]")


def main():
    parser = argparse.ArgumentParser(description="Order producer")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between orders")
    parser.add_argument(
        "--poison-every",
        type=int,
        default=7,
        help="Inject an invalid order every N messages (0 disables)",
    )
    args = parser.parse_args()

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry_client, load_schema())

    producer = SerializingProducer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
        }
    )

    order_num = 1000
    try:
        while True:
            order_num += 1
            poison = args.poison_every > 0 and order_num % args.poison_every == 0
            order = make_order(str(order_num), poison=poison)

            producer.produce(
                topic=TOPIC,
                key=order["orderId"],
                value=order,
                on_delivery=delivery_report,
            )
            producer.poll(0)

            if poison:
                log.warning(f"Injected poison order {order}")

            time.sleep(args.interval)
    except KeyboardInterrupt:
        log.info("Shutting down producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
