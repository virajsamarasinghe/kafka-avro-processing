# Kafka + Avro Order Processing

A Kafka-based system that produces and consumes order messages using Avro
serialization, with real-time price aggregation, retry logic for transient
failures, and a Dead Letter Queue (DLQ) for permanently failed messages.

## Architecture

```
[producer] --Avro--> [orders topic] --> [consumer] --> running average
                                              |
                                    (retries exhausted /
                                     permanent failure)
                                              v
                                       [orders.dlq topic] --> [dlq_monitor]
```

- **Schema**: `schemas/order.avsc` (`orderId: string`, `product: string`, `price: float`)
- **Schema Registry**: Confluent Schema Registry manages/validates the Avro schema centrally
- **Retry logic**: transient failures are retried up to 3 times with exponential backoff
- **DLQ**: messages that are permanently invalid (e.g. negative price) or exhaust
  their retries are published to `orders.dlq` with a `failure-reason` header

## Prerequisites

- Docker Desktop
- Python 3.9+

## 1. Start the Kafka stack

```powershell
docker compose up -d
```

This starts:

| Service | Purpose | URL |
|---|---|---|
| `kafka` | Kafka broker (KRaft mode, no ZooKeeper) | `localhost:9092` |
| `schema-registry` | Confluent Schema Registry | `http://localhost:8081` |
| `kafka-ui` | Web UI to browse topics/messages/consumer groups | `http://localhost:8080` |
| `kafka-init` | One-shot job that creates the `orders` and `orders.dlq` topics | — |

Check everything is healthy:

```powershell
docker compose ps
```

## 2. Install Python dependencies

Each component has its own `requirements.txt`. From the repo root:

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r producer/requirements.txt -r consumer/requirements.txt -r dlq_monitor/requirements.txt
```

## 3. Run the system

Open three terminals (with the venv activated in each):

```powershell
# Terminal 1 - consumer (aggregates + retries + DLQ routing)
python consumer/consumer.py

# Terminal 2 - DLQ monitor (shows permanently failed messages)
python dlq_monitor/dlq_consumer.py

# Terminal 3 - producer (generates orders, one every second)
python producer/producer.py --interval 1 --poison-every 7
```

You should see:
- The consumer logging each processed order with an updated running average
- Occasional `Transient failure ... Retrying in Xs` warnings that resolve on retry
- Every 7th order is an intentionally invalid ("poison") order — the consumer
  detects the invalid price and routes it straight to the DLQ, which the
  `dlq_monitor` picks up and prints

## 4. Inspect via Kafka UI

Open `http://localhost:8080` to see the `orders` and `orders.dlq` topics,
browse individual (Avro-decoded) messages, and check the registered schema
under **Schema Registry**.

## Design notes

- **Retry vs DLQ classification**: a negative `price` is treated as a permanent
  validation failure (no amount of retrying fixes bad data); a randomly
  injected "downstream hiccup" is treated as transient and retried with
  exponential backoff (`0.2s, 0.4s, 0.8s`) before falling back to the DLQ.
- **Running average** is kept in-memory in the consumer process for
  simplicity; it resets on restart.
- Offsets are committed manually, after a message either succeeds or is
  routed to the DLQ, so no message is skipped or reprocessed on a clean run.

## Stopping

```powershell
docker compose down
```
