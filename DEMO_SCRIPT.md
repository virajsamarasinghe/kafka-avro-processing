# Live Demo Script


Run through this top to bottom. Keep 4 windows visible: 3 terminals + browser.

## 0. Before you start (do this before the evaluator arrives)

- [ ] `docker compose up -d` — start the stack early so Kafka is warm
- [ ] `docker compose ps` — confirm all services show healthy/running
- [ ] Activate the venv: `.venv\Scripts\activate`
- [ ] Open browser tab to `http://localhost:8080` (Kafka UI), don't navigate yet
- [ ] Arrange 3 terminal windows side by side: **Consumer**, **DLQ Monitor**, **Producer**

If asked "is this reproducible / did you set it up in advance": yes — say so.
The stack starting up is infra, not the part being evaluated.

---

## 1. Explain the architecture (30 sec, before running anything)

Say this, pointing at `README.md`'s diagram:

> "Producer generates orders, serializes them as Avro using a schema registered
> in Confluent Schema Registry, and publishes to the `orders` topic. The
> consumer deserializes each message, keeps a running average of prices, and
> retries transient failures with exponential backoff. Anything that's
> permanently invalid, or exhausts its retries, goes to `orders.dlq`, which a
> separate DLQ monitor watches."

## 2. Show the schema and topics (1 min)

- [ ] Open `schemas/order.avsc` — point out `orderId`/`product`/`price` fields
- [ ] In Kafka UI → **Topics**, show `orders` and `orders.dlq` already exist
      (created by the `kafka-init` one-shot job)
- [ ] In Kafka UI → **Schema Registry**, show the registered `Order` schema
      (this appears only after the producer's first run — see step 3)

## 3. Start the consumer (Terminal 1)

```powershell
python consumer/consumer.py
```

- [ ] Say: "Consumer is up, subscribed to `orders`, waiting for messages."

## 4. Start the DLQ monitor (Terminal 2)

```powershell
python dlq_monitor/dlq_consumer.py
```

- [ ] Say: "This just watches `orders.dlq` — nothing there yet."

## 5. Start the producer (Terminal 3)

```powershell
python producer/producer.py --interval 1 --poison-every 7
```

- [ ] Point at Terminal 1 (consumer) as messages start flowing:
  ```
  Processed order 1001 product=Item3 price=123.45 running_avg=123.45 (n=1)
  Processed order 1002 product=Item1 price=88.10 running_avg=105.78 (n=2)
  ```
  Say: **"This is the real-time aggregation — running average recalculated
  on every message, in memory."**

## 6. Point out a retry live (happens ~15% of messages)

Watch Terminal 1 for a line like:

```
Transient failure on order 1005 (attempt 1/3): simulated transient downstream failure. Retrying in 0.20s
Processed order 1005 product=Item2 price=45.00 running_avg=98.12 (n=5)
```

- [ ] Say: **"That's the retry logic — transient failure, exponential
  backoff (0.2s, 0.4s, 0.8s), succeeds on retry, no message lost."**

If none shows up in the first ~20s (it's random, ~15% chance per message),
just keep talking through step 7 — one will appear within a few messages.

## 7. Point out the DLQ path (guaranteed every 7th order)

Terminal 3 (producer) shows:
```
WARNING Injected poison order {'orderId': '1007', 'product': 'Item4', 'price': -1.0}
```

Terminal 1 (consumer) shows:
```
ERROR Order 1007 sent to DLQ: invalid price -1.0
```

Terminal 2 (DLQ monitor) shows:
```
WARNING DLQ message: order={'orderId': '1007', 'product': 'Item4', 'price': -1.0} reason='invalid price -1.0'
```

- [ ] Say: **"Negative price is a permanent validation failure — no amount
  of retrying fixes bad data, so it goes straight to the DLQ with a
  `failure-reason` header, and the DLQ monitor picks it up immediately."**

## 8. Show it in Kafka UI (1 min)

- [ ] Topics → `orders` → Messages — show live Avro-decoded messages
- [ ] Topics → `orders.dlq` → Messages — show the poison order sitting there
      with its header
- [ ] Consumer Groups → show `order-processing-group` and `dlq-monitor-group`
      both active with committed offsets advancing

## 9. Anticipate these questions

| Likely question | Answer |
|---|---|
| Why is the transient failure random instead of a real downstream call? | It's a simulated flaky dependency (`TRANSIENT_FAILURE_RATE = 0.15` in `consumer.py`) so retry behavior is demonstrable on demand without standing up a real downstream service. |
| What happens if the consumer crashes mid-message? | Offsets are committed manually only *after* a message is fully handled (processed or routed to DLQ) — `enable.auto.commit=False` — so a crash re-delivers the in-flight message, nothing is silently dropped. |
| Where does the running average live / does it survive a restart? | In-memory in the consumer process (`RunningAverage` class) — resets on restart. Documented as a known simplification. |
| Why Avro + Schema Registry instead of plain JSON? | Schema is centrally validated and versioned; producer/consumer agree on the contract via the registry instead of trusting hand-written parsing. |
| How would you scale this / add partitions? | Kafka partition count + consumer group semantics — more consumers in `order-processing-group` would split partitions automatically; running average would need to move to a shared store (e.g. Redis) since it's per-process today. |

## 10. Shut down cleanly

```powershell
# Ctrl+C in each of the 3 terminals (producer, consumer, dlq_monitor)
docker compose down
```
