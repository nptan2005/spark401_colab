import argparse
import json
import random
import string
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

CHANNELS = ["ECOM", "POS", "ATM"]
COUNTRIES = ["VN", "TH", "MY", "SG"]
STATUSES = ["SUCCESS", "FAILED", "REVERSED"]

def rand_id(prefix: str, n: int) -> str:
    return prefix + "".join(random.choices(string.digits, k=n))

def iso_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")

def make_event(order_id: str | None = None) -> dict:
    now = datetime.now(timezone.utc).astimezone()
    event_dt = now - timedelta(minutes=random.randint(0, 180))
    oid = order_id or rand_id("o", 7)
    return {
        "order_id": oid,
        "customer_id": rand_id("c", 5),
        "merchant_id": rand_id("m", 5),
        "amount": round(random.uniform(1.0, 5000.0), 2),
        "event_ts": iso_ts(event_dt),
        "channel": random.choice(CHANNELS),
        "country": random.choice(COUNTRIES),
        "status": random.choice(STATUSES),
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--brokers", default="localhost:9094")
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--rate", type=float, default=2.0)
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--dup_rate", type=float, default=0.2)
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    producer = KafkaProducer(
        bootstrap_servers=args.brokers.split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=10,
        linger_ms=20,
    )

    seen: list[str] = []
    interval = 1.0 / max(args.rate, 0.0001)
    start = time.time()
    sent = 0

    try:
        while time.time() - start < args.seconds:
            use_dup = (seen and random.random() < args.dup_rate)
            if use_dup:
                oid = random.choice(seen)
                event = make_event(order_id=oid)
            else:
                event = make_event()
                seen.append(event["order_id"])
                if len(seen) > 2000:
                    seen = seen[-2000:]

            meta = producer.send(args.topic, value=event).get(timeout=10)
            sent += 1
            print(f"✅ sent #{sent} p={meta.partition} off={meta.offset} -> {event}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n⛔ stopped by user")
    finally:
        producer.flush()
        producer.close()
        print(f"Done. total_sent={sent}")

if __name__ == "__main__":
    main()