import json
import random
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

BOOTSTRAP = "localhost:9094"
TOPIC = "orders"

channels = ["ECOM", "POS", "ATM"]
countries = ["VN", "TH", "SG", "MY"]
statuses = ["SUCCESS", "FAILED", "REVERSED"]

def iso_ts(dt: datetime) -> str:
    # Kafka->Spark from_json TimestampType thường parse tốt dạng ISO
    return dt.replace(tzinfo=None).isoformat(timespec="seconds")

def gen_order(i: int) -> dict:
    now = datetime.now(timezone.utc) + timedelta(hours=7)  # VN time
    return {
        "order_id": f"o{random.randint(1_000_000, 9_999_999)}",
        "customer_id": f"c{random.randint(1, 99_999)}",
        "merchant_id": f"m{random.randint(1, 99_999)}",
        "amount": round(random.uniform(1, 5000), 2),
        "event_ts": iso_ts(now - timedelta(seconds=random.randint(0, 3600))),
        "channel": random.choice(channels),
        "country": random.choice(countries),
        "status": random.choice(statuses),
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=50,
    )
    i = 0
    print(f"Producing to {TOPIC} @ {BOOTSTRAP} ... Ctrl+C to stop")
    try:
        while True:
            msg = gen_order(i)
            producer.send(TOPIC, msg)
            if i % 50 == 0:
                producer.flush()
            print(msg)
            i += 1
            time.sleep(0.2)  # adjust rate
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()