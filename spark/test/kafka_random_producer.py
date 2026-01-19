import json
import random
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

BOOTSTRAP = "localhost:9094"
TOPIC = "orders"

channels = ["ECOM", "POS", "ATM"]
countries = ["VN", "SG", "TH", "MY"]
statuses = ["SUCCESS", "FAILED", "REVERSED"]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def rand_order():
    now = datetime.now(timezone.utc)
    # event_ts có thể “trễ” 0-120 phút để giống thực tế
    event_ts = now - timedelta(minutes=random.randint(0, 120))

    oid = f"o{random.randint(1, 9_999_999):07d}"
    cid = f"c{random.randint(1, 99_999):05d}"
    mid = f"m{random.randint(1, 99_999):05d}"

    return {
        "order_id": oid,
        "customer_id": cid,
        "merchant_id": mid,
        "amount": round(random.uniform(1.0, 5000.0), 2),
        "event_ts": event_ts.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "channel": random.choice(channels),
        "country": random.choice(countries),
        "status": random.choice(statuses),
    }

if __name__ == "__main__":
    print(f"Producing to {TOPIC} via {BOOTSTRAP} ... Ctrl+C to stop")
    try:
        while True:
            msg = rand_order()
            producer.send(TOPIC, msg)
            producer.flush()
            print("sent:", msg)
            time.sleep(0.2)  # ~5 msg/s
    except KeyboardInterrupt:
        pass
    finally:
        producer.close()