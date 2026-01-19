import json, random, time
from datetime import datetime, timezone
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

BOOTSTRAP = "localhost:9094"
TOPIC = "orders_raw"

p = Producer({"bootstrap.servers": BOOTSTRAP})

def gen():
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "order_id": f"o{random.randint(1, 200)}",
        "customer_id": f"c{random.randint(1, 50)}",
        "merchant_id": f"m{random.randint(1, 20)}",
        "amount": round(random.uniform(10, 1500), 2),
        "event_ts": now,
        "channel": random.choice(["ECOM", "POS", "APP"]),
        "country": "VN",
        "status": random.choice(["SUCCESS", "FAILED"])
    }

def delivery(err, msg):
    if err:
        print("DELIVERY_ERR:", err)
    # else: print("OK:", msg.topic(), msg.partition(), msg.offset())

if __name__ == "__main__":
    while True:
        payload = gen()
        p.produce(TOPIC, value=json.dumps(payload).encode("utf-8"), on_delivery=delivery)
        p.poll(0)
        print(payload)
        time.sleep(0.2)