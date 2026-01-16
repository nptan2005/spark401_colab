# spark/lab/lab_bronze_generate_orders.py
import os
import json
import time
import random
from datetime import datetime, timedelta

BRONZE_DIR = "data/bronze_lab33/orders_raw"
os.makedirs(BRONZE_DIR, exist_ok=True)

def gen_batch(batch_id: int, n: int, base_event_time: datetime, late_ratio: float = 0.15):
    rows = []
    for i in range(n):
        order_id = str(batch_id * 1000000 + i)
        customer_id = str(1 if random.random() < 0.25 else random.randint(2, 50000))  # skew nhẹ
        merchant_id = str(random.randint(1, 3000))
        amount = round(random.random() * 5000, 6)
        country = random.choice(["VN", "TH", "SG", "ID", "MY"])
        channel = random.choice(["POS", "ECOM", "ATM"])
        status = random.choice(["SUCCESS", "FAILED", "REVERSED"])

        # event_ts: phần lớn là ngày “hôm nay”, một phần là late data (lùi 1-3 ngày)
        if random.random() < late_ratio:
            event_ts = base_event_time - timedelta(days=random.randint(1, 3))
        else:
            event_ts = base_event_time

        ingest_ts = datetime.now()
        dt = event_ts.date().isoformat()

        rows.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "merchant_id": merchant_id,
            "amount": amount,
            "event_ts": event_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "channel": channel,
            "country": country,
            "status": status,
            "ingest_ts": ingest_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "dt": dt
        })
    return rows

def write_batch_json(batch_id: int, rows: list[dict]):
    path = os.path.join(BRONZE_DIR, f"batch_{batch_id:04d}.json")
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"✅ wrote {path} rows={len(rows)}")

if __name__ == "__main__":
    random.seed(7)
    base = datetime(2026, 1, 10, 10, 0, 0)

    # 6 micro-batches, mỗi batch 20k rows (tuỳ máy bạn tăng/giảm)
    for b in range(6):
        rows = gen_batch(batch_id=b, n=20000, base_event_time=base, late_ratio=0.20)
        write_batch_json(b, rows)
        time.sleep(1)  # tạo cảm giác “file đến theo thời gian”