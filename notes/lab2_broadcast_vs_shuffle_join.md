# broadcast vs shuffle join

## Lab Name:

`Lab2f_join` chạy được ngay với dataset kiểu lab1 (`orders 2M, customers 50k`), và bạn sẽ nhìn thấy rõ trên `Spark UI/History`: `BroadcastExchange vs Exchange` (+ `SortMergeJoin/ShuffledHashJoin`).

---

## Ý tưởng nhanh (để bạn đọc UI là hiểu ngay)

**Broadcast Hash Join (BHJ)**
-	Spark copy bảng nhỏ (customers) sang mọi `executor/partition` của bảng lớn (`orders`)
-	Không shuffle bảng lớn → ***thường nhanh hơn**
-	Trên plan/UI bạn sẽ thấy: `BroadcastExchange + BroadcastHashJoin`

**Shuffle Join (SMJ / SHJ)**
-	**Spark** phải ***shuffle** cả hai phía theo `join key`
-	***Thường tốn thời gian** vì `Exchange + sort/merge`
-	**Trên plan/UI bạn sẽ thấy:** `Exchange` (`hashpartitioning` theo `key`) và thường `SortMergeJoin` (phổ biến nhất)

---

## 1) Lab2f_join: 3 case để so sánh

Tạo 3 file ở folder `spark/lab/` hoặc `notebook` jupyter rồi chạy từng cái, mở `Spark UI/History` xem `SQL` tab.

### A) Case 1 — Default (thường sẽ Broadcast customers)

`lab2f_join_broadcast_default.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (SparkSession.builder
  .appName("lab2f_join_broadcast_default")
  .config("spark.sql.shuffle.partitions", "50")
  .getOrCreate())

orders = spark.read.parquet("data/silver/orders_enriched").select(
    "order_id","customer_id","amount","order_ts","country","channel","status"
)

customers = spark.read.parquet("data/silver/customers").select(
    "customer_id","segment","risk_tier","created_date"
)

# Join + 1 action để Spark thật sự chạy
df = (orders.join(customers, on="customer_id", how="left")
      .groupBy("segment").count())

df.explain("formatted")
df.show(20, False)

spark.stop()
```

### ✅ Bạn kỳ vọng thấy trong plan:
-	BroadcastExchange ở phía customers
-	BroadcastHashJoin (LeftOuter)

---

### B) Case 2 — Tắt broadcast để ép Shuffle Join

`lab2f_join_force_shuffle.py`

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
  .appName("lab2f_join_force_shuffle")
  .config("spark.sql.shuffle.partitions", "50")
  .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # tắt broadcast
  .getOrCreate())

orders = spark.read.parquet("data/silver/orders_enriched").select(
    "order_id","customer_id","amount","order_ts","country","channel","status"
)

customers = spark.read.parquet("data/silver/customers").select(
    "customer_id","segment","risk_tier","created_date"
)

df = (orders.join(customers, on="customer_id", how="left")
      .groupBy("segment").count())

df.explain("formatted")
df.show(20, False)

spark.stop()
```

#### ✅ Bạn kỳ vọng thấy:
-	Exchange hashpartitioning(customer_id, 50) ở cả hai phía
-	Join node thường là SortMergeJoin (phổ biến) hoặc ShuffledHashJoin (tuỳ Spark quyết định)

---

### C) Case 3 — Ép broadcast bằng hint (để chắc chắn)

`lab2f_join_force_broadcast_hint.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = (SparkSession.builder
  .appName("lab2f_join_force_broadcast_hint")
  .config("spark.sql.shuffle.partitions", "50")
  .getOrCreate())

orders = spark.read.parquet("data/silver/orders_enriched").select(
    "order_id","customer_id","amount","order_ts","country","channel","status"
)

customers = spark.read.parquet("data/silver/customers").select(
    "customer_id","segment","risk_tier","created_date"
)

df = (orders.join(broadcast(customers), on="customer_id", how="left")
      .groupBy("segment").count())

df.explain("formatted")
df.show(20, False)

spark.stop()
```

#### ✅ Dù threshold thế nào, bạn vẫn sẽ thấy BroadcastExchange.

---

## 2) Bạn cần nhìn gì trên Spark UI/History để “đọc được join”

Vào tab `SQL / DataFrame` → chọn `query` → xem `Plan Visualization`.

#### Dấu hiệu Broadcast
-	**Node:** `BroadcastExchange`
-	**Join node:** `BroadcastHashJoin`
-	Thường ít `Exchange` hơn (vì tránh `shuffle` bảng lớn)

#### Dấu hiệu Shuffle Join
-	**Node:** `Exchange` (`hashpartitioning` theo `customer_id`)
-	**Join node:** `SortMergeJoin` hoặc `ShuffledHashJoin`
-	Thường có ***Sort** trước ***merge join**

---

## 3) Giải thích “vì sao Spark hay chọn SortMergeJoin khi shuffle?”
-	`SortMergeJoin` là **“an toàn, ổn định”** cho ***dataset lớn***, hoạt động tốt với `spills`, và **Spark optimizer** hay ưu tiên.
-	`ShuffledHashJoin` thường xuất hiện khi **Spark** thấy một phía ***đủ nhỏ/đủ điều kiện** để `hash build` mà vẫn `shuffle` (***tuỳ phiên bản/config***).

> Bạn không cần ép loại join cụ thể ở bước này—chỉ cần thấy khác nhau giữa BroadcastExchange vs Exchange là đạt.

---

## 4) Lưu ý quan trọng (để bạn khỏi bị “đơ máy”)
-	Broadcast có thể fail nếu bảng “nhỏ” nhưng vẫn quá lớn cho driver/executor memory → lỗi kiểu OOM/broadcast timeout.
-	Local máy bạn đang thấy warning memory khi write repartition → join cũng có thể “ăn RAM”.
-	Nếu cần, tăng driver memory khi chạy:

```code
PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell" python spark/lab/...
```

(hoặc config trong builder: `.config("spark.driver.memory","4g")`)

