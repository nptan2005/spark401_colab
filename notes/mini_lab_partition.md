
# 🔥 Partition column + Partition pruning + Shuffle impact

Không có skew, không join phức tạp, chỉ 1 concept nhưng nhìn rất rõ trong Spark UI.

---

# 🧪 MINI LAB – PARTITION COLUMN & PARTITION PRUNING

## 🎯 Mục tiêu lab này

Sau lab này, bạn phải tự tin trả lời được:

1. Partition column khác repartition ở điểm nào?
2. Vì sao partition giúp IO chứ không giúp shuffle?
3. Partition column dùng để làm gì (KHÔNG phải repartition)
4. Vì sao partition đúng → Spark scan ít file hơn
5. Vì sao partition sai → IO chết, dù logic đúng
6. Nhìn Spark UI biết partition pruning có xảy ra hay không

---

## 🧠 TƯ DUY TRƯỚC KHI CODE (RẤT QUAN TRỌNG)

## ❌ Sai lầm phổ biến
-	“partition để tăng parallelism”
-	“partition = repartition”

> 👉 SAI

## ✅ Bản chất đúng
-	Partition column = tối ưu IO
-	Repartition = tối ưu compute

---

## 📦 DATA DÙNG LẠI

Ta dùng lại:

```code
data/silver/orders_enriched
```

Schema:

```code
order_id
customer_id
amount
order_ts
channel
country
segment
risk_tier
created_date
```

---

## 🔬 STEP 1 – GHI DATA KHÔNG PARTITION (baseline)

### Code (lab2a_no_partition.py)

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("lab2_no_partition")
    .getOrCreate()
)

df = spark.read.parquet("data/silver/orders_enriched")

(
    df
    .write
    .mode("overwrite")
    .parquet("data/silver_np/orders")
)

print("DONE: no partition")
spark.stop()
```

### 👉 Chạy:

```bash
python spark/lab/lab2a_no_partition.py
```

---

## 🔍 STEP 2 – QUERY DATA KHÔNG PARTITION

```python
df = spark.read.parquet("data/silver_np/orders")

df.filter("order_ts >= '2026-01-10' AND order_ts < '2026-01-11'") \
  .groupBy("country") \
  .count() \
  .explain("formatted")
```

### Kết qủa:

```code
== Physical Plan ==
AdaptiveSparkPlan (7)
+- HashAggregate (6)
   +- Exchange (5)
      +- HashAggregate (4)
         +- Project (3)
            +- Filter (2)
               +- Scan parquet  (1)


(1) Scan parquet 
Output [2]: [order_ts#3, country#5]
Batched: true
Location: InMemoryFileIndex [file:/Users/nptan2005/SourceCode/Python/spark401_colab/data/silver_np/orders]
PushedFilters: [IsNotNull(order_ts), GreaterThanOrEqual(order_ts,2026-01-10 00:00:00.0), LessThan(order_ts,2026-01-11 00:00:00.0)]
ReadSchema: struct<order_ts:timestamp,country:string>

(2) Filter
Input [2]: [order_ts#3, country#5]
Condition : ((isnotnull(order_ts#3) AND (order_ts#3 >= 2026-01-10 00:00:00)) AND (order_ts#3 < 2026-01-11 00:00:00))

(3) Project
Output [1]: [country#5]
Input [2]: [order_ts#3, country#5]

(4) HashAggregate
Input [1]: [country#5]
Keys [1]: [country#5]
Functions [1]: [partial_count(1)]
Aggregate Attributes [1]: [count#25L]
Results [2]: [country#5, count#26L]

(5) Exchange
Input [2]: [country#5, count#26L]
Arguments: hashpartitioning(country#5, 200), ENSURE_REQUIREMENTS, [plan_id=15]

(6) HashAggregate
Input [2]: [country#5, count#26L]
Keys [1]: [country#5]
Functions [1]: [count(1)]
Aggregate Attributes [1]: [count(1)#24L]
Results [2]: [country#5, count(1)#24L AS count#12L]

(7) AdaptiveSparkPlan
Output [2]: [country#5, count#12L]
Arguments: isFinalPlan=false
```

### 🔎 KHI XEM SPARK UI

Bạn sẽ thấy:

-	Scan parquet
-	Files read = tất cả
-	Không có dòng nào nói về PartitionFilters

👉 ❗ Spark phải đọc toàn bộ data

---

### 🧠 KẾT LUẬN TẠM

Filter đúng logic nhưng không giúp IO nhanh hơn

---

## 🧪 STEP 3 – GHI DATA CÓ PARTITION COLUMN (chuẩn)

### Chọn partition column đúng

Trong bank → date / dt / business_date

###  👉 Ta dùng:

```code
dt = to_date(order_ts)
```

### Code (lab2b_partitioned.py)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

spark = (
    SparkSession.builder
    .appName("lab2_partitioned")
    .getOrCreate()
)

df = spark.read.parquet("data/silver/orders_enriched")

df = df.withColumn("dt", to_date(col("order_ts")))

(
    df
    .write
    .mode("overwrite")
    .partitionBy("dt")
    .parquet("data/silver_p/orders")
)

print("DONE: partitioned by dt")
spark.stop()
```


### 👉 Chạy:

```bash
python spark/lab/lab2b_partitioned.py
```

---

### 📁 KIỂM TRA STRUCTURE (RẤT QUAN TRỌNG)

```bash
ls data/silver_p/orders | head
```

Bạn sẽ thấy:

```code
dt=2026-01-08/
dt=2026-01-09/
dt=2026-01-10/
dt=2026-01-11/
```

👉 Đây là physical partition, không phải repartition

---

### 🔍 STEP 4 – QUERY CÓ PARTITION PRUNING

```python
df = spark.read.parquet("data/silver_p/orders")

df.filter("dt = '2026-01-10'") \
  .groupBy("country") \
  .count() \
  .explain("formatted")
```

---

### 🔥 ĐỌC SPARK UI (CHỖ QUAN TRỌNG NHẤT)

Trong Scan parquet, bạn PHẢI thấy:

-	PartitionFilters:

```code
dt = 2026-01-10
```

-	Files read: chỉ của 1 ngày
-	Rows scanned: giảm rất mạnh

👉 Nếu thấy vậy → partition pruning WORKING

---

### 🧠 SO SÁNH TRỰC QUAN

|**Tiêu chí**|**Không partition**|**Partition by dt**|
|-----------|-------------------|-------------------|
|Files read|Tất cả|1 ngày|
|Scan time|Cao hơn|Thấp|
|Shuffle|Giống|Giống|
|IO cost|❌ Tốn|✅ Rẻ|
|Bank-grade|❌|✅|

---

## 🧠 ĐIỀU RẤT QUAN TRỌNG (BANK CONTEXT)

### ❌ Không partition theo:
-	customer_id
-	order_id
-	amount

> → Cardinality cao → folder explosion

### ✅ Nên partition theo:
-	business_date
-	dt
-	month (nếu data rất lớn)

---

## Câu hỏi liên quan bài lab

## 1️⃣ Partition column khác repartition ở điểm nào?

### Partition column (partitionBy)
-	Xảy ra lúc WRITE (lưu dữ liệu ra storage).
-	Tạo cấu trúc thư mục vật lý kiểu dt=2026-01-10/… → giúp Spark đọc ít file hơn khi filter theo partition column.
-	Mục tiêu: tối ưu IO / scan (đọc dữ liệu).

### Repartition (repartition, coalesce)
-	Xảy ra lúc COMPUTE (trong DAG).
-	Thay đổi số partition của RDD/DataFrame để điều phối task song song / cân bằng tải.
-	Thường tạo shuffle (repartition thường shuffle; coalesce thường không).
-	Mục tiêu: tối ưu compute / parallelism / file output count.

> Nói ngắn: partitionBy = layout trên đĩa. repartition = layout trong RAM/cluster để chạy.

---

### 2️⃣ Vì sao partition giúp IO chứ không giúp shuffle?

### Vì partition pruning chỉ ảnh hưởng tới bước Scan parquet:
-	Khi filter theo dt, Spark chỉ mở những folder dt phù hợp → giảm file đọc → giảm IO.

### Còn shuffle xảy ra do các phép:
-	groupBy, join, distinct, orderBy, window…

Những phép này cần repartition dữ liệu theo key để aggregate/join đúng → shuffle là “bắt buộc logic”, không liên quan dữ liệu nằm ở folder nào.

> Partition giúp “đọc ít”, nhưng không giúp “trộn dữ liệu” trong groupBy/join.

---

### 3️⃣ Filter bằng to_date(order_ts) thay vì dt thì pruning có xảy ra không?

### Thường là KHÔNG (hoặc không tối ưu).
-	Partition pruning hoạt động tốt nhất khi filter trực tiếp trên cột partition: dt = '2026-01-10'.
-	Nếu bạn viết: to_date(order_ts) = '2026-01-10' thì:
-	Spark phải tính to_date(order_ts) cho từng row → filter trở thành “row-level predicate”.
-	Nó không map trực tiếp về folder dt=… để skip từ đầu → dễ mất pruning.

> Best practice: luôn filter trên dt (cột partition) nếu có.

---

# ✅ Tiếp luôn: MINI-LAB “CHỨNG MINH PRUNING” (rất rõ trên Spark UI)

## Mục tiêu: 

Chạy 2 query giống nhau, 1 cái pruning ON, 1 cái pruning OFF.

---

## A) Chuẩn bị: đọc dataset partitioned

Bạn đã có `data/silver_p/orders (partitionBy dt)`. OK.

## B) Query 1 — Pruning ON (đúng chuẩn)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab2c_pruning_on").getOrCreate()
df = spark.read.parquet("data/silver_p/orders")

q1 = (
    df.filter("dt = '2026-01-10'")
      .groupBy("country")
      .count()
)

q1.explain("formatted")
q1.show(20, False)

spark.stop()
```


### Bạn nhìn Spark UI / SQL tab / Details:
- Scan parquet
- PartitionFilters: [isnotnull(dt), (dt = 2026-01-10)] (hoặc tương tự)
- Files read giảm mạnh (chỉ 1 folder)

---

## C) Query 2 — Pruning OFF (cố tình viết sai)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

spark = SparkSession.builder.appName("lab2c_pruning_off").getOrCreate()
df = spark.read.parquet("data/silver_p/orders")

q2 = (
    df.filter(to_date(col("order_ts")) == "2026-01-10")
      .groupBy("country")
      .count()
)

q2.explain("formatted")
q2.show(20, False)

spark.stop()
```

### Bạn nhìn Spark UI:
-	Thường không thấy PartitionFilters (hoặc không prune đúng)
-	Files read có thể tăng (đọc nhiều folder hơn)

> Đây chính là “bằng chứng sống” cho câu #3.

---

# ✅ Bonus: phân biệt PartitionBy vs Repartition trong output file count

## D) Test: nếu bạn muốn “mỗi ngày chỉ 1 file” (production hay cần)

PartitionBy tạo folder theo dt, nhưng mỗi dt có thể nhiều file vì số partitions lúc write.

**Bạn làm:**

```python
df = spark.read.parquet("data/silver/orders_enriched") \
          .withColumn("dt", to_date(col("order_ts")))

# 1) partitionBy dt nhưng sẽ ra N files mỗi dt (do partitions)
df.write.mode("overwrite").partitionBy("dt").parquet("data/tmp_p1")

# 2) ép mỗi dt ~ ít file hơn: repartition theo dt trước khi write
(df.repartition("dt")
   .write.mode("overwrite")
   .partitionBy("dt")
   .parquet("data/tmp_p2"))
```

#### Giải thích:
-	partitionBy(dt) tạo folder (IO optimization & layout).
-	repartition(dt) quyết định bao nhiêu task ghi mỗi dt (compute/output file control).

---

# Đánh giá lab 2: partition và repartition

---

## 1️⃣ Partition column khác repartition ở điểm nào?

#### Partition column (khi write) = layout dữ liệu trên disk
-	Ví dụ: .write.partitionBy("dt")...
-	Spark sẽ tạo folder theo dt: .../dt=2026-01-10/part-...parquet
-	Lợi ích chính: partition pruning → query có filter dt=... sẽ bỏ qua cả folder không liên quan → giảm IO.

#
#### repartition (khi transform) = chia lại partition trong Spark execution
-	Ví dụ: .repartition(200, "dt") hoặc .repartition("dt")
-	Đây là shuffle (đa phần) để phân phối lại rows giữa executors/tasks.
-	Mục tiêu: tăng/giảm parallelism, giảm skew, chuẩn bị cho join/groupBy, hoặc giảm small files trước khi write (nếu dùng đúng cách).

##### 👉 Nói ngắn gọn:
	•	partitionBy = tổ chức file/folder trên storage (read nhanh hơn khi filter đúng cột).
	•	repartition = tổ chức lại dữ liệu “trong RAM/compute” (tác động shuffle/parallel).

---

## 2️⃣ Vì sao partition giúp IO chứ không giúp shuffle?

#### Vì partitionBy tạo folder, còn shuffle xảy ra do yêu cầu “gom/đối chiếu key” trong compute.
-	Khi bạn filter dt = '2026-01-10' (đúng partition col) → Spark chỉ đọc 1 partition folder → IO giảm mạnh (plan của bạn có PartitionFilters và History UI thấy “number of partitions read: 1”).
-	Nhưng khi bạn groupBy(country) hay groupBy(dt, segment, ...) → Spark cần đưa cùng key về cùng reducer → phải Exchange (shuffle) để đảm bảo tính đúng.

> ✅ Partition có thể gián tiếp giúp shuffle trong một số tình huống rất cụ thể (vd: bạn groupBy chỉ theo dt và dữ liệu đã được chia/đọc theo từng dt rất “gọn”), nhưng Spark không coi “folder partition” là distribution guarantee để bỏ shuffle một cách chắc chắn. Vì vậy bạn vẫn thấy Exchange trong lab2.

---

## 3️⃣ Nếu filter bằng to_date(order_ts) thay vì dt thì pruning có xảy ra không?

**Không (hoặc gần như không)** — và đúng y như bạn đã đo được:
-	Pruning ON (lab2c_pruning_on.py): plan có
 `PartitionFilters: ... (dt = 2026-01-10)`

> → Spark bỏ qua partitions khác.

-	Pruning OFF (`lab2c_pruning_off.py`): plan không có PartitionFilters, chỉ có
`PushedFilters: order_ts >= ... AND order_ts < ...`

> → Spark phải scan mọi partition folder, rồi mới lọc bằng predicate pushdown/row group stats.

**👉 Lý do:** partition pruning hoạt động khi predicate tham chiếu trực tiếp partition column (dt). Còn to_date(order_ts) là expression trên cột data → Spark không map ngược được để “chỉ chọn folder dt=…”.

> **✅ Best practice:** materialize dt (cột date) và partitionBy(dt), rồi filter bằng dt.

---

# Tiếp: 1 ví dụ “kết hợp partition + repartition theo column” (để bạn tự tin trước lab 3)

## Mục tiêu ví dụ:
1.	Dữ liệu partitionBy(dt) để pruning (IO).
2.	Dữ liệu repartition theo dt để giảm shuffle writers + giảm small files, và quan sát trên UI.

## A) Tạo silver partitioned theo dt (đúng bài)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = (SparkSession.builder
  .appName("lab2d_partitioned_write")
  .config("spark.sql.shuffle.partitions", "50")   # giảm mặc định 200 cho máy local
  .getOrCreate())

df = (spark.read.parquet("data/silver/orders_enriched")
      .withColumn("dt", to_date(col("order_ts"))))

# (1) repartition theo dt trước khi write:
# - mục tiêu: mỗi dt gom về 1 số partition ổn định -> write ít file hơn, đỡ MemoryManager warn
df = df.repartition("dt")

(df.write
   .mode("overwrite")
   .partitionBy("dt")
   .parquet("data/silver_p/orders"))
```

**Bạn sẽ nhìn trong Spark UI/History:**

-	Write sẽ có shuffle (vì repartition), nhưng số file trong mỗi dt=.../ thường “đẹp” hơn.
-	Quan trọng: về sau query filter dt=... sẽ prune.

⸻

## B) Compare 2 query để thấy rõ pruning vs không pruning (y như bạn đang làm)

### Query 1 (prune tốt):

```python
spark.read.parquet("data/silver_p/orders") \
  .where(col("dt") == "2026-01-10") \
  .groupBy("country").count().explain("formatted")
```

### Query 2 (không prune):

```python
from pyspark.sql.functions import to_date

spark.read.parquet("data/silver_p/orders") \
  .where(to_date(col("order_ts")) == "2026-01-10") \
  .groupBy("country").count().explain("formatted")
```

**Trên History UI bạn kỳ vọng thấy:**
-	Query 1: “files read” ít hơn rất nhiều + “partitions read: 1”
-	Query 2: “files read” nhiều hơn (scan rộng), dù output giống nhau

---

### partition + xử lý partition theo column (repartition)

### repartition(dt) giúp “shape” file output (số file / kích thước) và parallelism khi write


# ✅ Lab2e: So sánh số file khi write (không repartition vs repartition)

---

## 1) Write partitioned nhưng không repartition

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = (SparkSession.builder
  .appName("lab2e_write_no_repartition")
  .config("spark.sql.shuffle.partitions", "50")
  .getOrCreate())

df = (spark.read.parquet("../data/silver/orders_enriched")
      .withColumn("dt", to_date(col("order_ts"))))

(df.write.mode("overwrite")
   .partitionBy("dt")
   .parquet("../data/silver_p_lab2e/no_repart"))

spark.stop()
```

## 2) Write partitioned và repartition theo dt

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = (SparkSession.builder
  .appName("lab2e_write_repartition_dt")
  .config("spark.sql.shuffle.partitions", "50")
  .getOrCreate())

df = (spark.read.parquet("../data/silver/orders_enriched")
      .withColumn("dt", to_date(col("order_ts")))
      .repartition("dt"))   # key điểm

(df.write.mode("overwrite")
   .partitionBy("dt")
   .parquet("../data/silver_p_lab2e/repart_dt"))

spark.stop()
```

## 3) Check bằng lệnh shell (so file count)

```bash
find data/silver_p_lab2e/no_repart -name "*.parquet" | wc -l
find data/silver_p_lab2e/repart_dt -name "*.parquet" | wc -l
```

# xem riêng 1 ngày

```bash
find data/silver_p_lab2e/no_repart/dt=2026-01-10 -name "*.parquet" | wc -l
find data/silver_p_lab2e/repart_dt/dt=2026-01-10 -name "*.parquet" | wc -l
```

**Bạn sẽ thấy thường:**

-	no_repart: nhiều file hơn / partition ngày có nhiều part hơn
-	repart_dt: ít file hơn (ổn định hơn theo dt)

> **Lưu ý:** ***repartition("dt")*** vẫn shuffle (tốn compute lúc write), nhưng đổi lại file layout “đẹp”, query sau này đọc nhanh hơn và ít overhead.

---

## C) Tại sao trong Query 1 bạn vẫn thấy Exchange dù đã prune?

Vì bạn groupBy theo country chứ không phải dt.

Nếu bạn thử:

```python
spark.read.parquet("../data/silver_p_lab2d/orders") \
  .where(col("dt") == "2026-01-10") \
  .groupBy("dt").count().explain("formatted")
```

Bạn sẽ thấy shuffle có thể “nhẹ” hoặc plan khác (tùy Spark quyết định), nhưng nguyên tắc: aggregate theo key thường vẫn cần Exchange.

---
