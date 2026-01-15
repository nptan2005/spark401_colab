# LAB 3.2 – Fact–Dim Join ở quy mô lớn  
## Chiến lược tối ưu THỰC TẾ (Bank / Payment / Analytics)

> Mục tiêu lab này:
> - Làm **fact–dim join quy mô lớn**
> - Có **skew, partition, pruning, join strategy**
> - Đọc được **Spark UI / Explain**
> - Gần với **bài toán ngân hàng – thanh toán – KPI**

---

## 0️⃣ Bối cảnh thực tế (Real-world context)

Trong ngân hàng / payment / fintech, ta thường có:

### FACT TABLE (lớn – hàng triệu đến tỷ)
- `orders / transactions / payments`
- Append-only
- Partition theo `dt`

### DIM TABLE (nhỏ hơn nhưng vẫn lớn)
- `customers`
- `merchants`
- `accounts`

👉 Bài toán phổ biến:

> **Tính KPI giao dịch theo ngày, quốc gia, phân khúc khách hàng**

---

## 1️⃣ Thiết kế dữ liệu cho LAB 3.2

### 1.1 Fact: `orders_fact_dt`
- ~ **10 triệu rows**
- Partition theo `dt`
- Có skew customer_id = `"1"` (VIP / Merchant lớn)

| column | ý nghĩa |
|------|-------|
| order_id | id giao dịch |
| customer_id | khách |
| amount | số tiền |
| country | VN / TH / SG |
| dt | ngày giao dịch |

---

### 1.2 Dim: `customers_dim`
- ~ **200k rows**
- Không partition
- Join key: `customer_id`

| column | ý nghĩa |
|------|-------|
| customer_id | PK |
| segment | MASS / AFFLUENT / SME |
| risk_tier | LOW / MED / HIGH |

---

## 2️⃣ Chuẩn bị data (đã chạy xong phía bạn)

Bạn **đã làm đúng** phần này 👍  
→ Chúng ta **tập trung JOIN & tối ưu**

---

## 3️⃣ Query baseline (chưa tối ưu)

### 3.1 Code

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count

spark = (
    SparkSession.builder
    .appName("lab3_2_baseline")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)

orders = spark.read.parquet("data/silver_lab32/orders_fact_dt")
customers = spark.read.parquet("data/silver_lab32/customers_dim")

res = (
    orders
    .where(col("dt") == "2026-01-10")
    .join(customers, "customer_id", "left")
    .groupBy("dt", "country", "segment", "risk_tier")
    .agg(
        count("*").alias("txns"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount")
    )
)

res.explain("formatted")
res.show(5)

spark.stop()

```

### 3.2 Kết quả

```code
== Physical Plan ==
AdaptiveSparkPlan (10)
+- HashAggregate (9)
   +- Exchange (8)
      +- HashAggregate (7)
         +- Project (6)
            +- BroadcastHashJoin LeftOuter BuildRight (5)
               :- Scan parquet  (1)
               +- BroadcastExchange (4)
                  +- Filter (3)
                     +- Scan parquet  (2)


(1) Scan parquet 
Output [4]: [customer_id#1, amount#2, country#3, dt#7]
Batched: true
Location: InMemoryFileIndex [file:/Users/nptan2005/SourceCode/Python/spark401_colab/data/silver_lab32/orders_fact_dt]
PartitionFilters: [isnotnull(dt#7), (dt#7 = 2026-01-10)]
ReadSchema: struct<customer_id:string,amount:double,country:string>

(2) Scan parquet 
Output [3]: [customer_id#8, segment#9, risk_tier#10]
Batched: true
Location: InMemoryFileIndex [file:/Users/nptan2005/SourceCode/Python/spark401_colab/data/silver_lab32/customers_dim]
PushedFilters: [IsNotNull(customer_id)]
ReadSchema: struct<customer_id:string,segment:string,risk_tier:string>

(3) Filter
Input [3]: [customer_id#8, segment#9, risk_tier#10]
Condition : isnotnull(customer_id#8)

(4) BroadcastExchange
Input [3]: [customer_id#8, segment#9, risk_tier#10]
Arguments: HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=31]

(5) BroadcastHashJoin
Left keys [1]: [customer_id#1]
Right keys [1]: [customer_id#8]
Join type: LeftOuter
Join condition: None

(6) Project
Output [5]: [amount#2, country#3, dt#7, segment#9, risk_tier#10]
Input [7]: [customer_id#1, amount#2, country#3, dt#7, customer_id#8, segment#9, risk_tier#10]

(7) HashAggregate
Input [5]: [amount#2, country#3, dt#7, segment#9, risk_tier#10]
Keys [4]: [dt#7, country#3, segment#9, risk_tier#10]
Functions [3]: [partial_count(1), partial_sum(amount#2), partial_avg(amount#2)]
Aggregate Attributes [4]: [count#33L, sum#34, sum#35, count#36L]
Results [8]: [dt#7, country#3, segment#9, risk_tier#10, count#37L, sum#38, sum#39, count#40L]

(8) Exchange
Input [8]: [dt#7, country#3, segment#9, risk_tier#10, count#37L, sum#38, sum#39, count#40L]
Arguments: hashpartitioning(dt#7, country#3, segment#9, risk_tier#10, 50), ENSURE_REQUIREMENTS, [plan_id=36]

(9) HashAggregate
Input [8]: [dt#7, country#3, segment#9, risk_tier#10, count#37L, sum#38, sum#39, count#40L]
Keys [4]: [dt#7, country#3, segment#9, risk_tier#10]
Functions [3]: [count(1), sum(amount#2), avg(amount#2)]
Aggregate Attributes [3]: [count(1)#30L, sum(amount#2)#31, avg(amount#2)#32]
Results [7]: [dt#7, country#3, segment#9, risk_tier#10, count(1)#30L AS txns#15L, sum(amount#2)#31 AS total_amount#16, avg(amount#2)#32 AS avg_amount#17]

(10) AdaptiveSparkPlan
Output [7]: [dt#7, country#3, segment#9, risk_tier#10, txns#15L, total_amount#16, avg_amount#17]
Arguments: isFinalPlan=false
```

### Giải thích:

Physical Plan này cực kỳ **ổn** và thường là trạng thái **lý tưởng nhất** cho các truy vấn Join giữa một bảng Fact lớn và một bảng Dimension nhỏ.

Đây chính là kịch bản "Happy Path" mà các Data Engineer luôn hướng tới. Hãy phân tích xem tại sao nó lại tốt:

#### 1. "Vũ khí hạng nặng": BroadcastHashJoin (5)

Thay vì phải dùng `SortMergeJoin` (tốn công Shuffle dữ liệu cả hai bảng qua mạng và Sort lại), Spark đã chọn **BroadcastHashJoin (BHJ)**.

* **Cơ chế:** Spark lấy bảng nhỏ (Customers - bên phải) gửi bản sao đến **tất cả** các máy Worker đang giữ các phần của bảng Orders.
* **Tại sao ổn?**:
* **Không Shuffle bảng to:** Bảng Orders (10 triệu dòng) đứng yên tại chỗ, không phải bay qua mạng. Điều này tiết kiệm cực nhiều băng thông và thời gian.
* **Phá giải hoàn toàn Skew:** Trong BHJ, Skew không còn là vấn đề đáng sợ nữa. Dù `customer_id=1` có 3 triệu dòng nằm ở 1 máy, máy đó chỉ việc lấy bảng Customers (đã nằm sẵn trong RAM) ra để "so khớp" (lookup) cực nhanh. Không có bước Shuffle dồn cục nên không có Task nào bị nghẽn (Straggler).



---

#### 2. Phân tích các bước then chốt

* **(4) BroadcastExchange**: Đây là bước gửi bảng Customers đi "phủ sóng" toàn cụm máy chủ. Bạn sẽ thấy `BuildRight`, nghĩa là bảng bên phải (Customers) được chọn để broadcast.
* **(1) PartitionFilters [dt = 2026-01-10]**: Vẫn giữ được kỹ thuật **Partition Pruning**. Spark chỉ đọc đúng file cần thiết, giúp tốc độ cực nhanh.
* **(7) & (9) HashAggregate**: Quá trình tính toán KPI (`txns`, `total_amount`) diễn ra mượt mà ngay sau khi Join xong.

---

#### 3. Tại sao Plan này lại xuất hiện?

Trước đó bạn thấy `SortMergeJoin` vì chúng ta đã chủ động tắt Broadcast bằng cấu hình:
`spark.sql.autoBroadcastJoinThreshold = -1`.

Trong Plan này, có vẻ bạn đã bật lại nó (mặc định là 10MB) hoặc không cấu hình tắt nó nữa. Spark thấy bảng Customers đủ nhỏ để nhét vừa RAM của các máy Worker nên nó tự động chọn BHJ.

---

#### 4. Một lưu ý nhỏ: `isFinalPlan=false` (10)

Dù Plan này rất tốt, nhưng ký hiệu `isFinalPlan=false` ở bước số (10) cho thấy đây vẫn là **AdaptiveSparkPlan**.

* Spark đang nói: *"Tôi dự định dùng BroadcastHashJoin, nhưng tôi sẽ vừa chạy vừa quan sát. Nếu sau khi đọc bảng Orders xong mà thấy nó quá nhỏ, hoặc bảng Customers thực tế lại to hơn tôi tưởng (vượt ngưỡng 10MB), tôi có thể sẽ đổi ý ở giây cuối cùng."*

#### 💡 So sánh nhanh cho Tân:

| Đặc điểm | SortMergeJoin (Lab trước) | BroadcastHashJoin (Lab này) |
| --- | --- | --- |
| **Tốc độ** | Chậm (do Shuffle & Sort) | **Cực nhanh** |
| **Skew** | Bị nghẽn tại Key bị lệch | **Bất chấp Skew** |
| **Băng thông mạng** | Tốn nhiều (gửi cả 2 bảng) | Tốn ít (chỉ gửi bảng nhỏ) |
| **Rủi ro** | Ít rủi ro OOM | Dễ **OOM** nếu bảng nhỏ không thực sự nhỏ |

**Kết luận:** Nếu bảng Dimension (Customers) của bạn dưới vài trăm MB, hãy luôn ưu tiên để Spark chạy **BroadcastHashJoin** như thế này. Đây là trạng thái tối ưu nhất!

---

## 4️⃣ Phân tích Explain – Baseline

### 4.1 Những điểm quan trọng trong plan

#### Bạn sẽ thấy:

```code
Scan parquet (orders)
PartitionFilters: dt = '2026-01-10'   ✅ PRUNING OK

SortMergeJoin ❌
Exchange (shuffle) ở cả 2 phía ❌❌
```

### 4.2 Ý nghĩa

|**Hiện tượng**|**Nhận xét**|
|--------------|------------|
|Partition pruning	|✅ Tốt|
|SortMergeJoin|	❌ Đắt|
|Shuffle trước join	|❌ Rất tốn IO|
|Shuffle sau agg|	❌ Không tránh được|

> 👉 Baseline chạy được nhưng chưa tối ưu

---

## 5️⃣ Tối ưu #1 – Broadcast Dim (THỰC TẾ DÙNG NHIỀU NHẤT)


### 5.1 Khi nào dùng?
-	Dim < ~100MB
-	Ít thay đổi
-	Join nhiều lần

### 5.2 Code

```python
from pyspark.sql.functions import broadcast

res = (
    orders
    .where(col("dt") == "2026-01-10")
    .join(broadcast(customers), "customer_id", "left")
    .groupBy("dt", "country", "segment", "risk_tier")
    .agg(
        count("*").alias("txns"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount")
    )
)

res.explain("formatted")
```

---

### 5.3 Explain – Broadcast Join

#### Bạn sẽ thấy:

```code
BroadcastHashJoin ✅
NO shuffle phía dim ✅
Shuffle chỉ còn ở aggregation
```

#### 5.4 Kết luận

|**Tiêu chí**|**Baseline**|**Broadcast**|
|------------|------------|-------------|
|Join|SortMergeJoin|BroadcastHashJoin|
|Shuffle|2 phía|1 phía|
|Runtime|Cao|Thấp hơn rõ|

> 👉 90% job analytics production dùng cách này

⸻

## 6️⃣ Vấn đề THỰC TẾ: Skew (customer_id = ‘1’)

### 6.1 Dấu hiệu trên Spark UI
-	1 task chạy lâu bất thường
-	Input records cực lớn ở 1 partition

> 👉 Bạn đã đúng khi phát hiện skew ở LAB trước

---

## 7️⃣ Tối ưu #2 – Salting (chỉ dùng khi BẮT BUỘC)

### ⚠️ Salting KHÔNG phải default
#### → chỉ dùng khi:
-	Không broadcast được
-	Skew cực nặng
-	Join bắt buộc shuffle

### 7.1 Ý tưởng

```code
customer_id = 1
→ chia thành (1, salt=0..N)
```

---

### 7.2 Code salting (đã FIX đúng cho bạn)

```python
from pyspark.sql.functions import rand, floor, explode, sequence

SALT_N = 16

o = orders.withColumn(
    "salt",
    floor(rand(7) * SALT_N).cast("int")
)

c = (
    customers
    .withColumn("salt", explode(sequence(0, SALT_N - 1)))
)

j = (
    o.join(
        c,
        (o.customer_id == c.customer_id) & (o.salt == c.salt),
        "left"
    )
)
```

---

### 7.3 Explain salting

#### Bạn sẽ thấy:

**SortMergeJoin**
**BUT:**
- partition key = (customer_id, salt)
- skew được dàn đều

### 7.4 Trade-off

|**Ưu**|**Nhược**|
|------|---------|
|Giảm skew|Tăng data dim|
|Ổn định runtime|	Code phức tạp|
|Dùng khi cần|Không default|


---

## 8️⃣ Thứ tự ưu tiên TỐI ƯU (RẤT QUAN TRỌNG)

#### 1️⃣ Partition pruning (dt)
#### 2️⃣ Broadcast dim
#### 3️⃣ AQE (Adaptive Query Execution)
#### 4️⃣ Repartition hợp lý
#### 5️⃣ Salting (cuối cùng)

#### ❌ KHÔNG: salting trước khi thử broadcast
#### ❌ KHÔNG: repartition mù quáng

---

## 9️⃣ Sơ đồ tư duy (ASCII – dễ nhớ)

```code
FACT (orders, 10M, partition dt)
   |
   |-- filter dt --> giảm IO
   |
   |-- join customers
         |
         |-- broadcast --> FAST (default)
         |
         |-- shuffle --> check skew
                 |
                 |-- skew nặng --> SALT
```

---

## 🔟 Kết luận LAB 3.2

-	✅ Hiểu fact–dim join quy mô lớn
-	✅ Đọc được Spark Explain
-	✅ Phân biệt:
-	Broadcast vs Shuffle
-	Khi nào dùng salting
-	✅ Tư duy production-grade

