# Giải thích code:

----

## Dùng spark tạo data mẫu

### Code:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, rand, expr, element_at, pmod, date_sub, to_date, broadcast, when
)

# =========================
# Config
# =========================
BASE_TS = "2026-01-12 10:23:17"
CUSTOMERS_N = 50_000
ORDERS_N = 2_000_000

OUT_CUSTOMERS = "data/silver/customers"
OUT_ORDERS = "data/silver/orders"
OUT_ORDERS_ENRICHED = "data/silver/orders_enriched"
OUT_ORDERS_PARTITIONED = "data/silver_p_lab2f/orders_enriched_by_dt"  # partitionBy dt

def build_spark():
    return (
        SparkSession.builder
        .appName("lab2f_prepare_join_data_v2")
        .config("spark.sql.shuffle.partitions", "50")  # local tune
        .config("spark.sql.adaptive.enabled", "true")  # AQE on
        .getOrCreate()
    )

def gen_customers(spark):
    customers = (
        spark.range(0, CUSTOMERS_N)
        .select(
            (col("id") + 1).cast("string").alias("customer_id"),
            element_at(expr("array('MASS','AFFLUENT','SME')"),
                       (pmod(col("id"), 3) + 1).cast("int")).alias("segment"),
            element_at(expr("array('LOW','MED','HIGH')"),
                       (pmod(col("id"), 3) + 1).cast("int")).alias("risk_tier"),
            date_sub(expr("date('2026-01-11')"),
                     pmod(col("id"), 365).cast("int")).alias("created_date"),
        )
    )
    return customers

def gen_orders(spark):
    orders = (
        spark.range(0, ORDERS_N)
        .select(
            (col("id") + 1).cast("string").alias("order_id"),
            expr("""
                CASE
                  WHEN rand(7) < 0.25 THEN '1'
                  ELSE cast(pmod(id * 17, 49999) + 2 as string)
                END
            """).alias("customer_id"),
            (rand(11) * 5000).alias("amount"),

            # ✅ FIX interval dynamic: Spark style (giống plan bạn đã thấy)
            expr(f"timestamp('{BASE_TS}') + -(INTERVAL 1 DAY * cast(pmod(id, 30) as int))").alias("order_ts"),

            element_at(expr("array('POS','ECOM','ATM')"),
                       (pmod(col("id"), 3) + 1).cast("int")).alias("channel"),
            element_at(expr("array('VN','SG','TH','ID','MY')"),
                       (pmod(col("id"), 5) + 1).cast("int")).alias("country"),
            element_at(expr("array('SUCCESS','FAILED','REVERSED')"),
                       (pmod(col("id"), 3) + 1).cast("int")).alias("status"),
        )
    )
    return orders

def enrich_orders(orders, customers, use_broadcast=False):
    c = broadcast(customers) if use_broadcast else customers

    enriched = (
        orders.join(c, on="customer_id", how="left")
        .withColumn(
            "amount_bucket",
            when(col("amount") < 1000, "LOW")
            .when(col("amount") < 3000, "MID")
            .otherwise("HIGH")
        )
        .withColumn("dt", to_date(col("order_ts")))
    )
    return enriched

def main():
    spark = build_spark()

    # ---- Generate
    customers = gen_customers(spark)
    orders = gen_orders(spark)

    # ---- Persist raw data (cho join lab / compare)
    customers.write.mode("overwrite").parquet(OUT_CUSTOMERS)
    orders.write.mode("overwrite").parquet(OUT_ORDERS)

    # ---- Enrich (join + dt) – default (không ép broadcast)
    orders_enriched = enrich_orders(orders, customers, use_broadcast=False)
    orders_enriched.write.mode("overwrite").parquet(OUT_ORDERS_ENRICHED)

    # ---- Partitioned dataset by dt (cho pruning labs)
    # Gợi ý: nếu muốn “ít file mỗi dt”, hãy repartition theo dt trước khi write:
    orders_enriched_repart_dt = orders_enriched.repartition(30, "dt")  # tune: 30~100 tùy máy
    (orders_enriched_repart_dt
        .write
        .mode("overwrite")
        .partitionBy("dt")
        .parquet(OUT_ORDERS_PARTITIONED)
    )

    # ---- Quick sanity
    print("✔ customers:", spark.read.parquet(OUT_CUSTOMERS).count())
    print("✔ orders:", spark.read.parquet(OUT_ORDERS).count())
    print("✔ orders_enriched:", spark.read.parquet(OUT_ORDERS_ENRICHED).count())
    print("✔ orders_enriched_by_dt:", OUT_ORDERS_PARTITIONED)

    spark.stop()

if __name__ == "__main__":
    main()
```

---

### Giải thích:

Đây là một bộ code Spark chuyên nghiệp dùng để giả lập dữ liệu (Data Synthesis) quy mô lớn. Thay vì dùng Python loop (rất chậm), code này sử dụng Spark Native Operations (phép toán trên DataFrame) để tận dụng sức mạnh tính toán song song.

Dưới đây là giải thích chi tiết từng phần cho bạn:

1. **Cấu hình và SparkSession (build_spark)**
```Python
.config("spark.sql.shuffle.partitions", "50")
.config("spark.sql.adaptive.enabled", "true")
```

**shuffle.partitions:** Khi bạn thực hiện Join hoặc GroupBy, Spark chia dữ liệu thành 50 phần (mặc định là 200). Với máy cá nhân (Local), 50 là con số hợp lý để không bị quá tải bộ nhớ.

**AQE (Adaptive Query Execution):** Đây là tính năng thông minh của Spark 3.x, nó tự động tối ưu kế hoạch thực thi (Query Plan) dựa trên kích thước dữ liệu thực tế lúc chạy.

2. **Kỹ thuật tạo Data mẫu (Data Synthesis)**

Đây là phần thú vị nhất trong code của bạn. Thay vì đọc từ file, bạn dùng spark.range để tự sinh dữ liệu.

**Hàm `gen_customers` (Tạo danh mục khách hàng)**

`spark.range(0, CUSTOMERS_N)`: Tạo một cột duy nhất là id chạy từ 0 đến 49,999.

`pmod(col("id"), 3)`: Phép chia lấy dư (idmod3). Kết quả trả về 0, 1, 2 tuần hoàn.

`element_at(expr("array('MASS','...'), ...)`: Đây là cách Spark "pick" giá trị từ một mảng. Dựa vào kết quả của pmod, nó sẽ gán nhãn 'MASS', 'AFFLUENT' hoặc 'SME' cho khách hàng một cách đều đặn.

`date_sub(...)`: Tạo ngày đăng ký ảo bằng cách lấy ngày hiện tại trừ đi số ngày tương ứng với id.

**Hàm gen_orders (Tạo dữ liệu giao dịch)**

Hàm này mô phỏng hành vi mua hàng thực tế:

`rand(7)`: Tạo số ngẫu nhiên từ 0 đến 1.

`WHEN rand(7) < 0.25 THEN '1'`: Giả lập khoảng 25% đơn hàng thuộc về khách hàng có ID là "1" (tạo ra dữ liệu bị lệch - Data Skew để test hiệu năng).

`INTERVAL 1 DAY * cast(pmod(id, 30) as int)`: Đây là cú pháp SQL Spark. Nó lấy mốc thời gian gốc rồi trừ đi 0 đến 29 ngày. Điều này đảm bảo dữ liệu trải đều trong vòng 1 tháng.

3. **Thao tác Join và Làm giàu dữ liệu (enrich_orders)**

Đây là logic chính để tạo ra bảng Fact hoàn chỉnh:

**broadcast(customers)**: Nếu tham số `use_broadcast=True`, Spark sẽ copy toàn bộ bảng customers (nhỏ) sang tất cả các máy thợ (Executors). Điều này cực kỳ nhanh vì tránh được việc xáo trộn (Shuffle) dữ liệu lớn.

**when(...).otherwise(...)**: Giống như câu lệnh `IF-ELSE`. Nếu `amount < 1000` thì là `'LOW'`, vv.

**to_date(col("order_ts")):** Trích xuất ngày từ `timestamp` để sau này dùng làm cột phân vùng `(Partition)`.

4. **Ghi dữ liệu và Tối ưu phân vùng (main)**

Đoạn code này xử lý cách lưu trữ dữ liệu xuống ổ cứng:

```Python
orders_enriched_repart_dt = orders_enriched.repartition(30, "dt")
orders_enriched_repart_dt.write.mode("overwrite").partitionBy("dt").parquet(...)
```

**repartition(30, "dt"):** Rất quan trọng! Nó gom tất cả dữ liệu có cùng ngày dt về cùng một phân vùng trước khi ghi.

**Tại sao cần?** Nếu không có lệnh này, mỗi máy thợ sẽ ghi một file nhỏ cho mỗi ngày, dẫn đến tình trạng ***"too many small files"*** làm chậm hệ thống.

**partitionBy("dt"):** Spark sẽ tạo ra các thư mục con kiểu dt=2026-01-01/. Khi bạn truy vấn dữ liệu của 1 ngày cụ thể, Spark chỉ đọc thư mục đó và bỏ qua (**Pruning**) toàn bộ các thư mục khác, giúp tốc độ tăng gấp hàng chục lần.

5. **Tóm tắt các hàm Spark SQL bạn cần nhớ:**

|**Hàm**|**Công dụng**|
|-------|------------|
|spark.range()|Tạo DataFrame với cột ID tự tăng (gốc rễ để tạo data ảo).|
|expr()|Cho phép viết cú pháp SQL thuần túy bên trong Python.|
|pmod()|	Chia lấy dư (rất hữu ích để tạo dữ liệu lặp lại/phân loại).|
|element_at()|Lấy phần tử trong mảng theo index (index của Spark SQL bắt đầu từ 1).|
|broadcast()|Ép Spark dùng Broadcast Join để tối ưu tốc độ.|

---

## Join vói `spark.sql.adaptive.enabled`

---

### Code:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (SparkSession.builder
  .appName("lab2f_join_default")
  .config("spark.sql.shuffle.partitions", "50")
  .config("spark.sql.adaptive.enabled", "true")
  .getOrCreate())

orders = spark.read.parquet("data/silver/orders")
customers = spark.read.parquet("data/silver/customers")

# Join + 1 aggregation nhẹ để plan rõ hơn
q = (orders
     .join(customers, "customer_id", "left")
     .groupBy("segment")
     .count())

q.explain("formatted")
q.show(10, False)

spark.stop()
```

### Kết qủa:

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
Output [1]: [customer_id#1]
Batched: true
Location: InMemoryFileIndex [file:/Users/nptan2005/SourceCode/Python/spark401_colab/data/silver/orders]
ReadSchema: struct<customer_id:string>

(2) Scan parquet 
Output [2]: [customer_id#7, segment#8]
Batched: true
Location: InMemoryFileIndex [file:/Users/nptan2005/SourceCode/Python/spark401_colab/data/silver/customers]
PushedFilters: [IsNotNull(customer_id)]
ReadSchema: struct<customer_id:string,segment:string>

(3) Filter
Input [2]: [customer_id#7, segment#8]
Condition : isnotnull(customer_id#7)

(4) BroadcastExchange
Input [2]: [customer_id#7, segment#8]
Arguments: HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=31]

(5) BroadcastHashJoin
Left keys [1]: [customer_id#1]
Right keys [1]: [customer_id#7]
Join type: LeftOuter
Join condition: None

(6) Project
Output [1]: [segment#8]
Input [3]: [customer_id#1, customer_id#7, segment#8]

(7) HashAggregate
Input [1]: [segment#8]
Keys [1]: [segment#8]
Functions [1]: [partial_count(1)]
Aggregate Attributes [1]: [count#25L]
Results [2]: [segment#8, count#26L]

(8) Exchange
Input [2]: [segment#8, count#26L]
Arguments: hashpartitioning(segment#8, 50), ENSURE_REQUIREMENTS, [plan_id=36]

(9) HashAggregate
Input [2]: [segment#8, count#26L]
Keys [1]: [segment#8]
Functions [1]: [count(1)]
Aggregate Attributes [1]: [count(1)#24L]
Results [2]: [segment#8, count(1)#24L AS count#13L]

(10) AdaptiveSparkPlan
Output [2]: [segment#8, count#13L]
Arguments: isFinalPlan=false
```

### Giải thích:

Đoạn code của bạn thực hiện một tác vụ kinh điển trong xử lý dữ liệu: **Join hai bảng và Thống kê (Aggregation)**. Điều thú vị nhất ở đây không nằm ở code, mà nằm ở cách Spark tự động tối ưu hóa thông qua bảng **Physical Plan (Kế hoạch vật lý)** mà bạn đã cung cấp.

Dưới đây là phân tích chi tiết:

---

#### 1. Giải thích Code

* **Mục tiêu**: Lấy bảng `orders` (bảng lớn) kết hợp với bảng `customers` (bảng nhỏ) để biết mỗi khách hàng thuộc phân khúc (`segment`) nào, sau đó đếm xem mỗi phân khúc có bao nhiêu đơn hàng.
* **Cấu hình AQE**: Bạn bật `spark.sql.adaptive.enabled = true`. Đây là lý do tại sao trong Plan xuất hiện node `AdaptiveSparkPlan`. Nó cho phép Spark thay đổi chiến thuật thực thi ngay trong lúc đang chạy nếu thấy dữ liệu thực tế khác với dự đoán.

---

#### 2. Phân tích Physical Plan (Từ dưới lên trên)

Hãy nhìn vào các con số trong ngoặc đơn để hiểu luồng đi của dữ liệu:

##### Bước 1: Đọc dữ liệu (Scan) & Lọc (Filter)

* **(1) & (2) Scan parquet**: Spark đọc dữ liệu từ ổ cứng. Chú ý: Spark chỉ đọc các cột cần thiết (`customer_id`, `segment`) thay vì đọc hết cả bảng để tiết kiệm I/O.
* **(3) Filter**: Spark tự động thêm bước `isnotnull(customer_id)`. Đây là tối ưu hóa **Predicate Pushdown** – loại bỏ các dòng null ngay từ đầu để không phải Join vô ích.

##### Bước 2: Cơ chế Join (BroadcastHashJoin)

Đây là phần quan trọng nhất trong Plan của bạn:

* **(4) BroadcastExchange**: Spark nhận thấy bảng `customers` đủ nhỏ. Thay vì xáo trộn cả 2 bảng (Shuffle), nó gửi toàn bộ bảng `customers` đến tất cả các máy thợ (Executors).
* **(5) BroadcastHashJoin**:
* **BuildRight**: Bảng bên phải (`customers`) được dùng để xây dựng bảng băm (Hash Table) trong bộ nhớ.
* **LeftOuter**: Giữ lại toàn bộ đơn hàng, đơn nào không có khách hàng tương ứng thì `segment` sẽ là `null`.
* **Ưu điểm**: Không tốn chi phí Shuffle bảng `orders`, tốc độ cực nhanh.



##### Bước 3: Gom nhóm & Đếm (HashAggregate)

Spark thực hiện gom nhóm qua 2 giai đoạn để tối ưu:

* **(7) HashAggregate (Partial)**: Mỗi máy thợ tự đếm các phân khúc trên phần dữ liệu nó đang có (`partial_count`). Điều này làm giảm lượng dữ liệu cần truyền qua mạng.
* **(8) Exchange (Shuffle)**: Spark gom các kết quả trung gian có cùng `segment` về cùng một máy thợ. Bạn có thể thấy `hashpartitioning(segment#8, 50)` – con số 50 chính là cấu hình `shuffle.partitions` bạn đã đặt.
* **(9) HashAggregate (Final)**: Tổng hợp kết quả cuối cùng từ các máy thợ để cho ra con số tổng.

---

#### 3. Các điểm "sáng" trong kế hoạch này

1. **Sự thông minh của AQE**: Mặc dù bạn không dùng hàm `broadcast()`, nhưng Spark tự đo kích thước bảng `customers`. Nếu nó nhỏ hơn ngưỡng (mặc định 10MB), Spark tự động chọn **BroadcastHashJoin** thay vì **SortMergeJoin**.
2. **Toàn vẹn dữ liệu**: `LeftOuter` đảm bảo bạn không làm mất đơn hàng nào, ngay cả khi đơn hàng đó có `customer_id` không tồn tại trong bảng khách hàng.
3. **Tối ưu hóa bộ nhớ**: Ở bước **(6) Project**, Spark loại bỏ cột `customer_id` ngay sau khi Join xong, chỉ giữ lại cột `segment` cho bước Aggregate tiếp theo. Điều này giúp giảm tải cho bộ nhớ RAM.

---

#### 4. Gợi ý thêm

Trong kết quả `q.show()`, nếu bạn thấy một dòng có `segment` là `null`, đó chính là các đơn hàng có `customer_id` không khớp với bảng khách hàng.

---

## Join ép Broadcast (chắc chắn ra BroadcastHashJoin):

### Code:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = (SparkSession.builder
  .appName("lab2f_join_force_broadcast")
  .config("spark.sql.shuffle.partitions", "50")
  .config("spark.sql.adaptive.enabled", "true")
  .getOrCreate())

orders = spark.read.parquet("data/silver/orders")
customers = spark.read.parquet("data/silver/customers")

q = (orders
     .join(broadcast(customers), "customer_id", "left")
     .groupBy("segment")
     .count())

q.explain("formatted")
q.show(10, False)

spark.stop()
```

---

### Kết qủa:

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
Output [1]: [customer_id#1]
Batched: true
Location: InMemoryFileIndex [file:/Users/nptan2005/SourceCode/Python/spark401_colab/data/silver/orders]
ReadSchema: struct<customer_id:string>

(2) Scan parquet 
Output [2]: [customer_id#7, segment#8]
Batched: true
Location: InMemoryFileIndex [file:/Users/nptan2005/SourceCode/Python/spark401_colab/data/silver/customers]
PushedFilters: [IsNotNull(customer_id)]
ReadSchema: struct<customer_id:string,segment:string>

(3) Filter
Input [2]: [customer_id#7, segment#8]
Condition : isnotnull(customer_id#7)

(4) BroadcastExchange
Input [2]: [customer_id#7, segment#8]
Arguments: HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=31]

(5) BroadcastHashJoin
Left keys [1]: [customer_id#1]
Right keys [1]: [customer_id#7]
Join type: LeftOuter
Join condition: None

(6) Project
Output [1]: [segment#8]
Input [3]: [customer_id#1, customer_id#7, segment#8]

(7) HashAggregate
Input [1]: [segment#8]
Keys [1]: [segment#8]
Functions [1]: [partial_count(1)]
Aggregate Attributes [1]: [count#25L]
Results [2]: [segment#8, count#26L]

(8) Exchange
Input [2]: [segment#8, count#26L]
Arguments: hashpartitioning(segment#8, 50), ENSURE_REQUIREMENTS, [plan_id=36]

(9) HashAggregate
Input [2]: [segment#8, count#26L]
Keys [1]: [segment#8]
Functions [1]: [count(1)]
Aggregate Attributes [1]: [count(1)#24L]
Results [2]: [segment#8, count(1)#24L AS count#13L]

(10) AdaptiveSparkPlan
Output [2]: [segment#8, count#13L]
Arguments: isFinalPlan=false


+--------+-------+
|segment |count  |
+--------+-------+
|MASS    |1000032|
|SME     |500092 |
|AFFLUENT|499876 |
```
---

### Giải thích:

Trong đoạn code này, đã thực hiện một bước tiến quan trọng: Sử dụng hàm **`broadcast()`** để chủ động điều phối cách Spark thực hiện phép Join. Mặc dù kết quả vật lý (Physical Plan) trông có vẻ giống bài trước, nhưng về bản chất, bạn đang **"ép" (hint)** Spark phải đưa bảng `customers` vào bộ nhớ đệm của tất cả các excutor.

Dưới đây là phân tích chi tiết:

---

#### 1. Phân tích Code: `join(broadcast(customers), ...)`

#### 2.Tại sao dùng Broadcast?

Trong Spark, khi Join hai bảng lớn, hệ thống phải thực hiện **Shuffle** (xáo trộn dữ liệu qua mạng), đây là tác vụ cực kỳ tốn kém. Bằng cách bao bọc `customers` trong hàm `broadcast()`:

1. Spark sẽ copy toàn bộ bảng `customers` small size.
2. Gửi bản sao đó tới **mọi Executor** đang chạy.
3. Bảng `orders` (rất lớn) sẽ đứng yên tại chỗ, mỗi excutor sẽ thực lấy dữ liệu đơn hàng mình đang giữ để Join với bảng khách hàng trong bộ nhớ RAM.

---

#### 2. Giải mã Physical Plan (Từng bước thực thi)

Spark thực hiện theo quy trình 10 bước trong một "Kế hoạch thích ứng" (**AdaptiveSparkPlan**):

##### Giai đoạn chuẩn bị dữ liệu (1, 2, 3)

* **(1) & (2) Scan parquet**: Spark đọc file từ đường dẫn cục bộ. Bạn có thể thấy `ReadSchema` chỉ lấy đúng cột `customer_id` từ bảng `orders` và `segment` từ bảng `customers`. Đây là kỹ thuật **Column Pruning** (không đọc cột thừa).
* **(3) Filter**: Spark tự thêm điều kiện `isnotnull`. Nếu Customer không có ID, họ sẽ bị loại ngay từ đầu để giảm bớt dữ liệu cần Broadcast.

### Giai đoạn Join (4, 5, 6)

* **(4) BroadcastExchange**: Đây là lệnh thực thi việc "Broadcast" bảng Customer qua mạng. Dữ liệu được chuyển thành `HashedRelation` (một dạng bảng băm) để tra cứu cực nhanh.
* **(5) BroadcastHashJoin**: Phép Join diễn ra.
* `BuildRight`: Bảng bên phải (customers) là bảng được chọn để tạo Hash Table.
* `LeftOuter`: Spark đảm bảo mọi đơn hàng ở bảng trái (Scan 1) đều được giữ lại.


* **(6) Project**: Spark loại bỏ cột `customer_id` vì sau bước này ta chỉ cần cột `segment` để thống kê.

##### Giai đoạn Thống kê (7, 8, 9)

Đây là mô hình **MapReduce** thu nhỏ:

* **(7) HashAggregate (Partial)**: Mỗi excutor tự đếm số lượng segment trên dữ liệu của chính nó (Ví dụ: Excutor A đếm được 100 MASS, Excutor B đếm được 150 MASS).
* **(8) Exchange**: Gom tất cả các bản ghi có chung tên `segment` về cùng một chỗ (Sử dụng 50 partitions như cấu hình).
* **(9) HashAggregate (Final)**: Cộng dồn các con số `partial_count` lại để ra kết quả cuối cùng.

---

#### 3. Phân tích kết quả Output

```text
+--------+-------+
|segment |count  |
+--------+-------+
|MASS    |1000032|
|SME     |500092 |
|AFFLUENT|499876 |
+--------+-------+

```

Dựa vào con số này, ta có thể rút ra nhận xét về dữ liệu bạn đã tạo:

1. **Dữ liệu bị lệch (Data Skew)**: Phân khúc `MASS` chiếm khoảng 50% tổng số đơn hàng (). Trong khi `SME` và `AFFLUENT` mỗi bên chiếm khoảng 25%.
2. **Nguyên nhân**: Trong hàm `gen_orders` ở bài trước, đã code:
`WHEN rand(7) < 0.25 THEN '1' ELSE ...`.
ID khách hàng số 1 luôn thuộc phân khúc đầu tiên (`MASS`), dẫn đến phân khúc này "phình to" hơn hẳn.

---

#### 4. Tổng kết bài học

* **Khi nào dùng code này?** Khi bảng `customers` nhỏ (thường < 100MB hoặc vài GB tùy cấu hình máy) và bảng `orders` rất lớn.
* **Ưu điểm**: Tránh được giai đoạn Shuffle nặng nề nhất, giúp xử lý 2 triệu dòng chỉ trong tích tắc.
* **Rủi ro**: Nếu bảng `customers` quá lớn mà bạn ép `broadcast`, excutor sẽ bị lỗi `OutOfMemory (OOM)`.

---

## Join ép Shuffle (tắt broadcast để thấy SortMergeJoin)

---

### Code:

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

---

### Kết qủa:

Trong bài lab này, thực hiện một thay đổi cực kỳ quan trọng về mặt kiến trúc:  **vô hiệu hóa cơ chế Broadcast** bằng cấu hình `.config("spark.sql.autoBroadcastJoinThreshold", "-1")`.

Điều này buộc Spark phải chuyển sang một cơ chế join nặng đô hơn nhưng mạnh mẽ hơn cho dữ liệu lớn: **SortMergeJoin (SMJ)**.

---

#### 1. Phân tích Code & Sự thay đổi chiến thuật

* **`autoBroadcastJoinThreshold = -1`**: ra lệnh cho Spark: "Bất kể bảng khách hàng có nhỏ thế nào, đừng đưa nó vào bộ nhớ RAM của tất cả Excutor. Hãy dùng cách xáo trộn (Shuffle) truyền thống."
* **Mục đích**: Đây là bài lab để hiểu cách Spark xử lý khi **cả hai bảng đều cực kỳ lớn** (Big Data thực thụ), không thể chứa vừa trong bộ nhớ của một máy đơn lẻ.

---

#### 2. Giải mã Physical Plan (Sự khác biệt nằm ở đây)

Khác với lần trước (chỉ có 1 nhánh Shuffle cho Aggregation), lần này bạn có tới **3 giai đoạn Shuffle (Exchange)**:

##### Giai đoạn 1: Shuffle & Sort (Nhánh 1, 2, 3 và 4, 5, 6, 7)

Đây là giai đoạn chuẩn bị cho SMJ. Spark không thể Join ngay vì dữ liệu đang nằm rải rác.

* **(2) & (6) Exchange (Shuffle)**: Spark thực hiện "xáo trộn" cả hai bảng `orders` và `customers` dựa trên `customer_id`.
* Những dòng có cùng `customer_id` sẽ được đưa về cùng một excutor (Partition).
* Số **50** (số lượng partition đã cấu hình) xuất hiện ở đây.


* **(3) & (7) Sort**: Sau khi gom về một chỗ, Spark tiến hành sắp xếp dữ liệu theo thứ tự tăng dần của `customer_id`.

##### Giai đoạn 2: Phép Join thực thụ (8)

* **(8) SortMergeJoin**: Khi dữ liệu ở cả hai bên đã được sắp xếp, Spark chỉ cần "trượt" qua hai danh sách để tìm các cặp khớp nhau.
* Cơ chế này giống như việc cầm hai danh bạ điện thoại đã xếp theo vần A-Z, bạn chỉ cần so sánh từng dòng một cách tuần tự.
* **Ưu điểm**: Cực kỳ ổn định, không lo lỗi tràn bộ nhớ (OOM) vì không cần nạp toàn bộ bảng vào RAM.



##### Giai đoạn 3: Gom nhóm & Thống kê (10, 11, 12)

Sau khi Join xong, quy trình đếm `segment` diễn ra tương tự như các bài trước:

* **(10) Partial Count**: Đếm sơ bộ tại mỗi excutor.
* **(11) Exchange (Shuffle lần 3)**: Gom các `segment` giống nhau về cùng một excutor để chốt con số cuối cùng.

---

#### 3. So sánh BroadcastHashJoin vs. SortMergeJoin

| Tiêu chí | BroadcastHashJoin (Bài trước) | SortMergeJoin (Bài này) |
| --- | --- | --- |
| **Số lần Shuffle** | 1 lần (cho Aggregation) | **3 lần** (2 cho Join + 1 cho Aggregation) |
| **Tốc độ** | Rất nhanh (vì ít truyền dữ liệu qua mạng) | Chậm hơn (do tốn chi phí Shuffle và Sort) |
| **Độ an toàn** | Dễ lỗi OOM nếu bảng nhỏ bỗng dưng to lên | **Rất an toàn**, xử lý được dữ liệu hàng Terabyte |
| **Sử dụng bộ nhớ** | Tốn RAM để chứa bảng băm | Tốn Disk (ổ cứng) để tạm lưu dữ liệu khi Shuffle |

---

#### 4. Phân tích kết quả Output

Kết quả không thay đổi:

```text
|MASS    |1000032|
|SME     |500092 |
|AFFLUENT|499876 |

```

Điều này chứng minh: **Dù thuật toán Join thay đổi, kết quả logic cuối cùng vẫn phải nhất quán.** Tuy nhiên, nếu bạn kiểm tra trên Spark UI, bạn sẽ thấy thời gian thực thi (Execution Time) của bài này sẽ lâu hơn bài trước đáng kể do phải thực hiện Shuffle và ghi dữ liệu tạm xuống ổ cứng (Shuffle Write/Read).

---

# kết hợp partition + join

## Mục tiêu:

Dưới đây là mini-lab 2g đúng mục tiêu: **partition pruning (dt) + broadcast join customers + KPI**. Chạy xong bạn xem explain("formatted") sẽ thấy:

-	PartitionFilters: ... dt = ... (pruning)
-	BroadcastHashJoin ... BuildRight + BroadcastExchange (broadcast customers)
-	Exchange hashpartitioning(...) (shuffle do groupBy)

---

### Code 1: Broadcast + Pruning

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count as _count, broadcast

ORDERS_P_PATH = "data/silver_p_lab2d/orders"   # <-- đổi nếu bạn muốn dùng data/silver_p/orders
CUSTOMERS_PATH = "data/silver/customers"

DT = "2026-01-10"

spark = (
    SparkSession.builder
    .appName("lab2g_join_with_pruning")
    .config("spark.sql.shuffle.partitions", "50")
    # optional: cho rõ ràng, default của bạn là 10MB rồi
    .config("spark.sql.autoBroadcastJoinThreshold", "10MB")
    .getOrCreate()
)

# --- Read
orders = spark.read.parquet(ORDERS_P_PATH)
customers = spark.read.parquet(CUSTOMERS_PATH).select("customer_id", "segment", "risk_tier")

# --- Filter dt để chắc chắn pruning
orders_d = orders.where(col("dt") == DT)

# --- Broadcast join (customers nhỏ)
joined = (
    orders_d.join(broadcast(customers), on="customer_id", how="left")
)

# --- KPI
kpi = (
    joined.groupBy("dt", "country", "segment", "risk_tier")
    .agg(
        _count("*").alias("txns"),
        _sum("amount").alias("total_amount"),
        _avg("amount").alias("avg_amount"),
    )
)

print("\n=== EXPLAIN (formatted) ===")
kpi.explain("formatted")

print("\n=== SAMPLE OUTPUT ===")
kpi.orderBy(col("txns").desc()).show(20, truncate=False)

spark.stop()
```

### Kết qủa:

```code
26/01/13 14:15:19 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable

=== EXPLAIN (formatted) ===
== Physical Plan ==
AdaptiveSparkPlan (11)
+- HashAggregate (10)
   +- Exchange (9)
      +- HashAggregate (8)
         +- Project (7)
            +- BroadcastHashJoin LeftOuter BuildRight (6)
               :- Scan parquet  (1)
               +- BroadcastExchange (5)
                  +- Project (4)
                     +- Filter (3)
                        +- Scan parquet  (2)


(1) Scan parquet 
Output [4]: [customer_id#0, amount#2, country#5, dt#11]
Batched: true
Location: InMemoryFileIndex [file:/Users/nptan2005/SourceCode/Python/spark401_colab/data/silver_p_lab2d/orders]
PartitionFilters: [isnotnull(dt#11), (dt#11 = 2026-01-10)]
ReadSchema: struct<customer_id:string,amount:double,country:string>

(2) Scan parquet 
Output [3]: [customer_id#12, segment#13, risk_tier#14]
Batched: true
Location: InMemoryFileIndex [file:/Users/nptan2005/SourceCode/Python/spark401_colab/data/silver/customers]
PushedFilters: [IsNotNull(customer_id)]
ReadSchema: struct<customer_id:string,segment:string,risk_tier:string>

(3) Filter
Input [3]: [customer_id#12, segment#13, risk_tier#14]
Condition : isnotnull(customer_id#12)

(4) Project
Output [3]: [customer_id#12 AS c_customer_id#16, segment#13 AS c_segment#17, risk_tier#14 AS c_risk_tier#18]
Input [3]: [customer_id#12, segment#13, risk_tier#14]

(5) BroadcastExchange
Input [3]: [c_customer_id#16, c_segment#17, c_risk_tier#18]
Arguments: HashedRelationBroadcastMode(List(input[0, string, true]),false), [plan_id=27]

(6) BroadcastHashJoin
Left keys [1]: [customer_id#0]
Right keys [1]: [c_customer_id#16]
Join type: LeftOuter
Join condition: None

(7) Project
Output [5]: [dt#11, country#5, c_segment#17 AS segment#23, c_risk_tier#18 AS risk_tier#24, amount#2]
Input [7]: [customer_id#0, amount#2, country#5, dt#11, c_customer_id#16, c_segment#17, c_risk_tier#18]

(8) HashAggregate
Input [5]: [dt#11, country#5, segment#23, risk_tier#24, amount#2]
Keys [4]: [dt#11, country#5, segment#23, risk_tier#24]
Functions [3]: [partial_count(1), partial_sum(amount#2), partial_avg(amount#2)]
Aggregate Attributes [4]: [count#37L, sum#38, sum#39, count#40L]
Results [8]: [dt#11, country#5, segment#23, risk_tier#24, count#41L, sum#42, sum#43, count#44L]

(9) Exchange
Input [8]: [dt#11, country#5, segment#23, risk_tier#24, count#41L, sum#42, sum#43, count#44L]
Arguments: hashpartitioning(dt#11, country#5, segment#23, risk_tier#24, 50), ENSURE_REQUIREMENTS, [plan_id=32]

(10) HashAggregate
Input [8]: [dt#11, country#5, segment#23, risk_tier#24, count#41L, sum#42, sum#43, count#44L]
Keys [4]: [dt#11, country#5, segment#23, risk_tier#24]
Functions [3]: [count(1), sum(amount#2), avg(amount#2)]
Aggregate Attributes [3]: [count(1)#34L, sum(amount#2)#35, avg(amount#2)#36]
Results [7]: [dt#11, country#5, segment#23, risk_tier#24, count(1)#34L AS txns#26L, sum(amount#2)#35 AS total_amount#27, avg(amount#2)#36 AS avg_amount#28]

(11) AdaptiveSparkPlan
Output [7]: [dt#11, country#5, segment#23, risk_tier#24, txns#26L, total_amount#27, avg_amount#28]
Arguments: isFinalPlan=false



=== SAMPLE OUTPUT ===
+----------+-------+--------+---------+-----+--------------------+-----------------+
|dt        |country|segment |risk_tier|txns |total_amount        |avg_amount       |
+----------+-------+--------+---------+-----+--------------------+-----------------+
|2026-01-10|TH     |MASS    |LOW      |33457|8.332187367829195E7 |2490.416764153748|
|2026-01-10|TH     |AFFLUENT|MED      |16614|4.1581479857752606E7|2502.79763198222 |
|2026-01-10|TH     |SME     |HIGH     |16596|4.1406404070576854E7|2494.962886874961|
+----------+-------+--------+---------+-----+--------------------+-----------------+
```

----

### Code 2: Tắt broadcast để thấy SortMergeJoin

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count as _count

ORDERS_P_PATH = "data/silver_p_lab2d/orders"
CUSTOMERS_PATH = "data/silver/customers"
DT = "2026-01-10"

spark = (
    SparkSession.builder
    .appName("lab2g_join_force_shuffle")
    .config("spark.sql.shuffle.partitions", "50")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # tắt broadcast
    .getOrCreate()
)

o = spark.read.parquet(ORDERS_P_PATH).alias("o")
c = spark.read.parquet(CUSTOMERS_PATH).select(
    col("customer_id").alias("c_customer_id"),
    col("segment").alias("c_segment"),
    col("risk_tier").alias("c_risk_tier"),
).alias("c")

o_d = o.where(col("o.dt") == DT)

j = o_d.join(
    c,
    col("o.customer_id") == col("c.c_customer_id"),
    "left"
)

j2 = j.select(
    col("o.dt").alias("dt"),
    col("o.country").alias("country"),
    col("c.c_segment").alias("segment"),
    col("c.c_risk_tier").alias("risk_tier"),
    col("o.amount").alias("amount"),
)

kpi = (
    j2.groupBy("dt", "country", "segment", "risk_tier")
      .agg(
          _count("*").alias("txns"),
          _sum("amount").alias("total_amount"),
          _avg("amount").alias("avg_amount"),
      )
)

kpi.explain("formatted")
spark.stop()
```

---

### Kết qủa:

```code
== Physical Plan ==
AdaptiveSparkPlan (14)
+- HashAggregate (13)
   +- Exchange (12)
      +- HashAggregate (11)
         +- Project (10)
            +- SortMergeJoin LeftOuter (9)
               :- Sort (3)
               :  +- Exchange (2)
               :     +- Scan parquet  (1)
               +- Sort (8)
                  +- Exchange (7)
                     +- Project (6)
                        +- Filter (5)
                           +- Scan parquet  (4)


(1) Scan parquet 
Output [4]: [customer_id#0, amount#2, country#5, dt#11]
Batched: true
Location: InMemoryFileIndex [file:/Users/nptan2005/SourceCode/Python/spark401_colab/data/silver_p_lab2d/orders]
PartitionFilters: [isnotnull(dt#11), (dt#11 = 2026-01-10)]
ReadSchema: struct<customer_id:string,amount:double,country:string>

(2) Exchange
Input [4]: [customer_id#0, amount#2, country#5, dt#11]
Arguments: hashpartitioning(customer_id#0, 50), ENSURE_REQUIREMENTS, [plan_id=28]

(3) Sort
Input [4]: [customer_id#0, amount#2, country#5, dt#11]
Arguments: [customer_id#0 ASC NULLS FIRST], false, 0

(4) Scan parquet 
Output [3]: [customer_id#12, segment#13, risk_tier#14]
Batched: true
Location: InMemoryFileIndex [file:/Users/nptan2005/SourceCode/Python/spark401_colab/data/silver/customers]
PushedFilters: [IsNotNull(customer_id)]
ReadSchema: struct<customer_id:string,segment:string,risk_tier:string>

(5) Filter
Input [3]: [customer_id#12, segment#13, risk_tier#14]
Condition : isnotnull(customer_id#12)

(6) Project
Output [3]: [customer_id#12 AS c_customer_id#16, segment#13 AS c_segment#17, risk_tier#14 AS c_risk_tier#18]
Input [3]: [customer_id#12, segment#13, risk_tier#14]

(7) Exchange
Input [3]: [c_customer_id#16, c_segment#17, c_risk_tier#18]
Arguments: hashpartitioning(c_customer_id#16, 50), ENSURE_REQUIREMENTS, [plan_id=29]

(8) Sort
Input [3]: [c_customer_id#16, c_segment#17, c_risk_tier#18]
Arguments: [c_customer_id#16 ASC NULLS FIRST], false, 0

(9) SortMergeJoin
Left keys [1]: [customer_id#0]
Right keys [1]: [c_customer_id#16]
Join type: LeftOuter
Join condition: None

(10) Project
Output [5]: [dt#11, country#5, c_segment#17 AS segment#23, c_risk_tier#18 AS risk_tier#24, amount#2]
Input [7]: [customer_id#0, amount#2, country#5, dt#11, c_customer_id#16, c_segment#17, c_risk_tier#18]

(11) HashAggregate
Input [5]: [dt#11, country#5, segment#23, risk_tier#24, amount#2]
Keys [4]: [dt#11, country#5, segment#23, risk_tier#24]
Functions [3]: [partial_count(1), partial_sum(amount#2), partial_avg(amount#2)]
Aggregate Attributes [4]: [count#37L, sum#38, sum#39, count#40L]
Results [8]: [dt#11, country#5, segment#23, risk_tier#24, count#41L, sum#42, sum#43, count#44L]

(12) Exchange
Input [8]: [dt#11, country#5, segment#23, risk_tier#24, count#41L, sum#42, sum#43, count#44L]
Arguments: hashpartitioning(dt#11, country#5, segment#23, risk_tier#24, 50), ENSURE_REQUIREMENTS, [plan_id=36]

(13) HashAggregate
Input [8]: [dt#11, country#5, segment#23, risk_tier#24, count#41L, sum#42, sum#43, count#44L]
Keys [4]: [dt#11, country#5, segment#23, risk_tier#24]
Functions [3]: [count(1), sum(amount#2), avg(amount#2)]
Aggregate Attributes [3]: [count(1)#34L, sum(amount#2)#35, avg(amount#2)#36]
Results [7]: [dt#11, country#5, segment#23, risk_tier#24, count(1)#34L AS txns#26L, sum(amount#2)#35 AS total_amount#27, avg(amount#2)#36 AS avg_amount#28]

(14) AdaptiveSparkPlan
Output [7]: [dt#11, country#5, segment#23, risk_tier#24, txns#26L, total_amount#27, avg_amount#28]
Arguments: isFinalPlan=false
```

----

### Giải thích:

Đây là một ví dụ nâng cao minh họa sự kết hợp giữa **Partition Pruning** (Lọc phân vùng) và hai chiến lược Join phổ biến nhất trong Spark. Việc bạn sử dụng tập dữ liệu đã được phân vùng bởi cột `dt` là một bước nhảy vọt về tối ưu hiệu năng.

---

## 1. Phân tích Chi tiết Code (Cả 2 Script)

Cả hai script đều có chung các bước xử lý logic, chỉ khác nhau ở cấu hình thực thi:

* **`o.where(col("o.dt") == DT)`**: Đây là dòng code "đắt giá" nhất. Vì dữ liệu `orders` được lưu dưới dạng `partitionBy("dt")`, Spark sẽ không quét toàn bộ dữ liệu mà chỉ truy cập thẳng vào thư mục `dt=2026-01-10`.
* **Alias & Rename**: Việc dùng `.alias("o")` và đặt tên lại cột ở bảng khách hàng (`c_customer_id`) giúp tránh lỗi **Ambiguous column** (trùng tên cột) khi Join, một lỗi rất phổ biến trong thực tế.
* **`.agg(...)`**: Bạn đang tính toán các KPI tài chính thực tế:
* `txns`: Tổng số lượng giao dịch.
* `total_amount`: Doanh thu tổng.
* `avg_amount`: Giá trị trung bình mỗi đơn hàng.



---

## 2. Phân tích Physical Plan (Explain)

### Script 1: BroadcastHashJoin (Sử dụng RAM)

* **(1) PartitionFilters**: Bạn sẽ thấy dòng `[isnotnull(dt#11), (dt#11 = 2026-01-10)]`. Đây là bằng chứng cho thấy **Partition Pruning** đã hoạt động. Spark chỉ đọc đúng file của ngày mười tháng một.
* **(5) BroadcastExchange**: Spark copy bảng `customers` (đã filter) và gửi đến tất cả excutor.
* **(6) BroadcastHashJoin**: Việc Join diễn ra ngay lập tức trên RAM của từng excutor mà không cần xáo trộn bảng `orders`.

### Script 2: SortMergeJoin (Sử dụng Disk/Shuffle)

* **(2) & (7) Exchange**: Vì bạn tắt Broadcast (`-1`), Spark buộc phải băm (hash) cả 2 bảng dựa trên `customer_id` và gửi qua mạng.
* **(3) & (8) Sort**: Trước khi Join, Spark phải sắp xếp lại dữ liệu theo `customer_id`.
* **(9) SortMergeJoin**: Thực hiện Join dựa trên 2 danh sách đã sắp xếp. Quy trình này tốn nhiều tài nguyên CPU và I/O hơn hẳn.

---

## 3. So sánh 2 Script và Kết quả

| Đặc điểm | Script 1 (Broadcast) | Script 2 (SortMerge) |
| --- | --- | --- |
| **Chiến lược Join** | `BroadcastHashJoin` | `SortMergeJoin` |
| **Số lần Shuffle** | 1 lần (để Aggregate) | **3 lần** (2 cho Join + 1 cho Aggregate) |
| **Hiệu năng** | **Nhanh nhất**. Tận dụng tối đa bộ nhớ. | **Chậm hơn**. Phải ghi dữ liệu tạm xuống đĩa. |
| **Partition Pruning** | Có (Scan 1 - lọc được ngày) | Có (Scan 1 - lọc được ngày) |
| **Khả năng mở rộng** | Giới hạn bởi RAM (bảng khách hàng không được quá lớn). | Vô hạn (xử lý được mọi kích cỡ dữ liệu). |

### Phân tích kết quả mẫu (Sample Output)

```text
|2026-01-10|TH |MASS |LOW |33457|8.332...|2490.4...|

```

* **Số lượng giao dịch (txns)**: 33,457 đơn hàng cho phân khúc MASS tại Thái Lan trong ngày 10/01.
* **Sự nhất quán**: Dù bạn chạy Script 1 hay 2, con số kết quả **phải giống hệt nhau**. Sự khác biệt chỉ nằm ở "cách Spark vận hành bên dưới" và "thời gian hoàn thành".

---

## 4. Rút ra kết luận (Best Practices)

1. **Partition Pruning là ưu tiên số 1**: Luôn luôn lọc dữ liệu theo cột phân vùng (như `dt`) càng sớm càng tốt. Nó giúp giảm 90-99% lượng dữ liệu cần đọc, quan trọng hơn bất kỳ kỹ thuật Join nào.
2. **Dùng Broadcast khi có thể**: Nếu một trong hai bảng nhỏ (danh mục khách hàng, danh mục sản phẩm, tỷ giá...), hãy dùng `broadcast()`. Nó biến một bài toán Shuffle nặng nề thành một bài toán tính toán tại chỗ.
3. **Lọc trước khi Join**: Trong Explain (3), Spark thực hiện `isnotnull` và `Filter` trước khi Join. Bạn nên chủ động lọc các bản ghi không hợp lệ hoặc không cần thiết trước khi thực hiện các phép Join tốn kém.
4. **Kiểm soát số lượng File**: Trong bài lab này, bạn dùng `shuffle.partitions=50`. Trong môi trường thật với dữ liệu lớn hơn, con số này có thể cần tăng lên (200, 500) để tránh mỗi partition quá nặng.

---

Chào bạn! Một khi cả hai bảng đều cực kỳ lớn (ví dụ `orders` và `customers` đều hàng tỷ dòng), ngay cả **SortMergeJoin** cũng trở nên chậm chạp vì bước **Shuffle** (chia lại dữ liệu qua mạng) và **Sort** (sắp xếp) chiếm tới 80% thời gian chạy.

Kỹ thuật **Bucketing** sinh ra để giải quyết vấn đề này: Chúng ta chủ động "chia túi" và sắp xếp dữ liệu ngay từ lúc lưu trữ. Khi Join, Spark chỉ cần bê các "túi" tương ứng khớp với nhau mà **không cần Shuffle hay Sort lại**.

Dưới đây là Code thực hiện kỹ thuật này trên dữ liệu hiện có:

---

## 1. Code: Lưu dữ liệu dưới dạng Bucket (Pre-optimize)

Thay vì ghi file Parquet thông thường, chúng ta sẽ ghi vào các "Buckets" dựa trên `customer_id`.

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("lab2h_bucketing_prepare")
    .enableHiveSupport() # Cần thiết để quản lý Metadata của Buckets
    .getOrCreate())

# 1. Đọc dữ liệu Silver hiện có
orders = spark.read.parquet("data/silver/orders")
customers = spark.read.parquet("data/silver/customers")

# 2. Ghi bảng Orders thành 16 buckets (chia túi theo customer_id)
# Lưu ý: Phải dùng saveAsTable để Spark lưu thông tin bucket vào metastore
(orders.write
    .mode("overwrite")
    .bucketBy(16, "customer_id")
    .sortBy("customer_id")
    .saveAsTable("bucketed_orders"))

# 3. Ghi bảng Customers thành 16 buckets (bắt buộc số lượng bucket phải bằng hoặc là bội số của nhau)
(customers.write
    .mode("overwrite")
    .bucketBy(16, "customer_id")
    .sortBy("customer_id")
    .saveAsTable("bucketed_customers"))

print("✔ Đã lưu xong 2 bảng dưới dạng Bucketed Tables")
spark.stop()

```

---

## 2. Code: Thực hiện Join không Shuffle

Bây giờ, khi bạn Join hai bảng này, Spark sẽ nhận ra dữ liệu đã được chia túi và sắp xếp sẵn.

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("lab2h_bucket_join")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") # Tắt broadcast để ép dùng Join lớn
    .enableHiveSupport()
    .getOrCreate())

# Đọc từ Table (không phải từ file trực tiếp) để lấy metadata về bucket
df_orders = spark.table("bucketed_orders")
df_customers = spark.table("bucketed_customers")

# Thực hiện Join
result = df_orders.join(df_customers, "customer_id", "left")

print("=== EXPLAIN (BUCKET JOIN) ===")
result.explain("formatted")

result.show(5)
spark.stop()

```

---

## 3. Phân tích sự khác biệt trong Physical Plan

Khi bạn chạy `explain`, bạn sẽ thấy một sự thay đổi kinh ngạc so với các bài trước:

1. **Mất đi Node `Exchange**`: Bạn sẽ không còn thấy `Exchange (hashpartitioning...)`. Điều này có nghĩa là **không có byte dữ liệu nào bay qua mạng**. Các máy thợ chỉ việc đọc file cục bộ và Join.
2. **Mất đi Node `Sort**`: Vì chúng ta đã dùng `.sortBy("customer_id")` khi ghi, Spark bỏ qua luôn bước sắp xếp trong lúc Join.
3. **Tốc độ**: Với dữ liệu cực lớn, cách này nhanh hơn gấp 5-10 lần so với SortMergeJoin thông thường.

---

## 4. Khi nào nên dùng Bucketing?

Kỹ thuật này rất mạnh nhưng có "giá" của nó:

| Ưu điểm | Nhược điểm |
| --- | --- |
| **Join cực nhanh**: Loại bỏ hoàn toàn Shuffle và Sort. | **Tốn thời gian ghi ban đầu**: Bước ghi dữ liệu sẽ lâu hơn do phải chia túi và sắp xếp. |
| **Hỗ trợ Aggregation**: Nếu bạn GroupBy theo cột đã bucket, nó cũng sẽ nhanh hơn. | **Cứng nhắc**: Chỉ có tác dụng nếu bạn Join đúng cột đã bucket (`customer_id`). |
| **Phù hợp bảng lớn**: Dùng cho các bảng Fact/Dim khổng lồ thường xuyên phải Join với nhau. | **Số lượng Bucket**: Phải được tính toán kỹ (thường là số lũy thừa của 2 như 16, 32, 64...). |

---

## Tổng kết chuỗi Lab:

1. **Broadcast Join**: Cho bảng Nhỏ + Lớn (Dùng RAM).
2. **SortMergeJoin**: Cho bảng Lớn + Lớn (Dùng Shuffle/Disk).
3. **Partition Pruning**: Lọc dữ liệu theo thư mục (Giảm I/O).
4. **Bucketing**: Cho bảng Lớn + Lớn nhưng Join lặp đi lặp lại nhiều lần (Triệt tiêu Shuffle).

---

Chào bạn! Đừng lo lắng, những dòng "lỗi" bạn thấy thực tế không phải là lỗi làm dừng chương trình mà chủ yếu là các **Cảnh báo (Warning)** về cấu hình giữa Spark và Hive Metastore.

---

# Giải thích kết quả Bucketing


## 1. Phân tích Physical Plan (Sự kỳ diệu của Bucketing)

Hãy nhìn kỹ vào các node trong Plan của bạn, nó cực kỳ "sạch sẽ":

* **(1) & (3) Bucketed: true**: Spark đã nhận diện được đây là bảng đã chia túi.
* **SelectedBucketsCount: 16 out of 16**: Spark đọc đúng 16 túi đã chia.
* **MẤT HOÀN TOÀN Node "Exchange"**: Đây là thành công lớn nhất! Trong các bài lab trước, luôn có một bước `Exchange (hashpartitioning...)` để xáo trộn dữ liệu qua mạng. Ở đây, Spark **bỏ qua hoàn toàn việc Shuffle**.
* **Node (2) & (5) Sort**: Bạn vẫn thấy node Sort vì Spark cực kỳ cẩn thận, nó kiểm tra lại thứ tự trong túi. Tuy nhiên, vì bạn đã `sortBy` khi ghi, bước này diễn ra cực nhanh hoặc gần như bằng không (Zero-sort).

**Kết luận:** Bạn đã biến một phép Join nặng nề thành việc "đọc file và khớp nối" tại chỗ. Đây là trạng thái tối ưu nhất của Join trong Big Data.

---

## 2. Kẻ thù cuối cùng: Data Skew (Lệch dữ liệu)

Dù bạn có dùng Bucketing hay Broadcast, bạn vẫn sẽ gặp ác mộng mang tên **Data Skew**.

### Data Skew là gì?

Hãy tưởng tượng bạn Join bảng `Orders` và `Customers`.

* Đa số khách hàng chỉ có 1-2 đơn hàng.
* Nhưng có 1 khách hàng "VIP" (ví dụ ID = '1') có tới **1 triệu đơn hàng**.

Khi Join, 1 máy thợ (Executor) sẽ phải gánh toàn bộ 1 triệu đơn hàng của khách hàng số 1 đó, trong khi các máy khác làm xong và ngồi chơi. Kết quả: Job của bạn cứ lẹt đẹt ở mức **99%** rồi treo hoặc lỗi **OutOfMemory**.

### Cách xử lý Data Skew (Kỹ thuật Salting)

Cách phổ biến nhất là **Salting (Thêm muối)**: Ta thêm một số ngẫu nhiên vào Key bị lệch để chia nhỏ nó ra nhiều máy.

#### Code ví dụ xử lý Skew bằng Salting:

```python
from pyspark.sql import functions as F

# 1. Giả sử bảng orders bị lệch ở customer_id = '1'
# Ta tạo một cột "salt" ngẫu nhiên từ 0-19
orders_salted = orders.withColumn("salt", (F.rand() * 20).cast("int"))
orders_salted = orders_salted.withColumn("salted_key", F.concat(F.col("customer_id"), F.lit("_"), F.col("salt")))

# 2. Bảng customers (bảng nhỏ hơn) phải được nhân bản (explode) để khớp với các muối
# Tạo một mảng [0, 1, 2, ..., 19]
salt_array = F.array([F.lit(i) for i in range(20)])
customers_exploded = customers.withColumn("salt", F.explode(salt_array))
customers_exploded = customers_exploded.withColumn("salted_key", F.concat(F.col("customer_id"), F.lit("_"), F.col("salt")))

# 3. Join trên salted_key
# Lúc này khách hàng ID '1' sẽ được chia ra 20 túi khác nhau, giúp 20 máy cùng làm việc
skewed_join = orders_salted.join(customers_exploded, "salted_key", "left")

```

### Khi nào dùng Salting?

* Khi bạn thấy 1 Task chạy cực lâu trong khi các Task khác xong rất nhanh.
* Khi dữ liệu tập trung quá nhiều vào một Key (ví dụ: khách hàng vãng lai, mã lỗi hệ thống, hoặc các thành phố lớn).

---

**Tổng kết lộ trình của bạn:**
Bạn đã đi từ **Join mặc định** -> **Broadcast** -> **SortMerge** -> **Partition Pruning** -> **Bucketing** và giờ là **Salting**. Đây chính là bộ công cụ hoàn chỉnh của một Senior Data Engineer khi đối mặt với các bài toán tối ưu hiệu năng.
