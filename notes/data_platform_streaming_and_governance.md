# 🧠 Oracle → Spark (PySpark) Cheat Sheet

---

# 🏗️ Data Platform Track (Streaming + Governance – Production Mindset)

Từ phần này trở đi, nội dung **đi song song Streaming + Governance**, đúng mô hình **Data Platform ngân hàng / enterprise**.

Tài liệu này giúp **chuyển tư duy + cú pháp từ Oracle (SQL/PLSQL)** sang **Spark SQL / PySpark**, tập trung vào **thực tế Data Engineering**.

---

## 1️⃣ SELECT / PROJECT

| Oracle                     | Spark SQL                  | PySpark                              |
| -------------------------- | -------------------------- | ------------------------------------ |
| `SELECT col1, col2 FROM t` | `SELECT col1, col2 FROM t` | `df.select("col1", "col2")`          |
| `SELECT *`                 | `SELECT *`                 | `df`                                 |
| Alias                      | `SELECT col1 AS c1`        | `df.select(col("col1").alias("c1"))` |

---

## 2️⃣ WHERE / FILTER

| Oracle         | Spark SQL      | PySpark                             |
| -------------- | -------------- | ----------------------------------- |
| `WHERE a = 1`  | `WHERE a = 1`  | `df.filter(col("a") == 1)`          |
| `BETWEEN`      | `BETWEEN`      | `df.filter(col("a").between(1,10))` |
| `LIKE '%abc%'` | `LIKE '%abc%'` | `df.filter(col("x").like("%abc%"))` |
| `IN (1,2)`     | `IN (1,2)`     | `df.filter(col("a").isin(1,2))`     |

---

## 3️⃣ INSERT / APPEND

| Oracle                     | Spark                                   |                |
| -------------------------- | --------------------------------------- | -------------- |
| `INSERT INTO t SELECT ...` | `df.write.mode("append").parquet(path)` |                |
| Bulk insert                | Distributed write                       | Spark tự scale |

💡 Spark **không có INSERT từng dòng** → luôn ghi theo batch.

---

## 4️⃣ UPDATE / DELETE (❗ khác Oracle)

| Oracle                       | Spark                    |
| ---------------------------- | ------------------------ |
| `UPDATE t SET a=1 WHERE ...` | ❌ Không hỗ trợ trực tiếp |
| `DELETE FROM t WHERE ...`    | ❌ Không                  |

### ✅ Spark pattern (Rewrite Partition)

```python
(df.filter("dt != '2026-01-10'")
 .union(updated_df)
 .write.mode("overwrite").partitionBy("dt").parquet(path))
```

➡️ **UPDATE = đọc → biến đổi → ghi lại partition**

---

## 5️⃣ MERGE INTO (UPSERT)

### Oracle

```sql
MERGE INTO tgt t
USING src s
ON (t.id = s.id)
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT
```

### Spark (foreachBatch + overwrite partition)

```python
existing = spark.read.parquet(path).filter(col("dt") == dt)
merged = existing.union(src).dropDuplicates(["id"])
merged.write.mode("overwrite").partitionBy("dt").parquet(path)
```

➡️ Thường dùng trong **Streaming / Incremental**

---

## 6️⃣ JOIN

| Oracle            | Spark                        |
| ----------------- | ---------------------------- |
| `INNER JOIN`      | `df.join(df2, "id")`         |
| `LEFT JOIN`       | `df.join(df2, "id", "left")` |
| `/*+ USE_HASH */` | `broadcast(df2)`             |

```python
from pyspark.sql.functions import broadcast
fact.join(broadcast(dim), "id")
```

---

## 7️⃣ UNION / UNION ALL

### ⚠️ Oracle yêu cầu đúng thứ tự cột

### Spark (an toàn hơn)

```python
df1.unionByName(df2)
```

➡️ \*\*Luôn dùng \*\***unionByName** trong production

---

## 8️⃣ GROUP BY / HAVING

| Oracle             | Spark                  |
| ------------------ | ---------------------- |
| `GROUP BY`         | `groupBy()`            |
| `HAVING sum(a)>10` | `.filter(sum("a")>10)` |

```python
df.groupBy("dt") \
  .agg(sum("amount").alias("total")) \
  .filter(col("total") > 1000)
```

---

## 9️⃣ Analytic Functions (OVER PARTITION BY)

| Oracle         | Spark                  |
| -------------- | ---------------------- |
| `ROW_NUMBER()` | `row_number()`         |
| `RANK()`       | `rank()`               |
| `PARTITION BY` | `Window.partitionBy()` |

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

w = Window.partitionBy("customer_id").orderBy(col("event_ts").desc())
df.withColumn("rn", row_number().over(w))
```

---

## 🔟 LISTAGG → collect\_list / collect\_set

| Oracle            | Spark             |
| ----------------- | ----------------- |
| `LISTAGG(x, ',')` | `collect_list(x)` |

```python
from pyspark.sql.functions import collect_list

df.groupBy("order_id") \
  .agg(collect_list("product_name").alias("products"))
```

➡️ Spark trả về **ARRAY** (mạnh hơn string)

---

## 1️⃣1️⃣ EXPLODE (Ngược LISTAGG)

```python
from pyspark.sql.functions import explode

df.select("order_id", explode("products").alias("product"))
```

➡️ Oracle làm rất khó, Spark làm cực tốt

---

## 1️⃣2️⃣ STRING / REGEX

| Oracle           | Spark            |
| ---------------- | ---------------- |
| `SUBSTR`         | `substring`      |
| `INSTR`          | `instr`          |
| `REGEXP_LIKE`    | `rlike`          |
| `REGEXP_REPLACE` | `regexp_replace` |

```python
df.withColumn("clean", regexp_replace("raw", "[^0-9]", ""))
```

---

## 1️⃣3️⃣ JSON

| Oracle       | Spark                 |
| ------------ | --------------------- |
| `JSON_VALUE` | `get_json_object`     |
| `JSON_TABLE` | `from_json + explode` |

```python
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType

schema = StructType().add("id", StringType())
df.withColumn("j", from_json("json_col", schema))
```

---

## 1️⃣4️⃣ HIERARCHY (CONNECT BY)

| Oracle             | Spark             |
| ------------------ | ----------------- |
| `CONNECT BY PRIOR` | ❌ Không trực tiếp |

### Spark pattern

- Iterative self-join
- GraphFrames
- BFS

```python
# parent_id → id self join
```

---

## 1️⃣5️⃣ PACKAGE / PROCEDURE / FUNCTION

| Oracle    | Spark                 |
| --------- | --------------------- |
| PACKAGE   | Python module         |
| PROCEDURE | PySpark job           |
| FUNCTION  | Python function / UDF |

```python
def calc_fee(amount):
    return amount * 0.01
```

---

## 🧠 Tư duy quan trọng

| Oracle Mindset | Spark Mindset          |
| -------------- | ---------------------- |
| Row-based      | Columnar               |
| Update/Delete  | Rewrite                |
| Stateful       | Stateless + checkpoint |
| Single DB      | Distributed compute    |

---

## ✅ Checklist khi chuyển Oracle → Spark

-

---

---

## 1️⃣ Data Streaming chuyên nghiệp (Oracle → Spark)

### 1.1 Oracle background (Batch / CDC)

Trong Oracle:

- OLTP ghi từng row
- ETL batch chạy theo giờ/ngày
- CDC dùng GoldenGate / trigger / logminer

```sql
INSERT INTO orders VALUES (...);
COMMIT;
```

👉 **Stateful, row-based**

---

### 1.2 Spark Streaming mindset

Trong Spark:

- Dữ liệu **append-only**
- Xử lý theo **micro-batch**
- State quản lý bằng **checkpoint + watermark**

```text
Source (Kafka / File / PubSub)
  → Bronze (raw)
    → Silver (dedup + clean)
      → Gold (KPI)
```

---

### 1.3 Bronze → Silver (Streaming Upsert thực tế)

#### Oracle MERGE INTO

```sql
MERGE INTO orders tgt
USING orders_src src
ON (tgt.order_id = src.order_id)
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT;
```

#### Spark equivalent (foreachBatch)

```python
def upsert(batch_df, batch_id):
    dedup = batch_df.dropDuplicates(["order_id"])
    (dedup
        .write
        .mode("overwrite")
        .partitionBy("dt")
        .parquet(SILVER_PATH))
```

💡 **Key idea**:

- Spark không update row
- Rewrite partition theo `dt`

---

### 1.4 Late data & Watermark

```python
stream
  .withWatermark("event_ts", "3 days")
  .dropDuplicates(["order_id", "event_ts"])
```

| Oracle           | Spark      |
| ---------------- | ---------- |
| Commit time      | Event time |
| Không xử lý late | Watermark  |

---

### 1.5 Streaming tuning (Production)

| Vấn đề    | Giải pháp                     |
| --------- | ----------------------------- |
| Batch lag | tăng trigger interval         |
| OOM       | giảm partition, tránh count() |
| Duplicate | idempotent write              |

---

## 2️⃣ Data Governance / Provenance (Enterprise)

### 2.1 Oracle governance truyền thống

- Data dictionary
- DB audit
- Manual lineage

---

### 2.2 Spark-native Governance pattern

#### a) Job Run Log (Provenance)

```text
run_id | job_name | input_rows | output_rows | status
```

👉 bạn đã implement `log_job_run()` ✅

---

#### b) Schema Registry

```python
snapshot_schema(spark, df, "gold.kpi_daily", path)
```

Lưu:

- version
- schema\_json
- created\_at

---

#### c) Lineage logic

```text
bronze.orders_raw
  → silver.orders_fact_dt_stream
    → gold.kpi_daily
```

---

### 2.3 Công cụ thực tế (GCP / OSS)

| Tool               | Mục đích     |
| ------------------ | ------------ |
| Data Catalog       | Metadata     |
| Dataplex           | Governance   |
| OpenLineage        | Lineage      |
| Marquez            | Lineage UI   |
| Great Expectations | Data Quality |

---

## 🧠 Tổng kết tư duy

| Oracle     | Spark Platform   |
| ---------- | ---------------- |
| DB-centric | Pipeline-centric |
| Procedure  | Job              |
| Commit     | Checkpoint       |
| Update     | Rewrite          |

---

## 🔜 Tiếp theo

- Streaming → Gold aggregation
- CDC simulation
- Data Quality rules
- SLA / SLO







---



# **🚀 Kafka end-to-end lab (Docker → Kafka UI → Produce JSON → Spark Streaming → Silver)**





## **🎯 Mục tiêu thực tế**



\


Bạn sẽ làm đúng pipeline “Data Platform” kiểu production (mini):



- **Kafka (Bronze streaming source)**: nhận message JSON từng sự kiện
- **Spark Structured Streaming**: parse JSON + watermark + dedup
- **Silver (Parquet partition dt)**: dữ liệu sạch, query nhanh theo ngày
- **Kafka UI**: nhìn thấy topic, message, lag (trực quan



##



## **0) Checklist trước khi chạy**





- Docker Desktop chạy OK
- Port không bị chiếm: 9094, 8089 (Kafka UI), 4040+ (Spark UI)
- Python env: cdp\_env có PySpark 4.0.1



## **1) Docker Compose Kafka + Kafka UI (đã fix lỗi image)**



\




### **✅ Lỗi bạn gặp**



\


bitnami/kafka:3.7 → **manifest not found** (tag không tồn tại / không match kiến trúc)

\




### **✅ Chuẩn dùng (khuyến nghị)**



\


Dùng apache/kafka:3.7.2 (KRaft mode), có EXTERNAL listener: 9094



```
```
