# (1) Cheat-sheet: Spark tương ứng Oracle Package/Procedure/SQL (đầy đủ, thực dụng)

## 1) “Package / Procedure / Function” tương ứng Spark

#### Oracle
-	PACKAGE: gom procedure/function, versioning, security
-	PROCEDURE: chạy job (ETL), có side-effect (insert/update)
-	FUNCTION: trả về giá trị (pure / deterministic)
-	JOB (DBMS_SCHEDULER): schedule chạy procedure

#### Spark tương ứng
-	Package → Python module / package (spark/jobs/*.py, spark/utils/*.py)
-	Procedure → Job script (main) hoặc “entrypoint function” def run(args):
-	Function → UDF / SQL function / reusable transform def transform(df):
-	Scheduler → Airflow / Databricks Jobs / Cron / Control-M

> Thực tế Data Engineering: “mỗi job = 1 file”, “mỗi layer = 1 folder”, “utils = library”.

---

## 2) DML/SQL mapping (Oracle ↔ Spark SQL/DataFrame)

### `SELECT`

#### Oracle

```sql
SELECT a,b FROM t WHERE dt='2026-01-10';
```

#### Spark SQL

```python
spark.sql("SELECT a,b FROM t WHERE dt='2026-01-10'")
```

#### Spark DataFrame

```python
df.select("a","b").where(col("dt")=="2026-01-10")
```

---

### `INSERT`

#### Oracle

```sql
INSERT INTO tgt SELECT ... FROM src;
```

#### Spark
-	File/Parquet: `df.write.mode("append").parquet(path)`
-	Table: `df.writeTo("db.tgt").append()` (với catalog phù hợp)

---

### UPDATE / DELETE (khác biệt rất lớn)

#### Oracle update/delete row-level trực tiếp.

#### Spark + Parquet: 
không update từng row (trừ khi dùng Delta/Iceberg/Hudi).
Cách làm thực tế (Parquet): rewrite partition hoặc rewrite dataset.

**Ví dụ rewrite partition theo dt:**

```python
(df.where(col("dt")=="2026-01-10")
 .write.mode("overwrite")
 .partitionBy("dt")
 .parquet(out))
```

---

### MERGE (UPSERT)

#### Oracle

```sql
MERGE INTO tgt t
USING src s
ON (t.id=s.id)
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

#### Spark
***Spark (Parquet thuần): mô phỏng bằng:**

1.	đọc partition/segment cũ
2.	union với new
3.	window dedup (order by ingest_ts desc)
4.	overwrite partition

(đúng y như bạn đang làm trong streaming foreachBatch)

Nếu dùng Delta/Iceberg/Hudi thì có MERGE INTO chuẩn.

---

### JOIN

#### Oracle

```sql
SELECT /*+ USE_HASH */ ...
FROM fact f
LEFT JOIN dim d ON f.k=d.k;
```

#### Spark SQL

```python
SELECT /*+ BROADCAST(d) */ ...
FROM fact f LEFT JOIN dim d ON f.k=d.k
```

#### Spark DataFrame

```python
from pyspark.sql.functions import broadcast
f.join(broadcast(d), "k", "left")
```

---

### UNION / UNION ALL

#### Oracle

```sql
SELECT ... FROM a
UNION ALL
SELECT ... FROM b;
```

#### Spark

```python
a.unionByName(b)          # giống UNION ALL
a.unionByName(b).distinct()  # gần giống UNION (distinct)
```

---

## 3) Window functions: row_number / rank / dense_rank / partition by

#### Oracle

```sql
ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingest_ts DESC)
```

#### Spark

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank

w = Window.partitionBy("id").orderBy(col("ingest_ts").desc())
df.withColumn("rn", row_number().over(w))
```

*	row_number: 1..N không trùng
*	rank: có gap (1,1,3…)
*	dense_rank: không gap (1,1,2…)

---

## 4) GROUP BY / HAVING

#### Oracle

```sql
SELECT k, SUM(x)
FROM t
GROUP BY k
HAVING SUM(x) > 100;
```


#### Spark

```python
df.groupBy("k").agg(sum("x").alias("sx")).where(col("sx")>100)
```

---


Dưới đây là “cheat map Oracle → Spark” theo kiểu thủ thuật thực chiến, kèm ví dụ SQL + DataFrame (ưu tiên DataFrame vì bạn đang code PySpark). 

---

## 5) Nguyên tắc vàng khi “dịch” Oracle → Spark
1.	Spark không phải OLTP
Oracle update/delete row-level rất “tự nhiên”, Spark file-based thì ưu tiên append + rewrite partition (hoặc dùng Delta/Iceberg/Hudi để có MERGE).
2.	Luôn ưu tiên: schema rõ ràng + column name rõ ràng
Tránh “lệch cột” (đúng như bạn nói về unionByName), tránh ambiguous columns khi join.
3.	Đừng collect() / toPandas() trên data lớn
Tương đương “SELECT *” không where trong Oracle.

---

## 6) UNION / MINUS / INTERSECT

### 1.1 UNION ALL

#### Oracle

```sql
SELECT a,b FROM t1
UNION ALL
SELECT a,b FROM t2;
```

#### Spark (an toàn nhất)

```python
t1.unionByName(t2, allowMissingColumns=True)
```

- ✅ unionByName = khớp theo tên cột
- ✅ allowMissingColumns=True = thiếu cột thì tự fill null (rất “data-lake”)

---

### 1.2 UNION (distinct)

#### Oracle

```sql
SELECT ... FROM t1
UNION
SELECT ... FROM t2;
```

#### Spark

```python
t1.unionByName(t2).distinct()
```

---

### 1.3 MINUS / EXCEPT

#### Oracle

```sql
SELECT id FROM a
MINUS
SELECT id FROM b;
```

#### Spark

```python
a.select("id").exceptAll(b.select("id"))   # giữ duplicates
a.select("id").exceptAll(b.select("id")).distinct()  # gần giống MINUS
```

---

### 1.4 INTERSECT

##### Oracle

```sql
SELECT id FROM a
INTERSECT
SELECT id FROM b;
```

##### Spark

```python
a.select("id").intersect(b.select("id"))
```

---

## 2) LISTAGG / string aggregation

Bạn đã đúng:

### 2.1 LISTAGG (có order)

#### Oracle

```sql
LISTAGG(product_name, ',') WITHIN GROUP (ORDER BY product_name)
```

#### Spark (best practice)

```python
from pyspark.sql.functions import collect_list, sort_array, concat_ws

df2 = (df.groupBy("id")
       .agg(concat_ws(",", sort_array(collect_list("product_name"))).alias("products")))
```

### 2.2 collect_set (unique)

```python
from pyspark.sql.functions import collect_set
df.groupBy("id").agg(collect_set("product_name").alias("products_set"))
```

---

## 3) EXPLODE / UNNEST (Oracle khó, Spark mạnh)

### 3.1 Explode array → nhiều dòng

#### Spark

```python
from pyspark.sql.functions import explode

df2 = df.select("id", explode("products_array").alias("product"))
```

### 3.2 Explode map → key/value rows

```python
from pyspark.sql.functions import explode

df2 = df.select("id", explode("attrs_map").alias("k", "v"))
```

---

## 4) JOIN: alias, tránh ambiguous, broadcast, semi/anti join

### 4.1 Tránh ambiguous (bạn từng gặp segment ambiguous)

#### Spark

```python
f = fact.alias("f")
d = dim.alias("d")

j = f.join(d, col("f.customer_id")==col("d.customer_id"), "left") \
     .select("f.*", col("d.segment").alias("segment"), col("d.risk_tier").alias("risk_tier"))
```

### 4.2 Broadcast hint (dimension nhỏ)

```bash
Oracle hint /*+ USE_NL */ / /*+ LEADING */…
```

#### Spark

```python
from pyspark.sql.functions import broadcast
j = fact.join(broadcast(dim), "customer_id", "left")
```

### 4.3 SEMI JOIN / EXISTS

#### Oracle

```sql
SELECT * FROM fact f
WHERE EXISTS (SELECT 1 FROM dim d WHERE d.id=f.id);
```

#### Spark

```python
fact.join(dim.select("id"), "id", "left_semi")
```

### 4.4 ANTI JOIN / NOT EXISTS

```python
fact.join(dim.select("id"), "id", "left_anti")
```

---

## 5) WINDOW: row_number / rank / dense_rank / lag / lead

### 5.1 ROW_NUMBER() OVER (PARTITION BY … ORDER BY …)

#### Oracle

```sql
row_number() over(partition by id order by ingest_ts desc)
```

#### Spark

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

w = Window.partitionBy("id").orderBy(col("ingest_ts").desc())
df2 = df.withColumn("rn", row_number().over(w))
```

### 5.2 Top-1 per group (dedup kiểu MERGE)

```python
df_latest = df2.where(col("rn")==1).drop("rn")
```

### 5.3 LAG/LEAD

```python
from pyspark.sql.functions import lag, lead
df2 = df.withColumn("prev_amt", lag("amount").over(w))
```

---

## 6) GROUP BY / HAVING / ROLLUP / CUBE / GROUPING SETS

### 6.1 HAVING

#### Oracle

```python
group by k having sum(x)>100
```

#### Spark

```python
from pyspark.sql.functions import sum as _sum
(df.groupBy("k")
   .agg(_sum("x").alias("sx"))
   .where(col("sx") > 100))
```

--- 

### 6.2 ROLLUP/CUBE


#### Oracle

```sql
GROUP BY ROLLUP(country, segment)
```

#### Spark

```python
df.rollup("country","segment").agg(_sum("amount").alias("amt"))
df.cube("country","segment").agg(_sum("amount").alias("amt"))
```

### 6.3 GROUPING SETS

```python
df.groupByExpr("grouping sets ((country),(segment),(country,segment))") \
  .agg(_sum("amount").alias("amt"))
```

---

## 7) CASE WHEN / NVL / COALESCE / NULLIF

### 7.1 CASE

#### Oracle

```sql
CASE WHEN amount>0 THEN 'P' ELSE 'N' END
```

#### Spark

```python
from pyspark.sql.functions import when
df.withColumn("flag", when(col("amount")>0,"P").otherwise("N"))
```

### 7.2 NVL / COALESCE

#### Oracle

```sql
 NVL(a,0) / COALESCE(a,b,c)
```

#### Spark

```python
from pyspark.sql.functions import coalesce, lit
df.withColumn("a0", coalesce(col("a"), lit(0)))
```

### 7.3 NULLIF

```python
from pyspark.sql.functions import expr
df.selectExpr("nullif(a,b) as x")
```

---

## 8) DATE/TIME: TRUNC, ADD_MONTHS, MONTHS_BETWEEN…

### 8.1 TRUNC(date)

#### Oracle 

```sql
TRUNC(ts)
```

#### Spark

```python
from pyspark.sql.functions import to_date
df.withColumn("d", to_date("event_ts"))
```

### 8.2 ADD_MONTHS

```python
from pyspark.sql.functions import add_months
df.withColumn("d2", add_months(col("dt"), 1))
```

### 8.3 MONTHS_BETWEEN

```python
from pyspark.sql.functions import months_between
df.withColumn("m", months_between(col("d1"), col("d2")))
```

---

## 9) PIVOT / UNPIVOT

### 9.1 PIVOT

#### Oracle 

```sql
PIVOT(...)
```

#### Spark

```python
(df.groupBy("customer_id")
   .pivot("channel", ["ATM","POS","ECOM"])
   .agg(_sum("amount").alias("amt")))
```

### 9.2 UNPIVOT (Spark: stack / explode)

```python
df.selectExpr("customer_id", "stack(3, 'ATM', ATM, 'POS', POS, 'ECOM', ECOM) as (channel, amt)")
```

---

## 10) MERGE/UPSERT (Oracle mạnh nhất) → Spark thực tế

### 10.1 Parquet thuần (giống bạn đang làm)

Mẫu “merge by key + overwrite partition”

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

combined = old.unionByName(new, allowMissingColumns=True)

w = Window.partitionBy("order_id").orderBy(col("ingest_ts").desc())
dedup = combined.withColumn("rn", row_number().over(w)).where(col("rn")==1).drop("rn")

# overwrite partition dt
(dedup.drop("dt")
     .write.mode("overwrite")
     .parquet(tmp_path))
# move tmp -> dt=...
```

### 10.2 Nếu dùng Delta/Iceberg/Hudi

Bạn sẽ có MERGE INTO giống Oracle (lab sau mình có thể dựng).

---

## 11) “Oracle Analytics Tricks” → Spark equivalents (cực hay)

### 11.1 KEEP (DENSE_RANK FIRST/LAST)

#### Oracle

```sql
max(amount) keep (dense_rank last order by event_ts)
```

#### Spark (window + filter rn=1)

```python
w = Window.partitionBy("id").orderBy(col("event_ts").desc())
(df.withColumn("rn", row_number().over(w))
   .where(col("rn")==1)
   .select("id","amount"))
```


### 11.2 QUALIFY (Oracle 23c / BigQuery style)

Spark không có QUALIFY, dùng subquery:

```python
tmp = df.withColumn("rn", row_number().over(w))
tmp.where(col("rn")==1)
```

---

## 12) “Oracle Package mindset” → Spark code layout (gợi ý nhanh)
-	spark/jobs/ = procedure (job entrypoints)
-	spark/transforms/ = function (pure transforms)
-	spark/gov/ = logging, schema registry, dq
-	spark/sql/ = view / query templates (nếu thích SQL)

⸻

### Bonus: 3 thủ thuật bạn nêu (viết lại đúng “best practice”)

#### 1) unionByName an toàn

```python
df_all = df1.unionByName(df2, allowMissingColumns=True)
```

#### 2) LISTAGG replacement

```python
from pyspark.sql.functions import collect_list, sort_array, concat_ws
df_products = df.groupBy("id").agg(
    concat_ws(",", sort_array(collect_list("product_name"))).alias("products")
)
```

#### 3) explode ngược groupBy

```python
from pyspark.sql.functions import explode
df_lines = df.select("id", explode("products_array").alias("product"))
```
