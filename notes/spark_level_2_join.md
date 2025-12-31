# 🚀 Spark Level 2 – Bài 2: Filter, Select, WithColumn & Execution Plan

## 🎯 Mục tiêu bài này

**Sau bài này bạn sẽ:**
>*	Hiểu Transformation vs Action
>*	Biết Spark có chạy ngay hay không
>*	Bắt đầu chạm vào shuffle (rất quan trọng)

---

## 🧪 Bài toán

**Dữ liệu giao dịch:**

|order_id|customer_id|amount|country|
|--------|-----------|------|-------|
|1|C001|120|VN|
|2|C002|80|VN|
|3|C003|200|SG|
|4|C001|50|VN|


---

### 📌 Bước 1 – Tạo DataFrame:

```python
from pyspark.sql import functions as F

data = [
    (1, "C001", 120, "VN"),
    (2, "C002", 80, "VN"),
    (3, "C003", 200, "SG"),
    (4, "C001", 50, "VN"),
]

columns = ["order_id", "customer_id", "amount", "country"]

df = spark.createDataFrame(data, columns)
df.show()
```
---

### 📌 Bước 2 – Transformation (KHÔNG chạy ngay)

```python
df_vn = df.filter(df.country == "VN")
df_vn_high = df_vn.filter(df_vn.amount > 100)
```

>❓ Spark đã chạy chưa?

>👉 CHƯA

---

### 📌 Bước 3 – Action (Spark bắt đầu chạy)

```python
df_vn_high.show()
```

#### 🧠 Nguyên lý quan trọng

|Loại|Ví dụ|
|----|-----|
|Transformation|filter, select, withColumn|
|Action|show, count, collect|

> 👉 Spark lazy execution

---

### 📌 Bước 4 – Thêm cột mới

```python
df2 = df.withColumn(
    "amount_category",
    F.when(df.amount >= 100, "HIGH").otherwise("LOW")
)

df2.show()
```
---


### 📌 Bước 5 – Xem Execution Plan:

```python
df2.explain(True)
```

Result:

```code
== Physical Plan ==
*(1) Project
+- *(1) Scan ExistingRDD
```

> 👉 Chưa có shuffle → nhẹ

---

# 🚀 Spark Level 2 – Bài 3: groupBy, agg & SHUFFLE (cốt lõi Spark)

> **Đây là bài QUAN TRỌNG NHẤT trước khi bạn làm Spark thật sự trong CDP / Dataproc / EMR**


## 🎯 Mục tiêu bài này

**Sau bài này bạn sẽ:**
>*	Hiểu shuffle là gì (đúng bản chất)
>*	Biết vì sao groupBy rất đắt
>*	Đọc được execution plan có shuffle
>*	Biết khi nào Spark scale / khi nào chết

---

## 1️⃣ Bài toán

Dữ liệu order (như trước):

```python
from pyspark.sql import functions as F

data = [
    (1, "C001", 120, "VN"),
    (2, "C002", 80, "VN"),
    (3, "C003", 200, "SG"),
    (4, "C001", 50, "VN"),
    (5, "C002", 70, "SG"),
]

columns = ["order_id", "customer_id", "amount", "country"]
df = spark.createDataFrame(data, columns)
df.show()
```

---

## 2️⃣ GroupBy cơ bản

❓ Yêu cầu

👉 Tổng tiền theo customer_id

```python
df_group = df.groupBy("customer_id").agg(
    F.sum("amount").alias("total_amount")
)

df_group.show()
```

---

# 🔥 STOP – Đây là lúc SHUFFLE xảy ra

## 3️⃣ Shuffle là gì? (Hiểu đúng, không mơ hồ)

### 🧠 Định nghĩa CHUẨN:

**Shuffle** = **Spark** phải di chuyển dữ liệu giữa các executor để gom các key giống nhau về cùng 1 nơi

**Ví dụ:**
>*	Order của C001 nằm ở partition 1
>*	Order khác của C001 nằm ở partition 5
>> → Spark bắt buộc phải chuyển dữ liệu

>👉 Network + Disk + Serialize = tốn tài nguyên

### 🧩 Minh họa logic

```code
Partition 1: C001, C002
Partition 2: C003
Partition 3: C001, C002

groupBy(customer_id)
        ↓
Shuffle
        ↓
Partition A: C001
Partition B: C002
Partition C: C003
```
---

## 5️⃣ Vì sao shuffle NGUY HIỂM?

|Vấn đề|Hậu quả|
|------|-------|
|Nhiều dữ liệu|Chậm|
|Skew key|Executor chết|
|Network yếu|Timeout|
|Disk chậm|Spill|

> 👉 90% job Spark chậm = shuffle kém kiểm soát

---

## 6️⃣ Ví dụ SHUFFLE TỆ (anti-pattern)

```python
df.groupBy("country", "customer_id").count().show()
```

>❌ GroupBy nhiều cột không cần thiết

>❌ Cardinality cao → shuffle nặng

---

## 7️⃣ Giảm shuffle – cách đầu tiên (cơ bản)

### ✅ Chỉ groupBy đúng thứ cần

```python
df.groupBy("country").sum("amount").show()
```

---

## 8️⃣ Kiểm soát số partition khi shuffle

```python
# Mặc định:

spark.conf.get("spark.sql.shuffle.partitions")

# → thường là 200 (QUÁ NHIỀU cho dataset nhỏ)
```

```python
# 🔧 Giảm xuống khi test / small data

spark.conf.set("spark.sql.shuffle.partitions", "4")

# 👉 Chạy lại groupBy và explain
```

---

# 🧠 CÂU HỎI & TRẢ LỜI

## 🔹 Spark Lazy Evaluation & Action

### ❓ 1. Vì sao filter() không chạy ngay?

### Trả lời:

**filter()** là **transformation**, **Spark** sử dụng **lazy evaluation**, nên chưa thực thi ngay mà chỉ xây dựng logical execution plan.

**Giải thích ngắn:**
>*	Spark chưa đọc dữ liệu
>*	Chỉ ghi nhớ: “khi nào cần thì filter thế này”

---

### ❓ 2. show() khác collect() ở điểm nào?

### Trả lời:

|**show()**|**collect()**|
|----------|-------------|
|Là action|Là action|
|Chỉ lấy một phần dữ liệu (mặc định 20 dòng)|Lấy toàn bộ dữ liệu|
|An toàn với data lớn|RẤT NGUY HIỂM với data lớn|
|Dùng để debug|Chỉ dùng khi data rất nhỏ|

#### Kết luận:

> ❌ Không dùng collect() trong production

> ✅ Dùng show(), take(), limit()

---

### ❓ 3. Khi nào Spark mới thực sự đọc dữ liệu?

### Trả lời:

> Spark chỉ thực sự đọc và xử lý dữ liệu khi gặp ACTION như:
>> show(), count(), collect(), write()

---

## 🔹 GroupBy & Shuffle:

### ❓ 4. Vì sao groupBy() luôn gây shuffle?

>Vì **Spark** cần gom tất cả các record có cùng **key** về cùng một **executor**, nên bắt buộc phải di chuyển dữ liệu giữa các **partition** → gây **shuffle**.

>> 📌 Không có cách nào groupBy mà không shuffle (trừ vài case rất đặc biệt).

---

### ❓ 5. Exchange trong execution plan nghĩa là gì?

### Trả lời:

**Exchange biểu thị giai đoạn shuffle, nơi Spark:**

>*	repartition dữ liệu
>*	truyền dữ liệu qua network
>*	ghi/đọc disk nếu cần

>>📌 Đây là bước đắt nhất trong Spark.

---

### ❓ 6. Vì sao spark.sql.shuffle.partitions = 200 nguy hiểm với dataset nhỏ?

### Trả lời:

Vì **Spark** tạo **200 task shuffle**, trong khi dữ liệu rất ít →
**overhead** (task scheduling, network, file) lớn hơn xử lý dữ liệu.

**📌 Với data nhỏ:**
>*	200 partitions = lãng phí
>*	Job chậm hơn thay vì nhanh

---

### ❓ 7. Khi nào shuffle bắt buộc, khi nào tránh được?

### Trả lời:

**Shuffle BẮT BUỘC khi:**
>*	groupBy
>*	join (không broadcast)
>*	distinct
>*	orderBy

**Shuffle CÓ THỂ TRÁNH khi:**
>*	filter
>*	select
>*	withColumn
>*	map
>*	limit
>*	broadcast join





