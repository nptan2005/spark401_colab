
# 📘 LAB 3 – Phân tích kết quả JOIN & SKEW (Markdown)

## 1️⃣ Case 1 – Không join (baseline, chỉ aggregate orders)

#### Query

```python
orders.groupBy("customer_id").count()
```

#### Kết quả

```code
customer_id = 1 → 500,428 records
các customer khác ~ 38–40 records
```

#### Physical Plan (rút gọn)

```code
Scan parquet
→ HashAggregate
→ Exchange (shuffle by customer_id)
→ HashAggregate
```

#### Ý nghĩa
-	Skew rõ ràng: customer_id = 1 chiếm ~25% tổng dữ liệu
-	Exchange hashpartitioning(customer_id) ⇒
-	1 partition rất nặng
-	nhiều partition gần như rỗng

📌 Đây là điểm xuất phát của vấn đề skew

---

## 2️⃣ Case 2 – Join thường (SortMergeJoin, không salting)

#### Query

```python
orders
  .join(customers, "customer_id", "left")
  .groupBy("segment")
  .count()
```

#### Physical Plan (chính)

```code
Scan orders
→ Exchange (hashpartition customer_id)
→ Sort
Scan customers
→ Exchange
→ Sort
→ SortMergeJoin
→ Aggregate
```

#### Spark UI quan sát
-	Shuffle read/write lớn
-	Một số task chạy lâu hơn hẳn
-	Executor bị under-utilized

#### Kết luận

**❌ Skew KHÔNG được giải quyết**

**Vì:**
-	customer_id = 1 vẫn nằm trong 1 partition
-	SortMergeJoin chỉ thay thuật toán join, không chia skew

---

## 3️⃣ Case 3 – Join + SALTING (điểm quan trọng nhất)

Kỹ thuật bạn áp dụng

```python
# Orders: random salt
orders.withColumn("salt", floor(rand(7) * 16))

# Customers: explode salt 0..15
customers.withColumn("salt", explode(sequence(0, 15)))

# Join on (customer_id, salt)
```

#### Physical Plan (quan trọng)

```code
Exchange hashpartitioning(customer_id, salt)
→ Sort
→ SortMergeJoin
```

#### Điểm khác biệt CHÍNH

|**Trước**|**Sau salting**|
|---------|---------------|
|customer_id = 1 → 1 partition|	customer_id = 1 → 16 partitions
|1 task rất nặng|workload chia đều|
|executor idle|executor chạy đồng đều|

#### Spark UI bạn gửi cho thấy
-	Nhiều task bị skipped (AQE tối ưu)
-	Task duration đồng đều
-	Không còn task “đuối” kéo dài

✅ Salting đã giải quyết skew thành công

---

## 4️⃣ So sánh tổng hợp 3 case

|**Tiêu chí**|**No Join**|**Join thường**|**Join + Salting**|
|-----------|-----------|---------------|----------------|
|Skew	|❌ Có	|❌ Có	|✅ Đã xử lý|
|Shuffle size|Trung bình|	Rất lớn|Lớn nhưng đều|
|Executor usage|Thấp|Thấp|Cao|
|Độ phức tạp|Thấp|Trung|Cao|
|Dùng khi nào|Debug|Data đều|Hot key rõ|


---

## 5️⃣ Vì sao kết quả COUNT vẫn giống nhau?

```code
MASS     1,000,032
SME        500,092
AFFLUENT   499,876
```

**👉 Vì salting KHÔNG làm thay đổi logic dữ liệu, chỉ:**
-	chia nhỏ key vật lý
-	tối ưu phân phối task

📌 Đây là đặc điểm đúng & bắt buộc của kỹ thuật salting.

---

## 6️⃣ Khi nào nên dùng SALTING?

### ✅ NÊN dùng khi:
-	Có hot key (top 1–5 key chiếm >10–20%)
-	Join lớn (fact–fact hoặc fact–dim lớn)
-	Shuffle chiếm phần lớn runtime
-	AQE không cứu được

### ❌ KHÔNG nên dùng khi:
-	Dimension nhỏ → broadcast join đủ
-	Key phân bố đều
-	Data < vài GB
-	Query ad-hoc

---

## 7️⃣ Vai trò của AQE trong các case bạn chạy

#### Bạn bật:

```python
adaptive.enabled = true
shuffle.partitions = 50
```

#### AQE đã:
-	Coalesce partition
-	Skip task không cần thiết
-	Điều chỉnh plan runtime

#### 📌 Nhưng:

AQE KHÔNG tự xử lý skew join nếu key quá lệch

→ Salting vẫn cần thiết.

---

