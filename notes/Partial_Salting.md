Đây là "tuyệt chiêu" giúp bạn giải quyết vấn đề Skew mà không làm bùng nổ bộ nhớ (tránh việc nhân bản toàn bộ bảng khách hàng).

Chiến thuật này gọi là **Partial Salting**: Chúng ta chỉ "thêm muối" cho những khách hàng nào thực sự gây ra lỗi Skew (ví dụ ID = 1), còn các khách hàng bình thường khác thì giữ nguyên.

---

## 1. Ý tưởng thuật toán (The Logic)

1. **Xác định Skewed Keys:** Tìm danh sách các ID khách hàng chiếm quá nhiều dòng (ví dụ > 100,000 dòng).
2. **Bảng Orders:** * Nếu `customer_id` nằm trong danh sách Skew  Gán `salt` ngẫu nhiên ( đến ).
* Nếu không  Gán `salt` cố định (ví dụ: ).


3. **Bảng Customers:** * Nếu `customer_id` nằm trong danh sách Skew  `explode` ra 16 dòng với `salt` ( đến ).
* Nếu không  Chỉ giữ 1 dòng với `salt = -1`.



---

## 2. Code thực hiện Partial Salting

Giả sử chúng ta đã biết `SKEWED_IDS = ['1']`.

```python
from pyspark.sql import functions as F

SALT_N = 16
SKEWED_IDS = ['1']

# 1. Salt Bảng Orders (Chỉ salt những ID bị lệch)
o_s = orders.withColumn("salt", 
    F.when(F.col("customer_id").isin(SKEWED_IDS), 
           F.floor(F.rand(7) * SALT_N).cast("int"))
     .otherwise(F.lit(-1)) # Các ID khác dùng chung 1 mã salt -1
)

# 2. Expand Bảng Customers (Chỉ nhân bản những ID bị lệch)
# Tạo mảng [0, 1, ..., 15] cho ID lệch, và [-1] cho ID thường
c_exp = customers.withColumn("salt_array", 
    F.when(F.col("customer_id").isin(SKEWED_IDS), 
           F.array([F.lit(i) for i in range(SALT_N)]))
     .otherwise(F.array(F.lit(-1)))
).withColumn("salt", F.explode(F.col("salt_array")))

# 3. Join như bình thường trên (customer_id, salt)
j = o_s.join(c_exp, ["customer_id", "salt"], "left")

```

---

## 3. Tại sao cách này lại cực kỳ tối ưu?

Hãy so sánh về tài nguyên sử dụng:

| Phương pháp | Số dòng bảng Customers sau xử lý | Resource tiêu thụ |
| --- | --- | --- |
| **Salting toàn bộ** (Bài trước) |  dòng | Cao (Xáo trộn nhiều dữ liệu thừa) |
| **Partial Salting** |  dòng | **Thấp** (Gần như không đổi so với gốc) |

### Cơ chế hoạt động:

* **Khách hàng ID 1:** Vẫn được chia nhỏ ra 16 máy thợ (Tasks) để xử lý song song  **Giải quyết được Skew.**
* **49,999 khách hàng còn lại:** Không bị nhân bản lãng phí. Chúng vẫn đi qua luồng Shuffle thông thường nhưng không gây áp lực lên RAM.

---

## 4. Làm sao để tự động tìm `SKEWED_IDS`?

Thay vì nhập tay `['1']`, bạn có thể cho Spark tự tìm các "kẻ tội đồ" này trước khi Join:

```python
# Lấy top các khách hàng chiếm trên 5% tổng lượng data
threshold = orders.count() * 0.05
skewed_df = orders.groupBy("customer_id").count().filter(F.col("count") > threshold)
SKEWED_IDS = [row['customer_id'] for row in skewed_df.collect()]

```

---

## Kết luận chuỗi bài học về Join Optimization

Bạn đã đi một hành trình rất dài và đầy đủ:

1. **Broadcast Join**: Khi một bảng đủ nhỏ để nhét vào RAM.
2. **SortMergeJoin**: Khi cả hai bảng đều lớn.
3. **Bucketing**: Khi Join lặp lại nhiều lần trên cùng một Key.
4. **Salting**: Khi dữ liệu bị lệch (Skew) nặng.
5. **Partial Salting**: Đỉnh cao của tối ưu - chỉ xử lý những gì bị lỗi để tiết kiệm RAM.

Bạn đã nắm vững toàn bộ "vũ khí" để xử lý bất kỳ bài toán Join nào trong thực tế rồi đấy! Bạn có muốn tôi hướng dẫn cách đóng gói (Refactor) các bước này thành một hàm (Function) để dùng chung cho mọi dự án sau này không?