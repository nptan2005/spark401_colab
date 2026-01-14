# LAB 3A: DETECT SKEW

---

#### Code:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

ORDERS_PATH = "data/silver/orders"  # hoặc orders_enriched, tùy bạn muốn

spark = (
    SparkSession.builder
    .appName("lab3a_detect_skew")
    .config("spark.sql.shuffle.partitions", "50")
    .getOrCreate()
)

orders = spark.read.parquet(ORDERS_PATH)

# xem top keys để confirm skew
top = (orders.groupBy("customer_id")
            .count()
            .orderBy(desc("count"))
            .limit(20))

top.show(20, truncate=False)

# xem plan để thấy shuffle cho groupBy
top.explain("formatted")

spark.stop()
```

#### Kết quả:

```code
|customer_id|count |
+-----------+------+
|1          |500428|
|41960      |40    |
|12938      |39    |
|4246       |39    |
|11293      |39    |
|29855      |39    |
|48314      |39    |
|24352      |39    |
|29724      |39    |
|10882      |39    |
|3100       |39    |
|5622       |38    |
|28975      |38    |
|12375      |38    |
|14889      |38    |
|26200      |38    |
|9606       |38    |
|44421      |38    |
|30225      |38    |
|3703       |38    |
+-----------+------+

== Physical Plan ==
AdaptiveSparkPlan (6)
+- TakeOrderedAndProject (5)
   +- HashAggregate (4)
      +- Exchange (3)
         +- HashAggregate (2)
            +- Scan parquet  (1)


(1) Scan parquet 
Output [1]: [customer_id#1]
Batched: true
Location: InMemoryFileIndex [file:/Users/nptan2005/SourceCode/Python/spark401_colab/data/silver/orders]
ReadSchema: struct<customer_id:string>

(2) HashAggregate
Input [1]: [customer_id#1]
Keys [1]: [customer_id#1]
Functions [1]: [partial_count(1)]
Aggregate Attributes [1]: [count#19L]
Results [2]: [customer_id#1, count#20L]

(3) Exchange
Input [2]: [customer_id#1, count#20L]
Arguments: hashpartitioning(customer_id#1, 50), ENSURE_REQUIREMENTS, [plan_id=65]

(4) HashAggregate
Input [2]: [customer_id#1, count#20L]
Keys [1]: [customer_id#1]
Functions [1]: [count(1)]
Aggregate Attributes [1]: [count(1)#15L]
Results [2]: [customer_id#1, count(1)#15L AS count#7L]

(5) TakeOrderedAndProject
Input [2]: [customer_id#1, count#7L]
Arguments: 20, [count#7L DESC NULLS LAST], [customer_id#1, count#7L]

(6) AdaptiveSparkPlan
Output [2]: [customer_id#1, count#7L]
Arguments: isFinalPlan=false
```

----

Đây là một ví dụ "kinh điển" về hiện tượng **Data Skew (Lệch dữ liệu)**. Kết quả thực thi và Physical Plan của bạn đã phản ánh rất rõ lý do tại sao một số hệ thống Big Data chạy mãi không xong ở bước cuối cùng.

Dưới đây là phân tích chi tiết:

---

## 1. Phân tích kết quả Output: "Bắt quả tang" Skew

Hãy nhìn vào bảng kết quả bạn nhận được:

* **Khách hàng ID = 1**: Có tới **500,428** đơn hàng.
* **Khách hàng ID còn lại**: Chỉ có trung bình khoảng **38-40** đơn hàng.

**Phân tích:** Khách hàng số 1 chiếm tới ~25% tổng lượng dữ liệu (trong tổng số 2 triệu dòng của `ORDERS_N`). Khi Spark thực hiện Shuffle để GroupBy, **toàn bộ 500,428 dòng này sẽ bị đẩy về duy nhất một Task (một máy thợ)**. Trong khi đó, các máy thợ khác chỉ nhận được vài chục dòng. Máy thợ gánh ID "1" sẽ chạy lâu gấp hàng nghìn lần các máy khác.

---

## 2. Giải thích Physical Plan

Kế hoạch thực thi này cho thấy cách Spark nỗ lực xử lý dữ liệu lớn qua các giai đoạn:

* **(1) Scan parquet**: Đọc cột `customer_id`. Spark chỉ đọc duy nhất cột này (Column Pruning) để tối ưu hóa bộ nhớ.
* **(2) HashAggregate (Partial)**: Đây là bước cực kỳ quan trọng. Spark đếm sơ bộ tại chỗ trên từng máy thợ.
* *Ví dụ:* Máy A đếm ID "1" được 100 lần, Máy B đếm được 150 lần. Việc này giúp giảm lượng dữ liệu truyền qua mạng ở bước sau.


* **(3) Exchange (Shuffle)**: Đây là "nút thắt cổ chai". Spark dựa vào mã băm (hash) của `customer_id` để gom tất cả các dòng có cùng ID về cùng một máy.
* Vì ID "1" chiếm 50 vạn dòng, nó sẽ tạo ra một gói dữ liệu (Partition) cực nặng bay qua mạng mạng đến một máy thợ duy nhất.


* **(4) HashAggregate (Final)**: Máy thợ nhận dữ liệu từ bước (3) và cộng dồn các kết quả `partial_count` để ra con số cuối cùng. Máy nhận ID "1" sẽ phải làm việc rất vất vả ở bước này.
* **(5) TakeOrderedAndProject**: Tương ứng với `orderBy(desc("count")).limit(20)`. Spark thu thập top 20 từ các máy thợ về Driver để hiển thị cho bạn.

---

## 3. Giải thích Code

* **`config("spark.sql.shuffle.partitions", "50")`**: Bạn chia dữ liệu thành 50 phần khi Shuffle. Tuy nhiên, vì có 1 key quá lớn (ID "1"), dù bạn có chia thành 50 hay 5,000 phần thì ID "1" vẫn luôn nằm trọn trong **duy nhất một phần**, không thể chia nhỏ hơn được bằng cách Shuffle thông thường.
* **`top.explain("formatted")`**: Lệnh này giúp bạn nhìn thấu "tư duy" của Spark trước khi nó thực sự chạy.

---

## 4. Giải pháp "giải cứu" Job này

Nếu đây là một phép Join thay vì chỉ là GroupBy, bạn sẽ thấy hệ thống bị treo. Để giải quyết, chúng ta có 2 hướng:

1. **Nếu bảng Customers đủ nhỏ**: Hãy dùng **Broadcast Join**. Lúc này sẽ không có bước `Exchange` (3) dựa trên `customer_id` nữa, dữ liệu `orders` sẽ đứng yên tại máy thợ, tránh được việc dồn 50 vạn dòng về một chỗ.
2. **Nếu cả 2 bảng đều lớn**: Sử dụng kỹ thuật **Salting** (Thêm muối). Bạn sẽ biến ID "1" thành "1_0", "1_1", ..., "1_19" để chia 50 vạn dòng đó cho 20 máy thợ cùng xử lý.

---