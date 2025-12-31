# 🚀 LEVEL 5 – SPARK ARCHITECT MINDSET (BẮT ĐẦU)

## 🔥 Bài 1: Design Spark Job cho BANK

---

### 🧠 Tư duy Architect

Không hỏi:
- Code chạy được chưa?

Phải hỏi:
- Có scale được không?
- Có audit được không?
- Có rollback được không?

---

### 1️⃣ Checklist thiết kế Spark job (Bank-grade)

- Idempotent output
- Deterministic logic
- No manual rerun dependency
- Clear SLA
- Cost predictable
- Monitoring ready

---

### 2️⃣ Gold Layer – Thiết kế đúng

#### Gold layer chỉ nên:
- Aggregation
- KPI
- Snapshot

#### 🚫 Không nên

- Join nhiều bảng lớn
- Heavy transformation
- Data cleaning

---

### 3️⃣ Một Spark job “đạt chuẩn bank”

- Runtime < SLA / 2
- Retry = 0
- Shuffle có kiểm soát
- Có explain plan lưu lại
- Có owner rõ ràng

---

## 🧠 CÂU HỎI BẮT BUỘC – LEVEL 5 / BÀI 1

1.	Vì sao idempotent quan trọng hơn performance?
>Idempotent quan trọng hơn performance vì:
>>- Job có thể retry an toàn
>>- Không sinh dữ liệu trùng
>>- Không làm sai số liệu tài chính
>>- Cho phép rerun khi incident

>📌 Bank rule

>>Chậm mà đúng > nhanh mà sai

>>👉 Performance có thể tuning sau, data sai thì không cứu được

2.	Vì sao job phải “predictable” hơn là “fast”?
> Predictable quan trọng vì:
>>- SLA ổn định
>>- Cost kiểm soát được
>>- Dễ capacity planning
>>- Không gây incident dây chuyền

>📌 Câu nói chuẩn Architect

>>Bank không cần job nhanh nhất, bank cần job không gây bất ngờ

3.	Vì sao bank ghét auto scale?
> Bank ghét auto scale vì:
>>- Cost không predictable
>>- Dễ vượt quota
>>- Audit khó giải thích
>>- Incident khó RCA

>📌 Insight

>>Auto scale phù hợp startup, không phù hợp hệ thống tài chính

4.	Khi nào nên refuse một yêu cầu business?
> Refuse business khi:
>>- Vi phạm SLA hiện tại
>>- Cost vượt ngân sách
>>- Yêu cầu phá kiến trúc
>>- Không đảm bảo data correctness

>📌 Architect mindset

>>Protect platform > chiều business

5.	Dấu hiệu job cần redesign?
> Job cần redesign khi:
>>- Runtime sát SLA
>>- Retry không an toàn
>>- Cost tăng theo data size
>>- Nhiều hotfix config
>>- Không explain được execution plan

>📌 Rule

>>Nếu phải “cầu nguyện” khi run job → cần redesign

---

# 🚀 LEVEL 5 – BÀI 2

## Spark Architecture Decision (CHỌN ĐÚNG > CODE ĐẸP)

---

## 🧠 Câu hỏi Architect hay hỏi

- Có cần Spark không?
- Dùng batch hay streaming?
- Serverless hay cluster?
- Scale theo data hay theo SLA?

---

## 1️⃣ Khi nào KHÔNG nên dùng Spark?

- Dataset < vài GB
- Logic đơn giản
- Query ad-hoc

👉 Dùng:
-	BigQuery
-	SQL
-	Python thuần

📌 Spark là vũ khí nặng – đừng dùng bừa

---

## 2️⃣ Batch vs Streaming (Bank-grade)

Batch:
- Báo cáo
- Reconciliation
- EOD

Streaming:
- Fraud
- Alert
- Near real-time

🚫 Không trộn mục đích

---

## 3️⃣ Serverless vs Cluster

Serverless:
- Thử nghiệm
- Dev
- Workload nhỏ, ngắn

Cluster:
- Prod
- SLA rõ
- Cost cần predict

📌 Bank thường chọn cluster

---

## 4️⃣ Scale theo cái gì?

Không scale theo:
- CPU
- Memory

Scale theo:
- Data growth
- SLA
- Cost ceiling

---

## 🧠 CÂU HỎI BẮT BUỘC – LEVEL 5 / BÀI 2

1.	Vì sao không nên dùng Spark cho mọi bài toán?
>Không nên dùng Spark cho mọi bài toán vì:
>>- Overhead cao (cluster, shuffle, JVM)
>>- Cost lớn
>>- Debug phức tạp
>>- Không tối ưu cho workload nhỏ

>📌 Architect quote

>>Spark không phải default choice, Spark là last resort cho big data

2.	Khi nào nên chuyển batch → streaming?
> Chuyển batch → streaming khi:
>>- Business cần phản ứng tức thì
>>- Giá trị data giảm theo thời gian
>>- Chậm vài phút gây rủi ro

>**📌 Insight**

>>Không phải vì “cool”, mà vì **business impact**

3.	Vì sao bank ưu tiên cluster hơn serverless?
> Bank ưu tiên cluster vì:
>>- SLA ổn định
>>- Cost predictable
>>- Quota kiểm soát được
>>- Dễ audit & RCA

>📌 Rule

>>Bank thích cái “nhàm chán nhưng ổn định”

4.	Scale theo data khác gì scale theo SLA?
>Scale theo data:
>>- Tăng executor, memory
>>- Không đổi logic

>Scale theo SLA:
>>- Phải redesign job
>>- Giảm shuffle
>>- Tách pipeline

>📌 Architect insight

>>Scale theo SLA là bài toán kiến trúc, không phải tuning

5.	Dấu hiệu kiến trúc Spark đang “vỡ”?
>Dấu hiệu kiến trúc vỡ:
>>- Cost tăng nhanh theo data
>>- Runtime chạm SLA
>>- Fix bằng config liên tục
>>- Không explain được plan
>>- Phụ thuộc manual rerun

---

# 🚀 LEVEL 5 – BÀI 3

## Spark Anti-patterns (BANK THỰC TẾ)

Đây là bài cực kỳ quan trọng – giúp bạn tránh sai lầm chết người trong production

---

## 🔥 Anti-pattern #1: “One Job To Rule Them All”

❌ 1 Spark job:
- Bronze → Silver → Gold
- Join đủ loại bảng
- Runtime 3–4 tiếng

✅ Đúng:

- Job nhỏ, single responsibility
- Mỗi layer 1 job
- Fail dễ, retry an toàn

---

## 🔥 Anti-pattern #2: Fix SLA bằng config

❌ Tăng:
- executor
- memory
- partitions

👉 Nhưng:

-	Shuffle vẫn vậy

-	Design vẫn sai

📌 Rule

Nếu SLA fail → design sai, không phải thiếu RAM

---

## 🔥 Anti-pattern #3: Broadcast “mù”

❌ Broadcast vì:
- Job chậm
- Nghe người khác nói nhanh

✅ Chỉ broadcast khi:

-	Table nhỏ & stable

-	Cardinality thấp

-	Memory đủ

---

## 🔥 Anti-pattern #4: Gold layer làm ETL nặng

❌ Gold:
- Cleaning
- Dedup
- Join lớn

✅ Gold:

- KPI
- Aggregation
- Snapshot

---

## 🔥 Anti-pattern #5: “Retry sẽ cứu”

❌ Retry:
- Skew
- Join sai
- OOM

📌 Rule

Retry không sửa được kiến trúc

---

## 🧠 CÂU HỎI BẮT BUỘC – LEVEL 5 / BÀI 3

1.	Vì sao job “all-in-one” rất nguy hiểm?
>Job all-in-one rất nguy hiểm vì:
>>- Runtime dài → vượt SLA
>>- Không retry được (idempotent khó)
>>- Fail ở 90% → mất toàn bộ effort
>>- RCA khó vì quá nhiều logic trong 1 job

>📌 Architect rule

>>Job càng lớn → rủi ro tăng theo cấp số nhân

2.	Khi nào config tuning là vô nghĩa?
>Config tuning vô nghĩa khi:
>>- Shuffle volume không đổi
>>- Exchange vẫn tồn tại
>>- Join strategy sai
>>- Skew chưa được xử lý

>📌 Quote

>>Config chỉ cứu performance, không cứu design

3.	Vì sao broadcast sai còn nguy hiểm hơn shuffle?
>Broadcast sai nguy hiểm vì:
>>- Gây OOM executor
>>- Fail toàn bộ stage
>>- Retry không có tác dụng
>>- Cost tăng đột biến

>📌 Rule

>>Shuffle chậm → chịu được

>>Broadcast OOM → chết ngay

4.	Vì sao Gold layer không nên xử lý dirty data?
>Gold layer không xử lý dirty data vì:
>>- Gold là source of truth cho business
>>- Dirty logic làm kết quả không deterministic
>>- Khó audit & explain
>>- Vi phạm separation of concerns

>📌 Rule

>>Gold chỉ tính toán, không “chữa bệnh”

5.	Dấu hiệu job đang “sống nhờ config”?
>Dấu hiệu job sống nhờ config:
>>- Tăng executor mỗi tháng
>>- Shuffle volume không giảm
>>- SLA giữ được nhưng cost tăng
>>- Không ai dám rollback config

>📌 Architect red flag

>>Job chạy được là nhờ may mắn, không phải thiết kế

---

## 🏁 KẾT LUẬN LEVEL 5 – BÀI 3

-	Nhận diện anti-pattern production
-	Phân biệt performance issue vs architecture issue
-	Bắt đầu suy nghĩ như Data Platform Owner

---


