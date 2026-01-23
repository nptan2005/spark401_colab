### 2. Hướng dẫn Build và Lấy file JAR

Bạn thực hiện 3 lệnh sau trong terminal để lấy file JAR về máy local (đặt vào thư mục `jars` của bạn):

**Bước 1: Build image builder**
```bash
docker build -t ol-agent-builder -f Dockerfile.build_jar .
```
*(Lưu ý: Bước này sẽ mất vài phút để Maven tải các thư viện về)*

**Bước 2: Tạo container tạm (không cần chạy)**
```bash
docker create --name temp-ol-builder ol-agent-builder
```

**Bước 3: Copy file JAR từ container ra máy thật**
Giả sử bạn muốn lưu file vào thư mục `./jars` trên máy tính của bạn:

```bash
mkdir -p ./jars
# Copy file jar có dependencies (file nặng > 10MB) ra ngoài
docker cp temp-ol-builder:/app/integration/spark/openlineage-spark-agent/target/openlineage-spark-agent-1.24.0.jar ./jars/openlineage-spark-agent.jar
```

**Bước 4: Dọn dẹp**
```bash
docker rm temp-ol-builder
# docker rmi ol-agent-builder (nếu muốn xóa luôn image build)
```

### 3. Sử dụng trong `docker-compose.yml`

Sau khi bạn đã có file `./jars/openlineage-spark-agent.jar` trên máy, bạn chỉ cần mount nó vào container Spark trong `docker-compose.yml` mà không cần sửa Dockerfile của Spark nữa:

```yaml
spark-master:
# ...
volumes:
# Mount file JAR bạn vừa build vào container
- ./jars/openlineage-spark-agent.jar:/opt/spark/jars/openlineage-spark-agent.jar
# ...