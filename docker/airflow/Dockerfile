FROM apache/airflow:2.10.5-python3.12

USER root

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# system libs: procps (ps), java, build tools (khi lỡ cần compile), + lz4 system package để tránh pip build
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jdk \
      procps \
      curl \
      gcc g++ make \
      libpq-dev \
      python3-lz4 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
# IMPORTANT: giữ /home/airflow/.local/bin để có airflow CLI
ENV PATH="/home/airflow/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

USER airflow

# constraints
RUN curl -sSL "${CONSTRAINT_URL}" -o /tmp/constraints.txt

# pin toolchain + packaging để tránh conflict như bạn gặp
RUN python -m pip install --no-cache-dir --upgrade "pip==25.0" setuptools wheel "packaging<25"

# đảm bảo đúng airflow version theo constraints (tránh bị kéo lên airflow 3.x)
RUN python -m pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" --constraint /tmp/constraints.txt \
 && command -v airflow && airflow version

# copy requirements của bạn
COPY requirements.txt /requirements.txt

# install requirements theo constraints
RUN python -m pip install --no-cache-dir -r /requirements.txt --constraint /tmp/constraints.txt

# OpenLineage + providers theo constraints (KHÔNG pin ngược version)
RUN python -m pip install --no-cache-dir \
      apache-airflow-providers-openlineage \
      openlineage-python \
      apache-airflow-providers-postgres \
      apache-airflow-providers-amazon \
      apache-airflow-providers-oracle \
      apache-airflow-providers-smtp \
      apache-airflow-providers-apache-spark \
      --constraint /tmp/constraints.txt

# pyspark 4.0.1: không dùng constraint (vì constraints pin 3.5.x)
RUN python -m pip install --no-cache-dir "pyspark==4.0.1"

# final sanity
RUN python -c "import pyspark; print('pyspark', pyspark.__version__)" \
 && python -c "import lz4; print('lz4 OK')" || true \
 && airflow version