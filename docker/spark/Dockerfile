FROM apache/spark:4.0.1

USER root

# -------------------------------------------------
# System + Python + Java
# -------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip \
    openjdk-17-jdk \
    curl ca-certificates procps \
    libaio1 libpq5 \
    && rm -rf /var/lib/apt/lists/*

# -------------------------------------------------
# Python setup
# -------------------------------------------------
RUN ln -sf /usr/bin/python3 /usr/bin/python

ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# -------------------------------------------------
# Java
# -------------------------------------------------
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:/opt/spark/bin:${PATH}"

# -------------------------------------------------
# Python deps
# -------------------------------------------------
RUN pip install --no-cache-dir \
    pandas numpy pyarrow fastparquet kafka-python

# -------------------------------------------------
# Spark jars
# -------------------------------------------------
COPY jars/*.jar /opt/spark/jars/
ENV SPARK_CLASSPATH="/opt/spark/jars/*"

# -------------------------------------------------
# Spark configs
# -------------------------------------------------
COPY configs/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY configs/log4j2.properties   /opt/spark/conf/log4j2.properties

# -------------------------------------------------
# History
# -------------------------------------------------
RUN mkdir -p /opt/spark/history && chown -R 185:185 /opt/spark/history

USER 185
WORKDIR /opt/spark