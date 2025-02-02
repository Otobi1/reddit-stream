FROM bitnami/spark:3.5

USER root
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y wget python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Scala 2.12 compatible connectors
RUN wget -P /opt/bitnami/spark/jars/ \
    https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.5.0/spark-cassandra-connector_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar

# Copy Spark application

COPY spark_app.py .

CMD ["spark-submit", \
     "--master", "spark://spark:7077", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0", \
     "/app/spark_app.py"]