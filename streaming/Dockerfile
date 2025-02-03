FROM bitnami/spark:latest

USER root
WORKDIR /app

RUN apt-get update && \
    apt-get install -y wget python3-pip && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY spark_app.py .

CMD ["/opt/bitnami/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0", \
     "--conf", "spark.cassandra.connection.host=cassandra", \
     "/app/spark_app.py"]