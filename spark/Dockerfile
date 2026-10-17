FROM apache/spark:3.5.1-python3
USER root
# Kafka connector for Structured Streaming (bundled into the image -> no network needed at runtime)
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/*.jar
COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt
COPY jobs /app/jobs
COPY entrypoint.sh /app/entrypoint.sh
RUN sed -i 's/\r$//' /app/entrypoint.sh && chmod +x /app/entrypoint.sh
WORKDIR /app
ENV PYTHONPATH=/app/jobs PYTHONUNBUFFERED=1 TZ=UTC PATH="/opt/spark/bin:${PATH}"
ENTRYPOINT ["/app/entrypoint.sh"]
