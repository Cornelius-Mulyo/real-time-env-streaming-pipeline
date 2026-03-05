FROM apache/spark:3.5.1

USER root
WORKDIR /app

# Download Postgres JDBC driver into Spark's jars dir
RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

COPY streaming_job.py /app/streaming_job.py

# optional: keep root since we set user: "0:0" in compose
USER 0